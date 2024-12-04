package ringbuffer

import (
	"fmt"
	"sync"
)

// Type definition

// Buffer is a thread-safe, dynamically growing circular buffer with absolute indexing.
type Buffer[T any] struct {
	mu     sync.RWMutex
	data   []T // Underlying slice to store elements
	start  int // Index of the first valid element in data slice
	size   int // Number of valid elements in the buffer
	offset int // Absolute index of the first valid element
}

// Constructor

// New creates a new Buffer with the specified initial capacity.
//
// Parameters:
// - capacity: The initial capacity of the ring buffer.
//
// Panics:
// - If capacity is less than zero.
//
// Returns:
// - A pointer to the newly created Buffer.
func New[T any](capacity int) *Buffer[T] {
	if capacity < 0 {
		panic("capacity must be greater than zero")
	}

	result := &Buffer[T]{
		mu:     sync.RWMutex{},
		data:   nil,
		start:  0,
		size:   0,
		offset: 0,
	}

	if capacity > 0 {
		result.data = make([]T, capacity)
	}

	return result
}

// Core Public Methods

// Append adds a new element to the end of the buffer.
// The buffer automatically expands if the capacity is exceeded.
//
// Parameters:
// - value: The value to append.
//
// Returns:
// - The absolute index of the appended element.
func (rb *Buffer[T]) Append(value T) int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size >= len(rb.data) {
		rb.internalExpand()
	}

	index := (rb.start + rb.size) % len(rb.data)
	rb.data[index] = value
	rb.size++

	return rb.offset + rb.size - 1
}

// At gets the value of the ring buffer at a given index.
func (rb *Buffer[T]) At(index int) T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	nativeIdx := rb.toNativeIndex(index)

	return rb.data[nativeIdx]
}

// Set sets the value of the ring buffer at a given index.
func (rb *Buffer[T]) Set(index int, value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	nativeIdx := rb.toNativeIndex(index)
	rb.data[nativeIdx] = value
}

// Len is the actual number of elements in the ring buffer.
func (rb *Buffer[T]) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.size
}

// Cap is the capacity of the ring buffer.
func (rb *Buffer[T]) Cap() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return len(rb.data)
}

// Resize adjusts the capacity of the buffer.
//
// Parameters:
//   - newLen: The new number of elements in the ring buffer.
func (rb *Buffer[T]) Resize(newLen int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.internalResize(newLen)
}

// Discard removes elements from the start of the buffer.
//
// Parameters:
// - count: Number of elements to remove.
//
// Panics:
// - If more elements than Len().
func (rb *Buffer[T]) Discard(count int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if count > rb.size {
		panic(fmt.Sprintf("Attempted to remove %d elements when only %d present", count, rb.size))
	}

	rb.size -= count
	rb.offset += count
	rb.start = (rb.start + count) % len(rb.data)
}

// Empty resets the ring buffer to its initial state, except it retains
// its capacity.
func (rb *Buffer[T]) Empty() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.start = 0
	rb.offset = 0
	rb.size = 0
}

// Range Iteration Methods

// Range calls the provided function for each element in the buffer, providing the index and value.
// Iteration stops if the function returns false.
//
// The iteration proceeds in order from the first valid index to one past the last valid index.
//
// Parameters:
//   - yeildFunc: A callback function that takes the absolute index and value of an element.
//     If the callback returns false, the iteration stops.
//
// Thread Safety:
// - The method is thread-safe. It locks the buffer only to extract a consistent snapshot of the buffer's state.
// - The iteration itself is performed outside the lock, allowing other operations to proceed concurrently.
func (rb *Buffer[T]) Range(yeildFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		rb.mu.RLock()
		defer rb.mu.RUnlock()

		data = rb.internalToSlice() // Create a local copy of the data
		startIndex = rb.offset      // Capture the starting absolute index
	}()

	rb.internalRange(yeildFunc, data, startIndex)
}

// RangeFirst is the index of the first element in the ring buffer.
func (rb *Buffer[T]) RangeFirst() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset
}

// RangeLen is the index after the last element in the ring buffer.
func (rb *Buffer[T]) RangeLen() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset + rb.size
}

// Slice Conversion Methods

// ToSlice converts the Buffer to a linear slice.
func (rb *Buffer[T]) ToSlice() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.internalToSlice()
}

// ToSlice converts the Buffer to a linear slice.
func (rb *Buffer[T]) ToMap() map[int]T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	asSlice := rb.internalToSlice()
	result := make(map[int]T, rb.size)
	startIndex := rb.offset
	rb.internalRange(func(idx int, value T) bool {
		result[idx] = value

		return true
	}, asSlice, startIndex)

	return result
}

// Internal Helper Methods

func (rb *Buffer[T]) internalToSlice() []T {
	result := make([]T, rb.size)

	for i := rb.offset; i < (rb.offset + rb.size); i++ {
		result[i-rb.offset] = rb.data[rb.toNativeIndex(i)]
	}

	return result
}

// internalExpand doubles the capacity of the buffer.
func (rb *Buffer[T]) internalExpand() {
	newCapacity := len(rb.data) << 1
	if newCapacity == 0 {
		newCapacity = 4 // not too tiny!
	}

	rb.internalResize(newCapacity)
}

func (rb *Buffer[T]) internalRange(yeildFunc func(index int, value T) bool, data []T, startIndex int) {
	// Iterate over the copied data outside the lock
	for i, value := range data {
		absIndex := startIndex + i
		if !yeildFunc(absIndex, value) {
			break
		}
	}
}

func (rb *Buffer[T]) internalResize(newLen int) {
	if newLen < rb.size {
		panic(fmt.Sprintf("Attempted to resize to %d elements (not big enough to hold %d elements)", newLen, rb.size))
	}

	tempNewMe := Buffer[T]{
		mu:     sync.RWMutex{},
		data:   make([]T, newLen),
		start:  0,
		size:   rb.size,
		offset: rb.offset,
	}

	rb.internalRange(func(index int, value T) bool {
		tempNewMe.Set(index, value)

		return true
	}, rb.internalToSlice(), rb.offset)

	rb.data = tempNewMe.data
	rb.start = 0
}

// toNativeIndex converts an external (absolute) index to an internal
// native one.
func (rb *Buffer[T]) toNativeIndex(absIdx int) int {
	if absIdx < rb.offset {
		panic(fmt.Sprintf("Attempted to access index %d before initial index %d", absIdx, rb.offset))
	}

	if absIdx >= (rb.offset + rb.size) {
		panic(fmt.Sprintf("Attempted to access index %d after final index %d", absIdx, (rb.offset + rb.size - 1)))
	}

	return (absIdx + rb.start - rb.offset) % len(rb.data)
}
