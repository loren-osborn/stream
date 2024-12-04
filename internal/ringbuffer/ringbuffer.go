package ringbuffer

import (
	"fmt"
	"sync"
)

// RingBuffer is a thread-safe, dynamically growing circular buffer with absolute indexing.
type RingBuffer[T any] struct {
	mu     sync.RWMutex
	data   []T // Underlying slice to store elements
	start  int // Index of the first valid element in data slice
	size   int // Number of valid elements in the buffer
	offset int // Absolute index of the first valid element
}

// NewRingBuffer creates a new RingBuffer with the specified initial capacity.
//
// Parameters:
// - capacity: The initial capacity of the ring buffer.
//
// Panics:
// - If capacity is less than zero.
//
// Returns:
// - A pointer to the newly created RingBuffer.
//
//	func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
//		if capacity < 0 {
//		    panic("capacity must be greater than zero")
//		}
//		result := &RingBuffer[T]{
//			data: nil,
//		}
//		if capacity > 0 {
//		    result.data = make([]T, capacity)
//		}
//		return result
//	}
func NewRingBuffer[T any](_ int) *RingBuffer[T] {
	return &RingBuffer[T]{
		mu:     sync.RWMutex{},
		data:   nil,
		start:  0,
		size:   0,
		offset: 0,
	}
}

// Append adds a new element to the end of the buffer.
// The buffer automatically expands if the capacity is exceeded.
//
// Parameters:
// - value: The value to append.
//
// Returns:
// - The absolute index of the appended element.
func (rb *RingBuffer[T]) Append(value T) int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size >= len(rb.data) {
		rb.expand()
	}

	index := (rb.start + rb.size) % len(rb.data)
	rb.data[index] = value
	rb.size++

	return rb.offset + rb.size - 1
}

// Discard removes elements from the start of the buffer.
//
// Parameters:
// - count: Number of elements to remove.
//
// Panics:
// - If more elements than Len().
func (rb *RingBuffer[T]) Discard(count int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if count > rb.size {
		panic(fmt.Sprintf("Attempted to remove %d elements when only %d present", count, rb.size))
	}

	rb.size -= count
	rb.offset += count
	rb.start += count
}

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
func (rb *RingBuffer[T]) Range(yeildFunc func(index int, value T) bool) {
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

func (rb *RingBuffer[T]) internalRange(yeildFunc func(index int, value T) bool, data []T, startIndex int) {
	// Iterate over the copied data outside the lock
	for i, value := range data {
		absIndex := startIndex + i
		if !yeildFunc(absIndex, value) {
			break
		}
	}
}

// ToSlice converts the RingBuffer to a linear slice.
func (rb *RingBuffer[T]) ToSlice() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.internalToSlice()
}

func (rb *RingBuffer[T]) internalToSlice() []T {
	result := make([]T, rb.size)

	for i := rb.offset; i < (rb.offset + rb.size); i++ {
		result[i-rb.offset] = rb.data[rb.toNativeIndex(i)]
	}

	return result
}

// expand doubles the capacity of the buffer.
func (rb *RingBuffer[T]) expand() {
	newCapacity := len(rb.data) << 1
	if newCapacity == 0 {
		newCapacity = 4 // not too tiny!
	}

	tempNewMe := RingBuffer[T]{
		mu:     sync.RWMutex{},
		data:   make([]T, newCapacity),
		start:  rb.start,
		size:   rb.size,
		offset: rb.offset,
	}

	rb.internalRange(func(index int, value T) bool {
		tempNewMe.Set(index, value)

		return true
	}, rb.internalToSlice(), rb.offset)

	rb.data = tempNewMe.data
}

// RangeFirst is the index of the first element in the ring buffer.
func (rb *RingBuffer[T]) RangeFirst() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset
}

// RangeLen is the index after the last element in the ring buffer.
func (rb *RingBuffer[T]) RangeLen() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset + rb.size
}

// Len is the actual number of elements in the ring buffer.
func (rb *RingBuffer[T]) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.size
}

// Cap is the capacity of the ring buffer.
func (rb *RingBuffer[T]) Cap() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return len(rb.data)
}

// Set sets the value of the ring buffer at a given index.
func (rb *RingBuffer[T]) Set(index int, value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	nativeIdx := rb.toNativeIndex(index)
	rb.data[nativeIdx] = value
}

// At gets the value of the ring buffer at a given index.
func (rb *RingBuffer[T]) At(index int) T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	nativeIdx := rb.toNativeIndex(index)

	return rb.data[nativeIdx]
}

// toNativeIndex converts an external (absolute) index to an internal
// native one.
func (rb *RingBuffer[T]) toNativeIndex(absIdx int) int {
	if absIdx < rb.offset {
		panic(fmt.Sprintf("Attempted to access index %d before initial index %d", absIdx, rb.offset))
	}

	if absIdx >= (rb.offset + rb.size) {
		panic(fmt.Sprintf("Attempted to access index %d after final index %d", absIdx, (rb.offset + rb.size - 1)))
	}

	return (absIdx + rb.start - rb.offset) % len(rb.data)
}

// // toAbsIndex converts an external (absolute) index to an internal
// // native one.
// func (rb *RingBuffer[T]) toAbsIndex(nativeIdx int) int {
// 	unwrappedIdx := nativeIdx
// 	if unwrappedIdx < rb.start {
// 		unwrappedIdx += len(rb.data)
// 	}
// 	absIdx := (unwrappedIdx + rb.offset - rb.start)
// 	if (absIdx < rb.offset) || (absIdx >= (rb.offset + rb.size)) {
// 		panic(fmt.Sprintf(
// 			"Internal index %d maps to absolute index %s which is outside the element range %d to %d",
// 			nativeIdx,
// 			absIdx,
// 			rb.offset,
// 			(rb.offset + rb.size - 1),
// 		))
// 	}
// 	if convertedNativeIdx := rb.toNativeIndex(absIdx); nativeIdx != convertedNativeIdx {
// 		panic(fmt.Sprintf(
// 			"INTERNAL LOGIC ERROR: Internal index %d converted to %d which round trips to %d:\ncap%d/len%d/off%d/start%d",
// 			nativeIdx,
// 			absIdx,
// 			convertedNativeIdx,
// 			len(rb.data),
// 			rb.size,
// 			rb.offset,
// 			rb.start,
// 		))
// 	}
// 	return absIdx
// }