// Package ringbuffer provides efficient and thread-safe implementations of ring buffers,
// including simple ring buffer and multi-reader variants.
//
// A ring buffer is a contiguous memory-based circular queue that grows dynamically
// to accommodate data. It differs from Go's container/ring package, which implements
// an untyped circular linked list. The ringbuffer package uses generics for type safety
// and optimizes for performance with contiguous memory storage.
//
// # Features
//
// The `ringbuffer` package includes:
//   - `Buffer`: A thread-safe, dynamically growing circular buffer supporting
//     appending, random access, iteration, and resizing.
//   - `MultiReaderBuf`: A multi-reader ring buffer allowing independent readers
//     to consume data concurrently, each maintaining its own position.
//
// # Thread Safety
//
// All components in this package are thread-safe, employing synchronization
// mechanisms to ensure consistency without compromising performance.
package ringbuffer

import (
	"fmt"
	"sync"
)

// Type definition

// Buffer is a thread-safe, dynamically growing circular buffer with absolute indexing.
//
// Buffer provides methods for appending, retrieving, and discarding elements,
// as well as iterating over its contents in a range-based manner. It expands
// automatically when capacity is exceeded.
//
// Type Parameter:
// - T: The type of elements stored in the buffer.
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
		panic("capacity must be positive")
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

// Append adds a new element to the end of the buffer, automatically resizing
// the buffer if capacity is exceeded.
//
// Parameters:
// - value: The value to append.
//
// Returns:
// - The absolute index of the appended element, starting from 0.
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

// At retrieves the value at a specific absolute index in the buffer.
//
// Parameters:
// - index: The absolute index of the element (not relative to the buffer's start).
//
// Returns:
// - The value at the specified index.
//
// Panics:
//   - If the index is out of range (less than RangeFirst() or greater than or
//     equal to RangeLen()).
func (rb *Buffer[T]) At(index int) T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	nativeIdx := rb.toNativeIndex(index)

	return rb.data[nativeIdx]
}

// Set updates the value at a specific absolute index in the buffer.
//
// Parameters:
// - index: The absolute index of the element to update.
// - value: The new value to set at the specified index.
//
// Panics:
// - If the index is out of bounds (less than RangeFirst() or greater than or equal to RangeLen()).
func (rb *Buffer[T]) Set(index int, value T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	nativeIdx := rb.toNativeIndex(index)
	rb.data[nativeIdx] = value
}

// Len returns the current number of elements in the buffer.
//
// Returns:
// - The number of valid elements in the buffer.
func (rb *Buffer[T]) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.size
}

// Cap returns the current capacity of the buffer.
//
// Returns:
// - The maximum number of elements the buffer can hold without resizing.
func (rb *Buffer[T]) Cap() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return len(rb.data)
}

// Resize adjusts the capacity of the buffer.
//
// Parameters:
// - newLen: The new capacity of the buffer.
//
// Panics:
// - If the new capacity is smaller than the current number of elements in the buffer (Len()).
//
// Notes:
// - Resize preserves the existing elements and their order.
// - If the new capacity is greater than the current capacity, the buffer is expanded.
// - If the new capacity is smaller but still sufficient to hold all current elements, the buffer is compacted.
func (rb *Buffer[T]) Resize(newLen int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.internalResize(newLen)
}

// Discard removes a specified number of elements from the start of the buffer.
//
// Parameters:
// - count: The number of elements to remove.
//
// Panics:
// - If count is greater than Len().
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

// Empty clears all elements from the buffer while retaining its capacity.
func (rb *Buffer[T]) Empty() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.start = 0
	rb.offset = 0
	rb.size = 0
}

// Range Iteration Methods

// Range calls the provided function for each element in the buffer.
//
// Parameters:
//   - yieldFunc: A callback function invoked with the absolute index and value of each element.
//     If the callback returns false, the iteration stops.
//
// Thread Safety:
//   - Locks the buffer only to extract a snapshot of its state, allowing
//     concurrent operations during iteration.
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

// RangeFirst returns the absolute index of the first valid element in the buffer.
//
// Returns:
// - The absolute index of the first valid element.
func (rb *Buffer[T]) RangeFirst() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset
}

// RangeLen returns the absolute index just past the last valid element in the buffer.
//
// Returns:
// - The absolute index after the last valid element.
func (rb *Buffer[T]) RangeLen() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.offset + rb.size
}

// Slice Conversion Methods

// ToSlice converts the buffer's contents into a linear slice.
//
// Returns:
// - A slice containing all elements in the buffer.
func (rb *Buffer[T]) ToSlice() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return rb.internalToSlice()
}

// ToMap converts the Buffer into a map where keys are absolute indices and values are the buffer elements.
//
// Returns:
// - A map[int]T containing all elements in the buffer, keyed by their absolute indices.
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

// internalToSlice creates a linear slice containing the buffer's elements.
func (rb *Buffer[T]) internalToSlice() []T {
	result := make([]T, rb.size)

	for i := rb.offset; i < (rb.offset + rb.size); i++ {
		result[i-rb.offset] = rb.data[rb.toNativeIndex(i)]
	}

	return result
}

// internalExpand doubles the capacity of the buffer, preserving its contents.
func (rb *Buffer[T]) internalExpand() {
	newCapacity := len(rb.data) << 1
	if newCapacity == 0 {
		newCapacity = 4 // not too tiny!
	}

	rb.internalResize(newCapacity)
}

// internalRange iterates over a snapshot of the buffer, invoking the callback for each element.
func (rb *Buffer[T]) internalRange(yeildFunc func(index int, value T) bool, data []T, startIndex int) {
	// Iterate over the copied data outside the lock
	for i, value := range data {
		absIndex := startIndex + i
		if !yeildFunc(absIndex, value) {
			break
		}
	}
}

// internalResize resizes the buffer to a new capacity.
//
// Panics:
// - If the new capacity is smaller than the current size of the buffer.
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

// toNativeIndex converts an absolute index to a native (internal) index.
//
// Panics:
// - If the index is out of bounds.
func (rb *Buffer[T]) toNativeIndex(absIdx int) int {
	if absIdx < rb.offset {
		panic(fmt.Sprintf("Attempted to access index %d before initial index %d", absIdx, rb.offset))
	}

	if absIdx >= (rb.offset + rb.size) {
		panic(fmt.Sprintf("Attempted to access index %d after final index %d", absIdx, (rb.offset + rb.size - 1)))
	}

	return (absIdx + rb.start - rb.offset) % len(rb.data)
}
