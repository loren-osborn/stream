package ringbuffer

import (
	"fmt"
	"sync"
)

// MultiReaderBuf is a multi-reader interface encapsulating a Buffer ring buffer.
type MultiReaderBuf[T any] struct {
	mu      sync.RWMutex
	data    *Buffer[T]
	readers []*Reader[T]
}

// Reader reads from a MultiReaderBuf.
type Reader[T any] struct {
	owner    *MultiReaderBuf[T]
	readerID int
	offset   int
}

// NewMultiReaderBuf creates a new Multi-Reader ring buffer with a given capacity and number of readers.
func NewMultiReaderBuf[T any](capacity int, numReaders int) *MultiReaderBuf[T] {
	result := &MultiReaderBuf[T]{
		mu:      sync.RWMutex{},
		data:    New[T](capacity),
		readers: make([]*Reader[T], numReaders),
	}
	for readerID := range numReaders {
		result.readers[readerID] = &Reader[T]{
			owner:    result,
			readerID: readerID,
			offset:   0,
		}
	}

	return result
}

// Writer methods

// Append adds a new value to the end of the ring buffer.
func (mrb *MultiReaderBuf[T]) Append(value T) int {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	return mrb.data.Append(value)
}

// Cap returns the current capacity of the buffer.
//
// Returns:
// - The maximum number of elements the buffer can hold without resizing.
func (mrb *MultiReaderBuf[T]) Cap() int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.data.Cap()
}

// Resize adjusts the capacity of the buffer.
//
// Parameters:
// - newLen: The new capacity of the buffer.
//
// Panics:
//   - If the new capacity is smaller than the maximum current number of elements
//     in the buffer from the point of view of any reader.
//
// Notes:
//   - Resize preserves the existing elements and their order.
//   - If the new capacity is greater than the current capacity, the buffer is expanded.
//   - If the new capacity is smaller but still sufficient to hold all current
//     elements, the buffer is compacted.
func (mrb *MultiReaderBuf[T]) Resize(newLen int) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	mrb.data.Resize(newLen)
}

// // Reader methods
// func (mrb *MultiReaderBuf[T]) ReaderPeekFirst(readerID int) (*T, error)
// func (mrb *MultiReaderBuf[T]) ReaderConsumeFirst(readerID int) (*T, error)
// func (mrb *MultiReaderBuf[T]) ReaderAt(readerID int, index int) T

// ReaderDiscard discards a given number of elements for the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderDiscard(readerID int, count int) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	mrb.internalReaderDiscard(readerID, count)
}

// ReaderRangeFirst gives the index of the first element from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderRangeFirst(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.readers[readerID].offset
}

// func (mrb *MultiReaderBuf[T]) ReaderRangeLen(readerID int) int

// ReaderRange ranges over the ring buffer from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderRange(readerID int, yeildFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		mrb.mu.RLock()
		defer mrb.mu.RUnlock()

		data = mrb.internalReaderToSlice(readerID) // Create a local copy of the data
		startIndex = mrb.readers[readerID].offset  // Capture the starting absolute index
	}()

	mrb.internalRange(yeildFunc, data, startIndex)
}

// ReaderLen returns the length of the ring buffer from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderLen(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderLen(readerID)
}

// func (mrb *MultiReaderBuf[T]) ReaderToSlice(readerID int) []T

// ReaderToMap converts the Buffer into a map from the POV of reader readerID.
//
// Returns:
//   - A map[int]T containing all elements reader readerID can see in the buffer,
//     keyed by their absolute indices.
func (mrb *MultiReaderBuf[T]) ReaderToMap(readerID int) map[int]T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderToMap(readerID)
}

// // Reader management
// func (mrb *MultiReaderBuf[T]) CloseReader(readerID int).

// GetReader returns the reader with the specified readerID.
func (mrb *MultiReaderBuf[T]) GetReader(readerID int) *Reader[T] {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.readers[readerID]
}

// RangeReaders iterates over readers, calling yeildFunc for each non-closed reader.
func (mrb *MultiReaderBuf[T]) RangeReaders(yeildFunc func(readerID int, reader *Reader[T]) bool) {
	var localReaders []*Reader[T]

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		mrb.mu.RLock()
		defer mrb.mu.RUnlock()

		localReaders = make([]*Reader[T], len(mrb.readers))
		copy(localReaders, mrb.readers)
	}()

	// Iterate over the copied readers outside the lock
	for i, rObj := range localReaders {
		if !yeildFunc(i, rObj) {
			break
		}
	}
}

// func (mrb *MultiReaderBuf[T]) LenReaders() int

// // Reader proxy methods
// func (r *Reader[T]) PeekFirst() (*T, error)
// func (r *Reader[T]) ConsumeFirst() (*T, error)

// ID returns the reader's readerID.
func (r *Reader[T]) ID() int {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.readerID
}

// func (r *Reader[T]) At(index int) T.

// Discard discards a given number of elements for the POV of reader.
func (r *Reader[T]) Discard(count int) {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()

	r.owner.internalReaderDiscard(r.readerID, count)
}

// func (r *Reader[T]) RangeFirst() int
// func (r *Reader[T]) RangeLen() int

// Range ranges over the ring buffer from the POV of the reader.
func (r *Reader[T]) Range(yeildFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		r.owner.mu.RLock()
		defer r.owner.mu.RUnlock()

		data = r.owner.internalReaderToSlice(r.readerID) // Create a local copy of the data
		startIndex = r.owner.readers[r.readerID].offset  // Capture the starting absolute index
	}()

	r.owner.internalRange(yeildFunc, data, startIndex)
}

// Len returns the length of the ring buffer from the POV of the reader.
func (r *Reader[T]) Len() int {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.owner.internalReaderLen(r.readerID)
}

// func (r *Reader[T]) ToSlice() []T

// ToMap converts the Buffer into a map from the POV of the reader.
//
// Returns:
//   - A map[int]T containing all elements reader readerID can see in the buffer,
//     keyed by their absolute indices.
func (r *Reader[T]) ToMap() map[int]T {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.owner.internalReaderToMap(r.readerID)
}

func (mrb *MultiReaderBuf[T]) internalReaderLen(readerID int) int {
	return mrb.data.RangeLen() - mrb.readers[readerID].offset
}

func (mrb *MultiReaderBuf[T]) internalReaderToSlice(readerID int) []T {
	localOffset := mrb.readers[readerID].offset - mrb.data.RangeFirst()
	resultSuperset := mrb.data.ToSlice()

	return resultSuperset[localOffset:]
}

func (mrb *MultiReaderBuf[T]) internalReaderDiscard(readerID int, count int) {
	if localSize := mrb.internalReaderLen(readerID); count > localSize {
		panic(fmt.Sprintf("Attempted to remove %d elements when only %d visible", count, localSize))
	}

	prevMinIdx := mrb.readers[readerID].offset
	newMinIdx := prevMinIdx + count

	for ID, pReader := range mrb.readers {
		if (ID != readerID) && (mrb.readers[readerID] != nil) {
			if prevMinIdx > pReader.offset {
				prevMinIdx = pReader.offset
			}

			if newMinIdx > pReader.offset {
				newMinIdx = pReader.offset
			}
		}
	}

	mrb.readers[readerID].offset += count
	if newMinIdx > prevMinIdx {
		mrb.data.Discard(newMinIdx - prevMinIdx)
	}
}

func (mrb *MultiReaderBuf[T]) internalReaderToMap(readerID int) map[int]T {
	asSlice := mrb.internalReaderToSlice(readerID)
	result := make(map[int]T, mrb.internalReaderLen(readerID))
	startIndex := mrb.readers[readerID].offset
	mrb.internalRange(func(idx int, value T) bool {
		result[idx] = value

		return true
	}, asSlice, startIndex)

	return result
}

// internalRange iterates over a snapshot of the buffer, invoking the callback for each element.
func (mrb *MultiReaderBuf[T]) internalRange(yeildFunc func(index int, value T) bool, data []T, startIndex int) {
	// Iterate over the copied data outside the lock
	for i, value := range data {
		absIndex := startIndex + i
		if !yeildFunc(absIndex, value) {
			break
		}
	}
}