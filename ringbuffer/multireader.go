package ringbuffer

import (
	"fmt"
	"io"
	"sync"
)

// MultiReaderBuf encapsulates a Buffer and allows multiple independent readers
// to consume data concurrently, each maintaining its own position within the
// buffer.
type MultiReaderBuf[T any] struct {
	mu      sync.RWMutex
	data    *Buffer[T]
	readers []*Reader[T]
}

// Reader provides an interface for reading from a MultiReaderBuf.
// It serves two primary roles:
//
//  1. State Management: Internally maintains state information specific to
//     its associated reader within the MultiReaderBuf.
//  2. Proxy Interface: Acts as a proxy, enabling object-oriented method
//     calls to interact with the MultiReaderBuf.
//
// By combining these roles, Reader offers both efficient state handling and a
// convenient API for buffer interactions.
type Reader[T any] struct {
	owner    *MultiReaderBuf[T]
	readerID int
	offset   int
}

// NewMultiReaderBuf creates a new MultiReaderBuf with the given capacity and
// number of readers. If numReaders is less than 1, NewMultiReaderBuf panics.
func NewMultiReaderBuf[T any](capacity int, numReaders int) *MultiReaderBuf[T] {
	if numReaders < 1 {
		panic(fmt.Sprintf("invalid numReaders (%d): must be at least 1", numReaders))
	}

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

// Append adds a new value to the end of the ring buffer. It returns the
// absolute index of the appended element.
func (mrb *MultiReaderBuf[T]) Append(value T) int {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	if mrb.data == nil {
		return -1
	}

	return mrb.data.Append(value)
}

// Cap returns the current capacity of the buffer, indicating the maximum
// number of elements it can hold without resizing.
func (mrb *MultiReaderBuf[T]) Cap() int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	if mrb.data == nil {
		return 0
	}

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

	if mrb.data == nil {
		return
	}

	mrb.data.Resize(newLen)
}

// Reader methods

// ReaderPeekFirst retrieves the first element visible to the specified reader
// without consuming it. It returns a pointer to the first element or nil if
// the buffer is empty. It returns io.EOF if no data is available for the
// reader. It panics if the readerID has been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderPeekFirst(readerID int) (*T, error) {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReader("peeking from", readerID)

	if mrb.internalReaderLen(readerID) < 1 {
		return nil, io.EOF
	}

	result := mrb.data.At(mrb.readers[readerID].offset)

	return &result, nil
}

// ReaderConsumeFirst retrieves the first element visible to the specified
// reader and removes it from the reader's view. It returns a pointer to the
// consumed element or nil if the buffer is empty. It returns io.EOF if no
// data is available for the reader. It panics if the readerID has been
// closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderConsumeFirst(readerID int) (*T, error) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	mrb.internalValidateReader("consuming from", readerID)

	if mrb.internalReaderLen(readerID) < 1 {
		return nil, io.EOF
	}

	result := mrb.data.At(mrb.readers[readerID].offset)

	mrb.internalReaderDiscard(readerID, 1)

	return &result, nil
}

// ReaderAt retrieves the value at the specified absolute index from the
// perspective of the reader identified by readerID. It panics if the readerID
// has been closed or is invalid, or if the index is out of bounds (less than
// RangeFirst() or greater than or equal to RangeLen()).
func (mrb *MultiReaderBuf[T]) ReaderAt(readerID int, index int) T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReader("reading from", readerID)

	if (index < mrb.readers[readerID].offset) || (index >= mrb.data.RangeLen()) {
		panic(fmt.Sprintf(
			"index %d is out of range (%d to %d)",
			index,
			mrb.readers[readerID].offset,
			mrb.data.RangeLen()-1,
		))
	}

	return mrb.data.At(index)
}

// ReaderDiscard discards the specified number of elements from the perspective
// of the reader identified by readerID. It panics if attempting to discard
// more elements than are visible to the reader or if the readerID
// has been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderDiscard(readerID int, count int) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	mrb.internalReaderDiscard(readerID, count)
}

// ReaderRangeFirst returns the absolute index of the first element visible to
// the reader identified by readerID. It panics if the readerID has been
// closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderRangeFirst(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReader("indexing", readerID)

	return mrb.readers[readerID].offset
}

// ReaderRangeLen returns the absolute index just past the last valid element
// in the buffer from the perspective of the reader identified by readerID.
// It panics if the readerID has been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderRangeLen(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReader("indexing", readerID)

	return mrb.data.RangeLen()
}

// ReaderRange iterates over the buffer from the perspective of the reader
// identified by readerID, calling yieldFunc for each element. If yieldFunc
// returns false, the iteration stops. It panics if the readerID has
// been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderRange(readerID int, yieldFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		mrb.mu.RLock()
		defer mrb.mu.RUnlock()

		mrb.internalValidateReader("iterating over", readerID)

		data = mrb.internalReaderToSlice(readerID) // Create a local copy of the data
		startIndex = mrb.readers[readerID].offset  // Capture the starting absolute index
	}()

	mrb.internalRange(yieldFunc, data, startIndex)
}

// ReaderLen returns the number of elements visible to the reader identified by
// readerID. It panics if the readerID has been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderLen(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderLen(readerID)
}

// ReaderToSlice converts the buffer's contents into a slice from the
// perspective of the reader identified by readerID. It panics if the readerID
// has been closed or is invalid.
func (mrb *MultiReaderBuf[T]) ReaderToSlice(readerID int) []T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderToSlice(readerID)
}

// ReaderToMap converts the buffer's contents into a map from the perspective
// of the reader identified by readerID. The keys are absolute indices, and the
// values are the buffer elements. It panics if the readerID has been closed or
// is invalid.
func (mrb *MultiReaderBuf[T]) ReaderToMap(readerID int) map[int]T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderToMap(readerID)
}

// // Reader management

// CloseReader closes the reader with the specified readerID. Once closed, any
// operation on this Reader is invalid and will panic. Closing a reader signals
// that the reader no longer needs to consume data, allowing the buffer to
// manage its data accordingly.
func (mrb *MultiReaderBuf[T]) CloseReader(readerID int) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	mrb.internalValidateReader("closing", readerID)

	newReaderCount := 0
	prevMinIdx := mrb.readers[readerID].offset
	newMinIdx := -1

	for ID, pReader := range mrb.readers {
		if (ID != readerID) && (pReader != nil) {
			newReaderCount++

			if prevMinIdx > pReader.offset {
				prevMinIdx = pReader.offset
			}

			if (newMinIdx == -1) || (newMinIdx > pReader.offset) {
				newMinIdx = pReader.offset
			}
		}
	}

	// reset Reader[T] to ZeroValue before removing it can cause a race
	// condition: instead we leave the owner and readerID intact to avoid
	// this (illegal but still possible) race:
	mrb.readers[readerID].offset = 0
	mrb.readers[readerID] = nil

	if newReaderCount < 1 {
		mrb.data = nil

		return
	}

	assertf(newMinIdx != -1, "INTERNAL ERROR: newMinIdx not set")

	if newMinIdx > prevMinIdx {
		mrb.data.Discard(newMinIdx - prevMinIdx)
	}
}

// Reader returns the reader associated with the given readerID. It returns
// nil if the readerID has been closed, and panics if it is invalid.
func (mrb *MultiReaderBuf[T]) Reader(readerID int) *Reader[T] {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReaderRange(readerID)

	return mrb.readers[readerID]
}

// ReaderValid returns true if the readerID has not been closed, false if it
// has been, and panics if it is invalid.
func (mrb *MultiReaderBuf[T]) ReaderValid(readerID int) bool {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	mrb.internalValidateReaderRange(readerID)

	return mrb.readers[readerID] != nil
}

// RangeReaders iterates over all active readers, calling yieldFunc for each
// non-closed reader. If yieldFunc returns false, the iteration stops.
func (mrb *MultiReaderBuf[T]) RangeReaders(yieldFunc func(readerID int, reader *Reader[T]) bool) {
	var localReaders []*Reader[T]

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		mrb.mu.RLock()
		defer mrb.mu.RUnlock()

		localReaders = make([]*Reader[T], len(mrb.readers))
		copy(localReaders, mrb.readers)
	}()

	// Iterate over the copied readers outside the lock
	for i, rObj := range localReaders {
		if rObj != nil {
			if !yieldFunc(i, rObj) {
				break
			}
		}
	}
}

// LenReaders returns the number of readers currently viewing the buffer.
func (mrb *MultiReaderBuf[T]) LenReaders() int {
	result := 0

	mrb.RangeReaders(func(_ int, _ *Reader[T]) bool {
		result++

		return true
	})

	return result
}

// RangeLenReaders returns the total number of reader slots allocated for the
// buffer.
func (mrb *MultiReaderBuf[T]) RangeLenReaders() int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return len(mrb.readers)
}

// // Reader proxy methods

// PeekFirst retrieves the first element visible to the reader without
// consuming it. If the buffer is empty, it returns nil and io.EOF. It
// panics if the Reader has been closed.
func (r *Reader[T]) PeekFirst() (*T, error) {
	return r.owner.ReaderPeekFirst(r.readerID)
}

// ConsumeFirst retrieves the first element visible to the reader and removes it
// from visibility. If the buffer is empty, it returns nil and io.EOF. It
// panics if the Reader has been closed.
func (r *Reader[T]) ConsumeFirst() (*T, error) {
	return r.owner.ReaderConsumeFirst(r.readerID)
}

// ID returns the reader's readerID. It is permissible to call this on a closed
// reader.
func (r *Reader[T]) ID() int {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.readerID
}

// Valid reports if a readerID is still open. It is permissible to call this
// on a closed reader.
func (r *Reader[T]) Valid() bool {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.owner.ReaderValid(r.readerID)
}

// At retrieves the value at the specified absolute index from the reader's
// perspective. It panics if the Reader has been closed or if the index is out
// of bounds (less than RangeFirst() or greater than or equal to RangeLen()).
func (r *Reader[T]) At(index int) T {
	return r.owner.ReaderAt(r.readerID, index)
}

// Discard discards the specified number of elements from the perspective
// of the reader identified by the Reader. It panics if attempting to discard
// more elements than are visible to the reader or if the Reader
// has been closed or is invalid.
func (r *Reader[T]) Discard(count int) {
	r.owner.ReaderDiscard(r.readerID, count)
}

// RangeFirst returns the absolute index of the first element visible to
// the Reader. It panics if the Reader has been closed or is invalid.
func (r *Reader[T]) RangeFirst() int {
	return r.owner.ReaderRangeFirst(r.readerID)
}

// RangeLen returns the absolute index just past the last valid element in
// the buffer from the POV of reader.
func (r *Reader[T]) RangeLen() int {
	return r.owner.ReaderRangeLen(r.readerID)
}

// Range iterates over the buffer from the perspective of the reader, calling
// yieldFunc for each element. If yieldFunc returns false, the iteration stops.
// It panics if the reader has been closed.
func (r *Reader[T]) Range(yieldFunc func(index int, value T) bool) {
	r.owner.ReaderRange(r.readerID, yieldFunc)
}

// Len returns the number of elements visible to the reader's perspective. It
// panics if the reader has been closed.
func (r *Reader[T]) Len() int {
	return r.owner.ReaderLen(r.readerID)
}

// ToSlice converts the reader's perspective of the buffer's contents into a
// slice. It panics if the reader has been closed.
func (r *Reader[T]) ToSlice() []T {
	return r.owner.ReaderToSlice(r.readerID)
}

// ToMap converts the reader's perspective of the buffer's contents into a map.
// The keys are absolute indices, and the values are the buffer elements. It
// panics if the reader has been closed.
func (r *Reader[T]) ToMap() map[int]T {
	return r.owner.ReaderToMap(r.readerID)
}

// We panic on range error only.
func (mrb *MultiReaderBuf[T]) internalValidateReaderRange(readerID int) {
	if (readerID < 0) || (readerID >= len(mrb.readers)) {
		panic(fmt.Sprintf("Reader ID %d is out of range (0 to %d)", readerID, len(mrb.readers)-1))
	}
}

// We panic on range error, closed readers and data inconsistency.
func (mrb *MultiReaderBuf[T]) internalValidateReader(operation string, readerID int) {
	mrb.internalValidateReaderRange(readerID)

	if mrb.readers[readerID] == nil {
		panic(fmt.Sprintf("%s closed reader %d", operation, readerID))
	}

	assertf(
		mrb.readers[readerID].owner == mrb,
		"INTERNAL ERROR: reader has bad owner %v, expected %v",
		mrb.readers[readerID].owner,
		mrb,
	)
	assertf(
		mrb.readers[readerID].readerID == readerID,
		"INTERNAL ERROR: reader %d has bad readerID %d",
		readerID,
		mrb.readers[readerID].readerID,
	)
	assertf(
		(mrb.readers[readerID].offset <= mrb.data.RangeLen()) &&
			(mrb.readers[readerID].offset >= mrb.data.RangeFirst()),
		"INTERNAL ERROR: reader %d offset (%d) is out of range (%d to %d)",
		readerID,
		mrb.readers[readerID].offset,
		mrb.data.RangeFirst(),
		mrb.data.RangeLen(),
	)
}

func (mrb *MultiReaderBuf[T]) internalReaderLen(readerID int) int {
	mrb.internalValidateReader("indexing", readerID)

	return mrb.data.RangeLen() - mrb.readers[readerID].offset
}

func (mrb *MultiReaderBuf[T]) internalReaderToSlice(readerID int) []T {
	mrb.internalValidateReader("snapshotting", readerID)

	localOffset := mrb.readers[readerID].offset - mrb.data.RangeFirst()
	resultSuperset := mrb.data.ToSlice()

	return resultSuperset[localOffset:]
}

func (mrb *MultiReaderBuf[T]) internalReaderDiscard(readerID int, count int) {
	mrb.internalValidateReader("discarding", readerID)

	if localSize := mrb.internalReaderLen(readerID); count > localSize {
		panic(fmt.Sprintf("only %d elements available when discarding %d", localSize, count))
	}

	prevMinIdx := mrb.readers[readerID].offset
	newMinIdx := prevMinIdx + count

	for ID, pReader := range mrb.readers {
		if (ID != readerID) && (pReader != nil) {
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
	mrb.internalValidateReader("snapshotting", readerID)

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
func (mrb *MultiReaderBuf[T]) internalRange(yieldFunc func(index int, value T) bool, data []T, startIndex int) {
	// Iterate over the copied data outside the lock
	for i, value := range data {
		absIndex := startIndex + i
		if !yieldFunc(absIndex, value) {
			break
		}
	}
}
