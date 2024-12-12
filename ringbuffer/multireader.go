package ringbuffer

import (
	"fmt"
	"io"
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

// NewMultiReaderBuf creates a new MultiReaderBuf with a specified capacity
// and number of readers.
//
// Parameters:
//   - capacity: The initial capacity of the buffer. If 0, the buffer starts with
//     a small default capacity.
//   - numReaders: The number of independent readers to initialize.
//
// Panics:
// - If numReaders is less than 1.
//
// Returns:
// - A pointer to the newly created MultiReaderBuf.
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

// Append adds a new value to the end of the ring buffer.
func (mrb *MultiReaderBuf[T]) Append(value T) int {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	if mrb.data == nil {
		return -1
	}

	return mrb.data.Append(value)
}

// Cap returns the current capacity of the buffer.
//
// Returns:
// - The maximum number of elements the buffer can hold without resizing.
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
// without consuming it.
//
// Parameters:
// - readerID: The ID of the reader.
//
// Returns:
// - A pointer to the first element, or nil if the buffer is empty.
//
// Errors:
// - io.EOF: If no data is available for the specified reader.
func (mrb *MultiReaderBuf[T]) ReaderPeekFirst(readerID int) (*T, error) {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	if !mrb.internalIsReaderValid(readerID) {
		return nil, io.EOF
	}

	if mrb.internalReaderLen(readerID) < 1 {
		return nil, io.EOF
	}

	result := mrb.data.At(mrb.readers[readerID].offset)

	return &result, nil
}

// ReaderConsumeFirst retrieves the first element visible to the specified reader
// and removes it from the reader's view.
//
// Parameters:
// - readerID: The ID of the reader.
//
// Returns:
// - A pointer to the consumed element, or nil if the buffer is empty.
//
// Errors:
// - io.EOF: If no data is available for the specified reader.
func (mrb *MultiReaderBuf[T]) ReaderConsumeFirst(readerID int) (*T, error) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	if !mrb.internalIsReaderValid(readerID) {
		panic("Reading from a reader that's already been closed isn't allowed.")
	}

	if mrb.internalReaderLen(readerID) < 1 {
		return nil, io.EOF
	}

	result := mrb.data.At(mrb.readers[readerID].offset)

	mrb.internalReaderDiscard(readerID, 1)

	return &result, nil
}

// ReaderAt retrieves the value at a specific absolute index from the POV of reader readerID.
//
// Parameters:
// - readerID: The reader corresponding to our perspective.
// - index: The absolute index of the element.
//
// Returns:
// - The value at the specified index.
//
// Panics:
// - If the index is out of bounds (less than RangeFirst() or greater than or equal to RangeLen()).
func (mrb *MultiReaderBuf[T]) ReaderAt(readerID int, index int) T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	if !mrb.internalIsReaderValid(readerID) {
		panic("Reading from a reader that's already been closed isn't allowed.")
	}

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

	if !mrb.internalIsReaderValid(readerID) {
		// off end:
		return mrb.data.RangeLen()
	}

	return mrb.readers[readerID].offset
}

// ReaderRangeLen returns the absolute index just past the last valid element in
// the buffer from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderRangeLen(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	_ = mrb.internalIsReaderValid(readerID)

	return mrb.data.RangeLen()
}

// ReaderRange ranges over the ring buffer from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderRange(readerID int, yieldFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	var valid bool

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		mrb.mu.RLock()
		defer mrb.mu.RUnlock()

		valid = mrb.internalIsReaderValid(readerID)

		if !valid {
			return
		}

		data = mrb.internalReaderToSlice(readerID) // Create a local copy of the data
		startIndex = mrb.readers[readerID].offset  // Capture the starting absolute index
	}()

	if !valid {
		return
	}

	mrb.internalRange(yieldFunc, data, startIndex)
}

// ReaderLen returns the length of the ring buffer from the POV of reader readerID.
func (mrb *MultiReaderBuf[T]) ReaderLen(readerID int) int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderLen(readerID)
}

// ReaderToSlice converts the Buffer into a slice from the POV of reader readerID.
//
// Returns:
//   - A []T containing all elements reader readerID can see in the buffer.
func (mrb *MultiReaderBuf[T]) ReaderToSlice(readerID int) []T {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return mrb.internalReaderToSlice(readerID)
}

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

// CloseReader destroys the reader with the given ID.
func (mrb *MultiReaderBuf[T]) CloseReader(readerID int) {
	mrb.mu.Lock()
	defer mrb.mu.Unlock()

	if !mrb.internalIsReaderValid(readerID) {
		// already closed
		return
	}

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
	// mrb.readers[readerID].owner = nil
	// mrb.readers[readerID].readerID = 0
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

// GetReader returns the reader with the specified readerID.
func (mrb *MultiReaderBuf[T]) GetReader(readerID int) *Reader[T] {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	// This will only return false when mrb.readers[readerID] is nil,
	// which is what we want to return for a (non-panicing) invalid reader:
	_ = mrb.internalIsReaderValid(readerID)

	return mrb.readers[readerID]
}

// RangeReaders iterates over readers, calling yieldFunc for each non-closed reader.
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

// LenReaders returns the number of readers viewing this buffer.
func (mrb *MultiReaderBuf[T]) LenReaders() int {
	result := 0

	mrb.RangeReaders(func(_ int, _ *Reader[T]) bool {
		result++

		return true
	})

	return result
}

// RangeLenReaders returns the number of reader slots allocated for this buffer.
func (mrb *MultiReaderBuf[T]) RangeLenReaders() int {
	mrb.mu.RLock()
	defer mrb.mu.RUnlock()

	return len(mrb.readers)
}

// // Reader proxy methods

// PeekFirst returns a pointer to the first element from the POV of reader,
// leaving it in place.
func (r *Reader[T]) PeekFirst() (*T, error) {
	return r.owner.ReaderPeekFirst(r.readerID)
}

// ConsumeFirst returns a pointer to the first element from the POV of reader,
// removing it from visibility.
func (r *Reader[T]) ConsumeFirst() (*T, error) {
	return r.owner.ReaderConsumeFirst(r.readerID)
}

// ID returns the reader's readerID.
func (r *Reader[T]) ID() int {
	// This will already panic on a nil owner
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.readerID
}

//

// At retrieves the value at a specific absolute index from the POV of reader.
//
// Parameters:
// - readerID: The reader corresponding to our perspective.
// - index: The absolute index of the element.
//
// Returns:
// - The value at the specified index.
//
// Panics:
// - If the index is out of bounds (less than RangeFirst() or greater than or equal to RangeLen()).
func (r *Reader[T]) At(index int) T {
	return r.owner.ReaderAt(r.readerID, index)
}

// Discard discards a given number of elements for the POV of reader.
func (r *Reader[T]) Discard(count int) {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()

	r.owner.internalReaderDiscard(r.readerID, count)
}

// RangeFirst gives the index of the first element from the POV of reader.
func (r *Reader[T]) RangeFirst() int {
	return r.owner.ReaderRangeFirst(r.readerID)
}

// RangeLen returns the absolute index just past the last valid element in
// the buffer from the POV of reader.
func (r *Reader[T]) RangeLen() int {
	return r.owner.ReaderRangeLen(r.readerID)
}

// Range ranges over the ring buffer from the POV of the reader.
func (r *Reader[T]) Range(yieldFunc func(index int, value T) bool) {
	var data []T

	var startIndex int

	func() { // Use an anonymous function to ensure defer unlocks the mutex
		r.owner.mu.RLock()
		defer r.owner.mu.RUnlock()

		data = r.owner.internalReaderToSlice(r.readerID) // Create a local copy of the data
		startIndex = r.owner.readers[r.readerID].offset  // Capture the starting absolute index
	}()

	r.owner.internalRange(yieldFunc, data, startIndex)
}

// Len returns the length of the ring buffer from the POV of the reader.
func (r *Reader[T]) Len() int {
	r.owner.mu.RLock()
	defer r.owner.mu.RUnlock()

	return r.owner.internalReaderLen(r.readerID)
}

// ToSlice converts the Buffer into a slice from the POV of reader readerID.
//
// Returns:
//   - A []T containing all elements reader readerID can see in the buffer.
func (r *Reader[T]) ToSlice() []T {
	return r.owner.ReaderToSlice(r.readerID)
}

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

// We panic on range error and data inconsistency, and return false on other errors.
func (mrb *MultiReaderBuf[T]) internalIsReaderValid(readerID int) bool {
	if (readerID < 0) || (readerID >= len(mrb.readers)) {
		panic(fmt.Sprintf("Reader ID %d is out of range (0 to %d)", readerID, len(mrb.readers)-1))
	}

	if mrb.readers[readerID] == nil {
		return false
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

	return true
}

func (mrb *MultiReaderBuf[T]) internalReaderLen(readerID int) int {
	if !mrb.internalIsReaderValid(readerID) {
		// empty:
		return 0
	}

	return mrb.data.RangeLen() - mrb.readers[readerID].offset
}

func (mrb *MultiReaderBuf[T]) internalReaderToSlice(readerID int) []T {
	if !mrb.internalIsReaderValid(readerID) {
		return nil
	}

	localOffset := mrb.readers[readerID].offset - mrb.data.RangeFirst()
	resultSuperset := mrb.data.ToSlice()

	return resultSuperset[localOffset:]
}

func (mrb *MultiReaderBuf[T]) internalReaderDiscard(readerID int, count int) {
	if !mrb.internalIsReaderValid(readerID) {
		panic("Discarding from a reader that's already been closed isn't allowed.")
	}

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
	if !mrb.internalIsReaderValid(readerID) {
		return nil
	}

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
