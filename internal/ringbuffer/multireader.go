package ringbuffer

import (
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

// // Reader methods
// func (mrb *MultiReaderBuf[T]) ReaderPeekFirst(readerID int) (*T, error)
// func (mrb *MultiReaderBuf[T]) ReaderConsumeFirst(readerID int) (*T, error)
// func (mrb *MultiReaderBuf[T]) ReaderAt(readerID int, index int) T
// func (mrb *MultiReaderBuf[T]) ReaderDiscard(readerID int, count int)
// func (mrb *MultiReaderBuf[T]) ReaderRangeFirst(readerID int) int
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

// func (mrb *MultiReaderBuf[T]) ReaderLen(readerID int) int
// func (mrb *MultiReaderBuf[T]) ReaderToSlice(readerID int) []T
// func (mrb *MultiReaderBuf[T]) ReaderToMap(readerID int) map[int]T

// // Reader management
// func (mrb *MultiReaderBuf[T]) CloseReader(readerID int)
// func (mrb *MultiReaderBuf[T]) GetReader(readerID int) *Reader[T]
// func (mrb *MultiReaderBuf[T]) RangeReaders(yeildFunc func(index int, reader *Reader[T]) bool)
// func (mrb *MultiReaderBuf[T]) LenReaders() int

// // Reader proxy methods
// func (r *Reader[T]) PeekFirst() (*T, error)
// func (r *Reader[T]) ConsumeFirst() (*T, error)
// func (r *Reader[T]) At(index int) T
// func (r *Reader[T]) Discard(count int)
// func (r *Reader[T]) RangeFirst() int
// func (r *Reader[T]) RangeLen() int
// func (r *Reader[T]) Range(yeildFunc func(index int, value T) bool)
// func (r *Reader[T]) Len() int
// func (r *Reader[T]) ToSlice() []T
// func (r *Reader[T]) ToMap() map[int]T

func (mrb *MultiReaderBuf[T]) internalReaderToSlice(readerID int) []T {
	localOffset := mrb.readers[readerID].offset - mrb.data.RangeFirst()
	resultSuperset := mrb.data.ToSlice()

	return resultSuperset[localOffset:]
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
