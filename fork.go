// fork.go implements fork and related features like spool and partition
package stream

import (
	"errors"
	"fmt"
	"slices"
)

// Spool allows buffering of elements that arrive early.
type Spooler[T any] struct {
	input  Source[T]
	buffer []T
}

// Pull proxies the call to the source lambda.
func (s *Spooler[T]) Pull(block BlockingType) (*T, error) {
	if len(s.buffer) > 0 {
		out := s.buffer[0]
		s.buffer = s.buffer[1:]

		return &out, nil
	}

	if s.input == nil {
		s.Close()

		return nil, ErrEndOfData
	}

	next, err := s.input.Pull(block)
	if err != nil {
		switch {
		case errors.Is(err, ErrEndOfData):
			s.Close()

			return nil, ErrEndOfData
		case (block == NonBlocking) && errors.Is(err, ErrNoDataYet):
			return nil, ErrNoDataYet
		case errors.Is(err, ErrNoDataYet):
			return nil, fmt.Errorf("unexpected sentinel error: %w", err)
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	return next, nil
}

// Stuff buffers the next items due to be Pull()ed.
func (s *Spooler[T]) Stuff(items []T) {
	reqdCap := (len(s.buffer) + len(items))
	if cap(s.buffer) < reqdCap {
		// allocate 2 times required capacity
		s.buffer = slices.Grow(s.buffer, reqdCap<<1)
	}

	s.buffer = append(s.buffer, items...)
}

// closeInput tells the source no more data will be Pull()ed but retains
// the remaining buffer to spool to consumer.
func (s *Spooler[T]) closeInput() {
	if s.input != nil {
		s.input.Close()
	}

	s.input = nil
}

// Close tells the source no more data will be Pull()ed.
func (s *Spooler[T]) Close() {
	s.closeInput()

	s.buffer = nil
}

// NewMapper creates a new Mapper.
func NewSpooler[T any](input Source[T]) *Spooler[T] {
	return &Spooler[T]{input: input, buffer: nil}
}
