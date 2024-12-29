// Package stream provides tools for lazy evaluation and data transformation pipelines.
package stream

// This file implements fork and related features like spool and partition.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
)

// Spooler buffers elements arriving early from a data source and provides
// sequential access to these elements. It supports buffering via Stuff and
// consumption via Pull.
//
// Spooler ensures elements are retrieved in the same order they were buffered
// or produced by the source. It can handle cases where elements are added to
// the buffer manually or arrive from an upstream source.
//
// Type Parameter:
//
//	T: The type of elements buffered and consumed by the Spooler.
type Spooler[T any] struct {
	input  Source[T]
	buffer []T
}

// Pull retrieves the next item from the Spooler.
//
// If there are buffered elements, Pull returns the next buffered element.
// Otherwise, it retrieves data from the upstream source. If the source is
// exhausted, Pull returns io.EOF.
//
// Parameters:
//
//	ctx: The context used to manage cancellation or timeout of the operation.
//
// Returns:
//
//	*T: A pointer to the next element, or nil if no data is available.
//	error: An error indicating the result of the operation.
//
// Notes:
//   - If the source returns io.EOF, the Spooler retains any buffered elements
//     for further consumption and closes the source.
func (s *Spooler[T]) Pull(ctx context.Context) (*T, error) {
	var next *T

	err := io.EOF
	ctxErr := ctx.Err()

	if ctxErr == nil {
		if len(s.buffer) > 0 {
			out := s.buffer[0]
			s.buffer = s.buffer[1:]

			return &out, nil
		}

		if s.input == nil {
			closeErr := s.Close()
			assertf(closeErr == nil, "Close() with nil input always returns nil")

			return nil, io.EOF
		}

		next, err = s.input.Pull(ctx)
		ctxErr = ctx.Err()
	}

	if (ctxErr != nil) || (err != nil) {
		switch {
		case ctxErr != nil:
			if err := s.Close(); err != nil {
				return nil, fmt.Errorf("error closing source while canceling: %w", errors.Join(err, ctxErr))
			}

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			s.input = nil // Source should have already closed itself
			closeErr := s.Close()
			assertf(closeErr == nil, "Close() with nil input always returns nil")

			return nil, io.EOF
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	return next, nil
}

// Stuff appends elements to the Spooler’s internal buffer for later retrieval.
//
// Parameters:
//
//	items: A slice of elements to be added to the buffer.
//
// Notes:
//   - If the buffer capacity is insufficient, it is grown to accommodate the
//     additional elements, with extra capacity allocated for future growth.
func (s *Spooler[T]) Stuff(items []T) {
	reqdCap := (len(s.buffer) + len(items))
	if cap(s.buffer) < reqdCap {
		// allocate 2 times required capacity
		s.buffer = slices.Grow(s.buffer, reqdCap<<1)
	}

	s.buffer = append(s.buffer, items...)
}

// closeInput signals to the upstream source that no more data will be pulled.
//
// Notes:
//   - This method does not clear the internal buffer, allowing further
//     consumption of buffered elements.
//   - It is intended for internal use and should not be called directly by
//     external consumers.
func (s *Spooler[T]) closeInput() error {
	var err error

	if s.input != nil {
		err = s.input.Close()
	}

	s.input = nil

	if err != nil {
		return fmt.Errorf("error closing source: %w", err)
	}

	return nil
}

// Close signals to the Spooler that it will no longer be used and releases
// its resources.
//
// This method clears the internal buffer and closes the upstream source.
func (s *Spooler[T]) Close() error {
	err := s.closeInput()

	s.buffer = nil

	return err
}

// NewSpooler creates a new Spooler instance for buffering and sequentially
// consuming elements from a data source.
//
// Parameters:
//
//	input: The upstream source from which elements are consumed.
//
// Returns:
//
//	*Spooler[T]: A pointer to the newly created Spooler.
func NewSpooler[T any](input Source[T]) *Spooler[T] {
	return &Spooler[T]{input: input, buffer: nil}
}
