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

// Spooler provides buffering for elements that arrive early from a data source.
// It allows buffering (via Stuff) and consumption (via Pull) of elements sequentially.
//
// Type Parameter:
// - T: The type of elements buffered and consumed.
type Spooler[T any] struct {
	input  Source[T]
	buffer []T
}

// Pull retrieves the next item from the Spooler.
// If buffered elements exist, the next buffered item is returned. Otherwise,
// it pulls data from the source.
//
// Parameters:
// - block: Specifies whether the operation should block if no data is available.
//
// Returns:
// - A pointer to the next element (if available).
// - An error, which can include:
//   - io.EOF: No more data is available from the source.
//   - ErrNoDataYet: No data is available yet, but the source may provide more later
//     (only if block is NonBlocking).
//   - Other errors related to the source data retrieval.
//
// Notes:
//   - If the source is exhausted (returns io.EOF), the Spooler will close
//     the source and retain any buffered elements for further consumption.
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
			s.Close()

			return nil, io.EOF
		}

		next, err = s.input.Pull(ctx)
		ctxErr = ctx.Err()
	}

	if (ctxErr != nil) || (err != nil) {
		switch {
		case ctxErr != nil:
			s.input.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			s.Close()

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
// - items: The elements to buffer.
//
// Notes:
//   - If the buffer capacity is insufficient, it will be grown to accommodate
//     the additional items, with extra capacity for future growth.
func (s *Spooler[T]) Stuff(items []T) {
	reqdCap := (len(s.buffer) + len(items))
	if cap(s.buffer) < reqdCap {
		// allocate 2 times required capacity
		s.buffer = slices.Grow(s.buffer, reqdCap<<1)
	}

	s.buffer = append(s.buffer, items...)
}

// closeInput signals to the source that no more data will be pulled.
// The Spooler retains any buffered elements for future consumption.
//
// Notes:
// - This is an internal method and should not be called directly by external consumers.
func (s *Spooler[T]) closeInput() {
	if s.input != nil {
		s.input.Close()
	}

	s.input = nil
}

// Close closes the Spooler and releases its resources.
// It signals to the source that no more data will be pulled and clears
// the internal buffer.
func (s *Spooler[T]) Close() {
	s.closeInput()

	s.buffer = nil
}

// NewSpooler creates and returns a new Spooler.
//
// Parameters:
// - input: The data source to be spooled.
//
// Returns:
// - A pointer to a new Spooler instance.
//
// Notes:
//   - The input source may provide elements dynamically, which the Spooler
//     buffers for sequential consumption.
func NewSpooler[T any](input Source[T]) *Spooler[T] {
	return &Spooler[T]{input: input, buffer: nil}
}
