// Package stream provides tools for lazy evaluation and data transformation pipelines.
//
// # Overview
//
// The stream package offers abstractions for handling streams of data, enabling
// efficient, type-safe, and composable processing pipelines. The primary focus
// of this package is on transforming and consuming streams of data, often used
// in scenarios such as lazy evaluation, incremental data processing, and
// functional programming-inspired workflows.
//
// # Core Concepts
//
//   - Source[T]: An interface that represents a producer of data elements, emitting
//     elements one at a time via the Pull method.
//   - Sink: A conceptual construct representing the consumption of elements, often
//     used to aggregate or process data from a Source.
//   - Transformer: A conceptual construct combining the behavior of a Source and a Sink.
//     Transformers take input from a Source, apply a transformation, and act as
//     a new Source for output.
//
// Note: While Source[T] is an actual Go interface defined in this package, the
// Sink and Transformer are conceptual constructs and not explicitly defined as
// interfaces or types in this package. They are described here to aid understanding
// and avoid confusion when working with the components provided.
//
// # Source[T] Lifecycle
//
// Sources have clear responsibilities for resource management and predictable
// behavior:
//
//   - A Source[T] that reaches io.EOF is responsible for closing itself.
//   - If a Sink closes a Source[T], the Sink assumes responsibility for ensuring
//     that the Source is properly closed.
//   - The Close method on a Source[T] must be idempotent. Calling Close multiple
//     times should have no effect after the first call.
//
// # Common Implementations
//
//   - SliceSource: Wraps a slice to act as a Source.
//   - Mapper: Applies a transformation function to elements in a Source.
//   - Filter: Filters elements in a Source based on a predicate.
//   - Taker: Emits a limited number of elements from a Source.
//   - ReduceTransformer: Incrementally reduces elements from a Source while emitting
//     intermediate results.
//
// # Usage
//
// The stream package encourages building composable pipelines. For example:
//
// # Examples
//
// Example 1: Mapping and Filtering
//
//	var result []string
//
//	data := []int{1, 2, 3, 4, 5}
//	source := NewSliceSource(data)
//
//	squareMapper := NewMapper(source, func(n int) int { return n * n })     // Square each number
//	filter := NewFilter(squareMapper, func(n int) bool { return n%2 == 0 }) // Keep even numbers
//	toStrMapper := NewMapper(filter, strconv.Itoa)                          // Convert to string
//	sink := NewSliceSink(&result)
//
//	outResult, err := sink.Append(context.Background(), toStrMapper)
//
//	if err != nil {
//		return // Fail
//	}
//
//	fmt.Println(strings.Join(*outResult, ", ")) // Output: 4, 16
//
// Example 2: Reducing
//
//	data := []int{1, 2, 3, 4, 5}
//	source := NewSliceSource(data)
//	reducer := NewReducer(0, func(acc, next int) int { return acc + next })
//	result, _ := reducer.Reduce(source)
//	fmt.Println(result) // Output: 15
//
// The above example demonstrates the use of Sources, Transformers, and a Sink
// to create and consume a stream of data.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Source represents a source of data that emits elements one at a time.
//
// Pull retrieves the next element, or an error if none is available. Implementations
// should return io.EOF when the source is exhausted. Close releases any resources held by
// the source.
type Source[T any] interface {
	Pull(ctx context.Context) (*T, error) // Returns the next element.
	Close()                               // Lets the consumer tell the source that no more data will be Pull()ed.
}

// sourceFunc is a Source implementation backed by function calls.
type sourceFunc[T any] struct {
	srcFunc   func(context.Context) (*T, error)
	closeFunc func()
}

// Pull calls the underlying source function to retrieve the next element.
// It returns an error if the context is canceled or the source is exhausted.
func (sf *sourceFunc[T]) Pull(ctx context.Context) (*T, error) {
	var val *T

	err := io.EOF
	ctxErr := ctx.Err()

	if (ctxErr == nil) && (sf.srcFunc != nil) {
		val, err = sf.srcFunc(ctx)
		ctxErr = ctx.Err()
	}

	if (err != nil) || (ctxErr != nil) {
		switch {
		case ctxErr != nil:
			sf.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			sf.Close()

			return nil, io.EOF
		default:
			// we expect srcFunc to wrap its own errors.
			return nil, err
		}
	}

	return val, nil
}

// Close releases the resources associated with the source function.
func (sf *sourceFunc[T]) Close() {
	if sf.closeFunc != nil {
		sf.closeFunc()
	}

	sf.srcFunc = nil
	sf.closeFunc = nil
}

// SourceFunc creates a Source backed by function calls.
//
// srcFunc is called to produce elements, and closeFunc is called when
// the source is closed.
func SourceFunc[T any](srcFunc func(context.Context) (*T, error), closeFunc func()) Source[T] {
	return &sourceFunc[T]{
		srcFunc:   srcFunc,
		closeFunc: closeFunc,
	}
}

// SliceSource is a Source implementation backed by a slice.
type SliceSource[T any] struct {
	data []T
}

// NewSliceSource returns a new SliceSource backed by the provided slice.
func NewSliceSource[T any](data []T) *SliceSource[T] {
	return &SliceSource[T]{data: data}
}

// Pull retrieves the next element from the slice or io.EOF if exhausted.
func (sp *SliceSource[T]) Pull(ctx context.Context) (*T, error) {
	if (ctx.Err() != nil) || (len(sp.data) < 1) {
		sp.Close() // Free unused slice

		return nil, io.EOF
	}

	value := sp.data[0]
	sp.data = sp.data[1:]

	return &value, nil
}

// Close releases the slice data.
func (sp *SliceSource[T]) Close() {
	sp.data = nil
}

// SliceSink collects elements from a Source into a slice.
type SliceSink[T any] struct {
	dest *[]T
}

// NewSliceSink creates a new SliceSink writing to the provided slice.
func NewSliceSink[T any](dest *[]T) *SliceSink[T] {
	return &SliceSink[T]{dest: dest}
}

// Append collects all elements from the source and appends them to the slice.
//
// It stops on context cancellation, an error, or when the source is exhausted.
func (ss *SliceSink[T]) Append(ctx context.Context, input Source[T]) (*[]T, error) {
	ctxErr := ctx.Err()

	for {
		var next *T

		var err error

		if ctxErr == nil {
			next, err = input.Pull(ctx)
			ctxErr = ctx.Err()
		}

		if (err != nil) || (ctxErr != nil) {
			switch {
			case ctxErr != nil:
				input.Close()

				return nil, fmt.Errorf("operation canceled: %w", ctxErr)
			case errors.Is(err, io.EOF):
				return ss.dest, nil
			default:
				return nil, fmt.Errorf("data pull failed: %w", err)
			}
		}
		// if next != nil { // This should always be true in Blocking mode.
		*ss.dest = append(*ss.dest, *next) // }
	}
}

// Mapper applies a transformation function to elements from a Source.
type Mapper[TIn, TOut any] struct {
	input Source[TIn]
	mapFn func(TIn) TOut
}

// NewMapper returns a new Mapper wrapping the given Source.
func NewMapper[TIn, TOut any](input Source[TIn], mapFn func(TIn) TOut) *Mapper[TIn, TOut] {
	return &Mapper[TIn, TOut]{input: input, mapFn: mapFn}
}

// Pull retrieves the next element from the input Source, applies the mapping function, and returns the.
func (mt *Mapper[TIn, TOut]) Pull(ctx context.Context) (*TOut, error) {
	var nextIn *TIn

	err := io.EOF
	ctxErr := ctx.Err()

	if (ctxErr == nil) && (mt.input != nil) {
		nextIn, err = mt.input.Pull(ctx)
		ctxErr = ctx.Err()
	}

	if (err != nil) || (ctxErr != nil) {
		switch {
		case ctxErr != nil:
			mt.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			mt.input = nil // Source should have already closed itself
			mt.Close()

			return nil, io.EOF
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	nextOut := mt.mapFn(*nextIn)

	if ctxErr = ctx.Err(); ctxErr != nil {
		mt.Close()

		return nil, fmt.Errorf("operation canceled: %w", ctxErr)
	}

	return &nextOut, nil
}

// Close releases resources associated with the Mapper.
func (mt *Mapper[TIn, TOut]) Close() {
	if mt.input != nil {
		mt.input.Close()
	}

	mt.input = nil
	mt.mapFn = nil
}

// Filter selects elements from a Source based on a predicate function.
type Filter[T any] struct {
	input     Source[T]
	predicate func(T) bool
}

// NewFilter returns a new Filter wrapping the given Source.
func NewFilter[T any](input Source[T], predicate func(T) bool) *Filter[T] {
	return &Filter[T]{input: input, predicate: predicate}
}

// Pull retrieves the next element from the input Source that satisfies the predicate.
func (ft *Filter[T]) Pull(ctx context.Context) (*T, error) {
	for {
		var next *T

		err := io.EOF
		ctxErr := ctx.Err()

		if (ctxErr == nil) && (ft.input != nil) {
			next, err = ft.input.Pull(ctx)
			ctxErr = ctx.Err()
		}

		if (ctxErr != nil) || (err != nil) {
			switch {
			case ctxErr != nil:
				ft.Close()

				return nil, fmt.Errorf("operation canceled: %w", ctxErr)
			case errors.Is(err, io.EOF):
				ft.input = nil // Source should have already closed itself
				ft.Close()

				return nil, io.EOF
			default:
				return nil, fmt.Errorf("data pull failed: %w", err)
			}
		}

		skipElement := !(ft.predicate(*next))

		if ctxErr = ctx.Err(); ctxErr != nil {
			ft.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		}

		if !skipElement {
			return next, nil
		}
	}
}

// Close releases resources associated with the Filter.
func (ft *Filter[T]) Close() {
	if ft.input != nil {
		ft.input.Close()
	}

	ft.input = nil
	ft.predicate = nil
}

// Taker limits the number of elements returned from a Source.
type Taker[T any] struct {
	input     Source[T]
	predicate func(T) bool
}

// NewTaker returns a new Taker wrapping the given Source.
func NewTaker[T any](input Source[T], elCount int) *Taker[T] {
	left := elCount
	pred := func(_ T) bool {
		if left < 1 {
			return false
		}

		left--

		return true
	}

	return &Taker[T]{input: input, predicate: pred}
}

// NewTakeWhile returns a new TakeWhile wrapping the given Source.
func NewTakeWhile[T any](input Source[T], pred func(T) bool) *Taker[T] {
	return &Taker[T]{input: input, predicate: pred}
}

// Pull retrieves the next element, decrementing the remaining count.
func (tt *Taker[T]) Pull(ctx context.Context) (*T, error) {
	var next *T

	err := io.EOF
	ctxErr := ctx.Err()

	if (ctxErr == nil) && (tt.input != nil) {
		next, err = tt.input.Pull(ctx)
		ctxErr = ctx.Err()
	}

	if (ctxErr != nil) || (err != nil) {
		switch {
		case ctxErr != nil:
			tt.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			tt.input = nil // Source should have already closed itself
			tt.Close()

			return nil, io.EOF
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	keepGoing := tt.predicate(*next)

	if ctxErr = ctx.Err(); ctxErr != nil {
		tt.Close()

		return nil, fmt.Errorf("operation canceled: %w", ctxErr)
	}

	if !keepGoing {
		tt.Close()

		return nil, io.EOF
	}

	return next, nil
}

// Close releases resources associated with the Taker.
func (tt *Taker[T]) Close() {
	if tt.input != nil {
		tt.input.Close()
	}

	tt.input = nil
}

// NewDropper returns a Filter that skips the first elCount elements.
func NewDropper[T any](input Source[T], elCount int) *Filter[T] {
	skip := elCount

	return &Filter[T]{
		input: input,
		predicate: func(T) bool {
			if skip <= 0 {
				return true
			}
			skip--

			return false
		},
	}
}

// ReduceTransformer applies a reduction function to a source, producing finalized elements incrementally.
type ReduceTransformer[TIn, TOut any] struct {
	input       Source[TIn]
	reducer     func([]TOut, TIn) ([]TOut, []TOut)
	buffer      []TOut
	accumulator []TOut
}

// NewReduceTransformer returns a new ReduceTransformer instance.
//
// The ReduceTransformer applies a user-defined reduction function to each element of the input source.
// The reducer function separates finalized output elements from an intermediate state.
//
// The input parameter specifies the source of elements, and the reducer processes each element incrementally.
func NewReduceTransformer[TIn, TOut any](
	input Source[TIn],
	reducer func([]TOut, TIn) ([]TOut, []TOut),
) *ReduceTransformer[TIn, TOut] {
	return &ReduceTransformer[TIn, TOut]{
		input:       input,
		reducer:     reducer,
		buffer:      nil,
		accumulator: nil,
	}
}

// Pull generates the next finalized element from the reduction or returns io.EOF when complete.
//
// It first processes any remaining finalized elements in the buffer. If the buffer is empty,
// it pulls elements from the input source and applies the reducer function.
//
// The method returns a pointer to the next finalized element or an error indicating
// cancellation, completion, or other issues.
func (rt *ReduceTransformer[TIn, TOut]) Pull(ctx context.Context) (*TOut, error) {
	ctxErr := ctx.Err()

	if (ctxErr == nil) && (len(rt.buffer) > 0) {
		out := rt.buffer[0]
		rt.buffer = rt.buffer[1:]

		return &out, nil
	}

	if rt.input == nil {
		rt.Close()

		return nil, io.EOF
	}

	var next *TIn

	var err error

	if ctxErr == nil {
		next, err = rt.input.Pull(ctx)
		ctxErr = ctx.Err()
	}

	if (ctxErr != nil) || (err != nil) {
		switch {
		case ctxErr != nil:
			rt.Close()

			return nil, fmt.Errorf("operation canceled: %w", ctxErr)
		case errors.Is(err, io.EOF):
			rt.input = nil // Source should have already closed itself
			rt.buffer = rt.accumulator
			rt.accumulator = nil

			return rt.Pull(ctx)
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	rt.buffer, rt.accumulator = rt.reducer(rt.accumulator, *next)

	return rt.Pull(ctx)
}

// closeInput signals the input source to stop processing and retains any buffered elements.
func (rt *ReduceTransformer[TIn, TOut]) closeInput() {
	if rt.input != nil {
		rt.input.Close()
	}

	rt.input = nil
}

// Close releases all resources held by the ReduceTransformer and stops further processing.
//
// Any buffered elements are discarded, and the reducer function is cleared to prevent future use.
func (rt *ReduceTransformer[TIn, TOut]) Close() {
	rt.closeInput()
	rt.buffer = nil
	rt.accumulator = nil
	rt.reducer = nil
}

// Reducer processes an entire input source and reduces it to a single output value.
type Reducer[TIn, TOut any] struct {
	reducer    func(acc TOut, next TIn) TOut
	initialAcc TOut
}

// NewReducer returns a new Reducer instance configured with the initial accumulator value.
//
// The reducer function processes each input element, updating the accumulator with each step.
// The final value of the accumulator is returned once the input source is fully consumed.
func NewReducer[TIn, TOut any](
	initialAcc TOut,
	reducer func(acc TOut, next TIn) TOut,
) *Reducer[TIn, TOut] {
	return &Reducer[TIn, TOut]{
		reducer:    reducer,
		initialAcc: initialAcc,
	}
}

// Reduce consumes all elements from the input source and computes a single reduced value.
//
// It uses the provided reducer function to process each element, combining it into
// the accumulator. When the input source is exhausted, the final accumulator value is returned.
//
// An error is returned if context cancellation or another error occurs during processing.
func (rc *Reducer[TIn, TOut]) Reduce(ctx context.Context, input Source[TIn]) (TOut, error) {
	acc := rc.initialAcc

	for {
		var next *TIn

		err := io.EOF

		ctxErr := ctx.Err()
		if ctxErr == nil {
			next, err = input.Pull(ctx)
			ctxErr = ctx.Err()
		}

		if (ctxErr != nil) || (err != nil) {
			switch {
			case ctxErr != nil:
				input.Close()

				return *new(TOut), fmt.Errorf("operation canceled: %w", ctxErr)
			case errors.Is(err, io.EOF):
				return acc, nil
			default:
				return *new(TOut), fmt.Errorf("data pull failed: %w", err)
			}
		}
		// 	if next != nil { // In blocking mode, this should always be true.
		acc = rc.reducer(acc, *next) // 	}
	}
}
