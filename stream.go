// Package stream provides tools for lazy evaluation and data transformation pipelines.
//
// # Overview
//
// This package defines abstractions for producing, consuming, and transforming data
// streams in Go. Streams are modeled using interfaces and generics, enabling type-safe
// and reusable pipelines for processing large datasets.
//
// # Core Components
//
// Source: A data source that emits elements one at a time.
//
// # Common Implementations
//
// - SliceSource: Wraps a slice to act as a source.
// - Map: Transforms elements of one type into another.
// - Filter: Filters elements based on a predicate.
// - Reduce: Reduces an entire source to a single value.
// - ReduceTransformer: Incrementally reduces a source while emitting intermediate results.
//
// # Examples
//
// Example 1: Mapping and Filtering
//
// var result []string
//
// data := []int{1, 2, 3, 4, 5}
// source := NewSliceSource(data)
//
// squareMapper := NewMapper(source, func(n int) int { return n * n })     // Square each number
// filter := NewFilter(squareMapper, func(n int) bool { return n%2 == 0 }) // Keep even numbers
// toStrMapper := NewMapper(filter, strconv.Itoa)                          // Convert to string
// sink := NewSliceSink(&result)
//
// outResult, err := sink.Append(context.Background(), toStrMapper)
//
//	if err != nil {
//		return // Fail
//	}
//
// fmt.Println(strings.Join(*outResult, ", ")) // Output: 4, 16
//
// Example 2: Reducing
//
// data := []int{1, 2, 3, 4, 5}
// source := NewSliceSource(data)
// reducer := NewReducer(0, func(acc, next int) int { return acc + next })
// result, _ := reducer.Reduce(source)
// fmt.Println(result) // Output: 15
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Source represents a source of data that can pull elements one at a time.
type Source[T any] interface {
	Pull(ctx context.Context) (*T, error) // Returns the next element.
	Close()                               // Lets the consumer tell the source that no more data will be Pull()ed.
}

// sourceFunc is simple way to turn a lambda into a Source.
type sourceFunc[T any] struct {
	srcFunc   func(context.Context) (*T, error)
	closeFunc func()
}

// Pull proxies the call to the source lambda.
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

// Close tells the source no more data will be Pull()ed.
func (sf *sourceFunc[T]) Close() {
	if sf.closeFunc != nil {
		sf.closeFunc()
	}

	sf.srcFunc = nil
	sf.closeFunc = nil
}

// SourceFunc lets a lambda become a source.
func SourceFunc[T any](srcFunc func(context.Context) (*T, error), closeFunc func()) Source[T] {
	return &sourceFunc[T]{
		srcFunc:   srcFunc,
		closeFunc: closeFunc,
	}
}

// SliceSource is a producer backed by a slice of elements.
type SliceSource[T any] struct {
	data []T
}

// NewSliceSource creates a new SliceSource from a slice.
func NewSliceSource[T any](data []T) *SliceSource[T] {
	return &SliceSource[T]{data: data}
}

// Pull emits the next element from the slice or returns io.EOF if all elements are produced.
func (sp *SliceSource[T]) Pull(ctx context.Context) (*T, error) {
	if (ctx.Err() != nil) || (len(sp.data) < 1) {
		sp.Close() // Free unused slice

		return nil, io.EOF
	}

	value := sp.data[0]
	sp.data = sp.data[1:]

	return &value, nil
}

// Close tells the source no more data will be Pull()ed.
func (sp *SliceSource[T]) Close() {
	sp.data = nil
}

// SliceSink is simple way to capture the result of a source into a slice.
type SliceSink[T any] struct {
	dest *[]T
}

// NewSliceSink creates a new SliceSink from a slice pointer.
func NewSliceSink[T any](dest *[]T) *SliceSink[T] {
	return &SliceSink[T]{dest: dest}
}

// Append pulls all elements from the given source and appends them to the slice.
//
// Parameters:
// - input: The source from which elements are pulled.
//
// Returns:
// - A pointer to the resulting slice containing all pulled elements.
// - An error if the operation encounters unexpected errors.
//
// Notes:
// - The method processes all available elements in the source.
// - It stops when the source is exhausted, returning `io.EOF`.
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
				input.Close()

				return ss.dest, nil
			default:
				return nil, fmt.Errorf("data pull failed: %w", err)
			}
		}
		// if next != nil { // This should always be true in Blocking mode.
		*ss.dest = append(*ss.dest, *next) // }
	}
}

// Mapper applies a mapping function to a source, transforming TIn elements into TOut.
type Mapper[TIn, TOut any] struct {
	input Source[TIn]
	mapFn func(TIn) TOut
}

// NewMapper creates a new Mapper.
func NewMapper[TIn, TOut any](input Source[TIn], mapFn func(TIn) TOut) *Mapper[TIn, TOut] {
	return &Mapper[TIn, TOut]{input: input, mapFn: mapFn}
}

// Pull transforms the next input element using the mapping function.
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
			mt.Close()

			return nil, io.EOF
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	nextOut := mt.mapFn(*nextIn)

	return &nextOut, nil
}

// Close tells the source no more data will be Pull()ed.
func (mt *Mapper[TIn, TOut]) Close() {
	if mt.input != nil {
		mt.input.Close()
	}

	mt.input = nil
	mt.mapFn = nil
}

// Filter filters elements in a source based on a predicate.
type Filter[T any] struct {
	input     Source[T]
	predicate func(T) bool
}

// NewFilter creates a new Filter.
func NewFilter[T any](input Source[T], predicate func(T) bool) *Filter[T] {
	return &Filter[T]{input: input, predicate: predicate}
}

// Pull emits the next element that satisfies the predicate.
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
				ft.Close()

				return nil, io.EOF
			default:
				return nil, fmt.Errorf("data pull failed: %w", err)
			}
		}

		if ft.predicate(*next) {
			return next, nil
		}
	}
}

// Close tells the source no more data will be Pull()ed.
func (ft *Filter[T]) Close() {
	if ft.input != nil {
		ft.input.Close()
	}

	ft.input = nil
	ft.predicate = nil
}

// Taker limits the number of elements returned from a source.
//
// Taker provides a way to process only the first `n` elements of a stream,
// discarding the rest.
type Taker[T any] struct {
	input Source[T]
	left  int
}

// NewTaker creates a new Taker that only returns the first elCount elements.
func NewTaker[T any](input Source[T], elCount int) *Taker[T] {
	return &Taker[T]{input: input, left: elCount}
}

// Pull emits the next element that satisfies the predicate.
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
			tt.Close()

			return nil, io.EOF
		default:
			return nil, fmt.Errorf("data pull failed: %w", err)
		}
	}

	if tt.left <= 0 {
		tt.Close()

		return nil, io.EOF
	}

	tt.left--

	return next, nil
}

// Close tells the source no more data will be Pull()ed.
func (tt *Taker[T]) Close() {
	if tt.input != nil {
		tt.input.Close()
	}

	tt.input = nil
}

// NewDropper creates a new Taker that skips the first elCount elements.
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

// NewReduceTransformer creates a new ReduceTransformer.
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
			rt.closeInput()
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

// closeInput tells the source no more data will be Pull()ed but retains
// the remaining buffer to spool to consumer.
func (rt *ReduceTransformer[TIn, TOut]) closeInput() {
	if rt.input != nil {
		rt.input.Close()
	}

	rt.input = nil
}

// Close tells the source no more data will be Pull()ed.
func (rt *ReduceTransformer[TIn, TOut]) Close() {
	rt.closeInput()
	rt.buffer = nil
	rt.accumulator = nil
	rt.reducer = nil
}

// Reducer consumes an entire input source and reduces it to a single output value.
type Reducer[TIn, TOut any] struct {
	reducer    func(acc TOut, next TIn) TOut
	initialAcc TOut
}

// NewReducer creates a new Reducer with an initial accumulator value.
func NewReducer[TIn, TOut any](
	initialAcc TOut,
	reducer func(acc TOut, next TIn) TOut,
) *Reducer[TIn, TOut] {
	return &Reducer[TIn, TOut]{
		reducer:    reducer,
		initialAcc: initialAcc,
	}
}

// Reduce processes all elements from the input producer and returns the final reduced value.
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
