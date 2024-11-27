// Package streams provides tools for lazy evaluation and data transformation pipelines.
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
// - Reduce: Reduces an entire stream to a single value.
// - ReduceTransformer: Incrementally reduces a stream while emitting intermediate results.
//
// # Examples
//
// Example 1: Mapping and Filtering
//
//	data := []int{1, 2, 3, 4, 5}
//	source := NewSliceSource(data)
//
//	mapper := NewMap(source, func(n int) int { return n * n }) // Square each number
//	filter := NewFilter(mapper, func(n int) bool { return n%2 == 0 }) // Keep even numbers
//
//	for {
//	    value, err := filter.Pull(Blocking)
//	    if errors.Is(err, ErrEndOfData) {
//	        break
//	    }
//	    fmt.Println(value) // Output: 4, 16
//	}
//
// Example 2: Reducing
//
//	data := []int{1, 2, 3, 4, 5}
//	source := NewSliceSource(data)
//
//	reducer := func(acc, next int) int { return acc + next } // Summing reducer
//	finalizer := func(acc int) int { return acc }
//	consumer := NewReducer(0, reducer, finalizer)
//
//	result, _ := consumer.Consume(source)
//	fmt.Println(result) // Output: 15
package streams

import (
	"errors"
)

// BlockingType is used to indicate whether a Pull call should block or not.
type BlockingType bool

const (
	NonBlocking BlockingType = false
	Blocking    BlockingType = true
)

// ErrEndOfData is returned by Pull when the stream has no more data to produce.
var ErrEndOfData = errors.New("end of data")

// ErrNoDataYet is returned by Pull(NonBlocking) when the stream has no more data yet.
var ErrNoDataYet = errors.New("data not ready")

// // DataPullError is returned by Pull when the stream is not ErrEndOfData and
// // Pull()ing data results in an error.
// type DataPullError struct {
//     err error
// }

// // Error returns a helpful error message.
// func (e *DataPullError) Error() string {
// 	return fmt.Sprintf("Data pull failed: %v", e.err)
// }

// Source represents a source of data that can pull elements one at a time.
type Source[T any] interface {
	Pull(block BlockingType) (*T, error)
}

// SourceFunc lets a lambda become a source.
type SourceFunc[T any] func(BlockingType) (*T, error)

// Pull proxies the call to the source lambda.
func (sf SourceFunc[T]) Pull(blocks BlockingType) (*T, error) {
	return sf(blocks)
}

// SliceSource is a producer backed by a slice of elements.
type SliceSource[T any] struct {
	data []T
}

// NewSliceSource creates a new SliceSource from a slice.
func NewSliceSource[T any](data []T) *SliceSource[T] {
	return &SliceSource[T]{data: data}
}

// Pull emits the next element from the slice or returns ErrEndOfData if all elements are produced.
func (sp *SliceSource[T]) Pull(_ BlockingType) (*T, error) {
	if len(sp.data) < 1 {
		return nil, ErrEndOfData
	}

	value := sp.data[0]
	sp.data = sp.data[1:]

	return &value, nil
}

// SliceSink is simple way to capture the result of a source into a slice.
type SliceSink[T any] struct {
	dest *[]T
}

// NewSliceSink creates a new SliceSink from a slice pointer.
func NewSliceSink[T any](dest *[]T) *SliceSink[T] {
	return &SliceSink[T]{dest: dest}
}

// Reduce processes all elements from the input producer and returns the final reduced value.
func (ss *SliceSink[T]) Append(input Source[T]) (*[]T, error) {
	for {
		next, err := input.Pull(Blocking)
		if errors.Is(err, ErrEndOfData) {
			return ss.dest, nil
		}
		// if err != nil {
		// 	return &DataPullError{err:err}
		// }
		// if next != nil { // This should always be true in Blocking mode.
		*ss.dest = append(*ss.dest, *next) // }
	}
}

// Mapper applies a mapping function to a stream, transforming TIn elements into TOut.
type Mapper[TIn, TOut any] struct {
	input Source[TIn]
	mapFn func(TIn) TOut
}

// NewMapper creates a new Mapper.
func NewMapper[TIn, TOut any](input Source[TIn], mapFn func(TIn) TOut) *Mapper[TIn, TOut] {
	return &Mapper[TIn, TOut]{input: input, mapFn: mapFn}
}

// Pull transforms the next input element using the mapping function.
func (mt *Mapper[TIn, TOut]) Pull(block BlockingType) (*TOut, error) {
	nextIn, err := mt.input.Pull(block)
	if errors.Is(err, ErrEndOfData) {
		return nil, ErrEndOfData
	}
	// if err != nil {
	// 	return nil, &DataPullError{err:err}
	// }
	// if errors.Is(err, ErrNoDataYet) {
	// 	return nil, ErrNoDataYet
	// }
	nextOut := mt.mapFn(*nextIn)

	return &nextOut, nil
}

// Filter filters elements in a stream based on a predicate.
type Filter[T any] struct {
	input     Source[T]
	predicate func(T) bool
}

// NewFilter creates a new Filter.
func NewFilter[T any](input Source[T], predicate func(T) bool) *Filter[T] {
	return &Filter[T]{input: input, predicate: predicate}
}

// Pull emits the next element that satisfies the predicate.
func (ft *Filter[T]) Pull(block BlockingType) (*T, error) {
	for {
		next, err := ft.input.Pull(block)
		if errors.Is(err, ErrEndOfData) {
			return nil, ErrEndOfData
		}
		// if err != nil {
		// 	return nil, &DataPullError{err:err}
		// }
		// if errors.Is(err, ErrNoDataYet) {
		// 	return nil, ErrNoDataYet
		// }
		if ft.predicate(*next) {
			return next, nil
		}
	}
}

// ReduceTransformer applies a reduction function to a stream, producing finalized elements incrementally.
type ReduceTransformer[TIn, TOut any] struct {
	input       Source[TIn]
	reducer     func([]TOut, TIn) ([]TOut, []TOut)
	buffer      []TOut
	accumulator []TOut
	eod         error
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
		eod:         nil,
	}
}

// Pull generates the next finalized element from the reduction or returns ErrEndOfData when complete.
func (rt *ReduceTransformer[TIn, TOut]) Pull(block BlockingType) (*TOut, error) {
	if len(rt.buffer) > 0 {
		out := rt.buffer[0]
		rt.buffer = rt.buffer[1:]

		return &out, nil
	}

	if rt.eod != nil {
		return nil, rt.eod
	}

	next, err := rt.input.Pull(block)
	if errors.Is(err, ErrEndOfData) {
		rt.eod = err
		rt.buffer = rt.accumulator
		rt.accumulator = nil

		return rt.Pull(block)
	}
	// if err != nil {
	// 	return nil, err
	// }
	// if errors.Is(err, ErrNoDataYet) {
	// 	return nil, ErrNoDataYet
	// }
	rt.buffer, rt.accumulator = rt.reducer(rt.accumulator, *next)

	return rt.Pull(block)
}

// Reducer consumes an entire input stream and reduces it to a single output value.
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
func (rc *Reducer[TIn, TOut]) Reduce(input Source[TIn]) (TOut, error) {
	acc := rc.initialAcc

	for {
		next, err := input.Pull(Blocking)
		if errors.Is(err, ErrEndOfData) {
			return acc, nil
		}
		// if err != nil {
		// 	return * new(TOut), &DataPullError{err:err}
		// }
		// 	if next != nil { // In blocking mode, this should always be true.
		acc = rc.reducer(acc, *next) // 	}
	}
}
