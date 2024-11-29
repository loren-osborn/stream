package stream_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

const (
	DataPullErrorSrcErrStr = "data pull failed: source error"
)

// ErrTestSourceError is a synthetic source error used for testing.
var ErrTestSourceError = errors.New("source error")

// ErrTestOriginalError is a synthetic original error used for testing.
var ErrTestOriginalError = errors.New("original error")

// TestSliceSource validates the behavior of SliceSource.
func TestSliceSource(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	for _, expected := range data {
		value, err := source.Pull(stream.Blocking)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if value == nil {
			t.Errorf("expected %d, got nil", expected)
		} else if *value != expected {
			t.Errorf("expected %d, got %d", expected, *value)
		}
	}

	value, err := source.Pull(stream.Blocking)
	if !errors.Is(err, stream.ErrEndOfData) {
		t.Errorf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

// TestMapper validates the behavior of Mapper.
func TestMapper(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)
	mapper := stream.NewMapper(source, func(n int) int { return n * 2 }) // Double each value

	expected := []int{2, 4, 6, 8, 10}
	for _, exp := range expected {
		value, err := mapper.Pull(stream.Blocking)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, *value)
		}
	}

	value, err := mapper.Pull(stream.Blocking)
	if !errors.Is(err, stream.ErrEndOfData) {
		t.Errorf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

// TestFilter validates the behavior of Filter.
func TestFilter(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)
	filter := stream.NewFilter(source, func(n int) bool { return n%2 == 0 }) // Even numbers

	expected := []int{2, 4}
	for _, exp := range expected {
		value, err := filter.Pull(stream.Blocking)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, *value)
		}
	}

	value, err := filter.Pull(stream.Blocking)
	if !errors.Is(err, stream.ErrEndOfData) {
		t.Errorf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

// TestReducer validates the behavior of Reducer.
func TestReducer(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)
	consumer := stream.NewReducer(0, func(acc, next int) int { return acc + next })

	result, err := consumer.Reduce(source)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result != 15 { // Sum of 1 to 5
		t.Errorf("expected 15, got %d", result)
	}
}

// TestReduceTransformer validates the behavior of ReduceTransformer.
func TestReduceTransformer(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	itCount := 0
	reducer := func(acc []int, next int) ([]int, []int) {
		itCount++

		if len(acc) == 0 {
			acc = []int{0}
		}

		acc[0] += next
		if itCount%3 == 0 {
			return acc, nil
		}

		return nil, acc
	}

	transformer := stream.NewReduceTransformer(source, reducer)

	expected := []int{6, 9}
	for _, exp := range expected {
		value, err := transformer.Pull(stream.Blocking)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, value)
		}
	}

	value, err := transformer.Pull(stream.Blocking)
	if !errors.Is(err, stream.ErrEndOfData) {
		t.Errorf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

type sinkErrorTestCase struct {
	name          string
	sourceError   error
	expectedError error
}

func getSinkErrorTestCases() []sinkErrorTestCase {
	return []sinkErrorTestCase{
		{
			name:          "EndOfData",
			sourceError:   stream.ErrEndOfData,
			expectedError: nil,
		},
		{
			name:          "ErrNoDataYet not expected when blocking",
			sourceError:   stream.ErrNoDataYet,
			expectedError: fmt.Errorf("unexpected sentinel error: %w", stream.ErrNoDataYet),
		},
		{
			name:          "ErrorHandling",
			sourceError:   ErrTestOriginalError,
			expectedError: fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
		},
	}
}

type sinkOutputTestCase[T any] struct {
	name      string
	generator func(stream.Source[T]) (any, error) // returning any, because we only care about nil-ness
}

func getSinkOutputTestCase() []sinkOutputTestCase[int] {
	return []sinkOutputTestCase[int]{
		{
			name: "SliceSink",
			generator: func(src stream.Source[int]) (any, error) {
				dummyDest := []int{}
				sink := stream.NewSliceSink(&dummyDest)

				return sink.Append(src)
			},
		},
		{
			name: "Reducer",
			generator: func(src stream.Source[int]) (any, error) {
				reducer := stream.NewReducer(
					nil,
					func(acc []int, next int) []int {
						if acc == nil {
							acc = []int{0}
						}

						return []int{acc[0] + next}
					},
				)

				return reducer.Reduce(src)
			},
		},
	}
}

// TestSinkErrorHandling validates error handling in SliceSink.Append() and Reducer.Reduce().
func TestSinkErrorHandling(t *testing.T) {
	t.Parallel()

	for _, testCase := range CartesianProduct(getSinkErrorTestCases(), getSinkOutputTestCase()) {
		// range getSinkErrorTestCases() {
		t.Run(fmt.Sprintf("%s %s", testCase.Second.name, testCase.First.name), func(t *testing.T) {
			t.Parallel()

			source := stream.SourceFunc[int](func(block stream.BlockingType) (*int, error) {
				if block != stream.Blocking {
					t.Errorf("expected Pull(Blocking), got Pull(NonBlocking)")
				}

				return nil, testCase.First.sourceError
			})

			val, err := testCase.Second.generator(source)

			if testCase.First.expectedError != nil && !(reflect.ValueOf(val).IsNil()) {
				if err == nil {
					t.Errorf("Got non-nil value %v when error expected", val)
				} else {
					t.Errorf("Got non-nil value %v with non-nil error %v", val, err)
				}
			}

			switch {
			case testCase.First.expectedError == nil && err != nil:
				t.Errorf("Got non-nil error %v when none expected", err)
			case testCase.First.expectedError != nil && err == nil:
				t.Errorf("Got no error when expecting %v", testCase.First.expectedError)
			case testCase.First.expectedError != nil && err != nil:
				if testCase.First.expectedError.Error() != err.Error() {
					t.Errorf("Got error %v when expecting error %v", err, testCase.First.expectedError)
				}
			}
		})
	}
}

type transformErrorTestCase struct {
	name          string
	block         stream.BlockingType
	sourceError   error
	expectedError error
}

func getTransformErrorTestCases() []transformErrorTestCase {
	return []transformErrorTestCase{
		{
			name:          "EndOfData blocking",
			block:         stream.Blocking,
			sourceError:   stream.ErrEndOfData,
			expectedError: stream.ErrEndOfData,
		},
		{
			name:          "EndOfData non-blocking",
			block:         stream.NonBlocking,
			sourceError:   stream.ErrEndOfData,
			expectedError: stream.ErrEndOfData,
		},
		{
			name:          "ErrNoDataYet not expected when blocking",
			block:         stream.Blocking,
			sourceError:   stream.ErrNoDataYet,
			expectedError: fmt.Errorf("unexpected sentinel error: %w", stream.ErrNoDataYet),
		},
		{
			name:          "ErrNoDataYet non-blocking",
			block:         stream.NonBlocking,
			sourceError:   stream.ErrNoDataYet,
			expectedError: stream.ErrNoDataYet,
		},
		{
			name:          "ErrorHandling blocking",
			block:         stream.Blocking,
			sourceError:   ErrTestOriginalError,
			expectedError: fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
		},
		{
			name:          "ErrorHandling non-blocking",
			block:         stream.NonBlocking,
			sourceError:   ErrTestOriginalError,
			expectedError: fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
		},
	}
}

type transformerOutputTestCase[T any] struct {
	name      string
	generator func(stream.Source[T], stream.BlockingType) func() (*T, error)
}

func getTransformerIntOutputTestCase() []transformerOutputTestCase[int] {
	return []transformerOutputTestCase[int]{
		{
			name: "Mapper",
			generator: func(src stream.Source[int], blk stream.BlockingType) func() (*int, error) {
				mapper := stream.NewMapper(src, func(n int) int { return n * 2 })

				return func() (*int, error) {
					return mapper.Pull(blk)
				}
			},
		},
		{
			name: "Filter",
			generator: func(src stream.Source[int], blk stream.BlockingType) func() (*int, error) {
				filter := stream.NewFilter(src, func(n int) bool { return n%2 == 0 })

				return func() (*int, error) {
					return filter.Pull(blk)
				}
			},
		},
		{
			name: "Taker",
			generator: func(src stream.Source[int], blk stream.BlockingType) func() (*int, error) {
				taker := stream.NewTaker(src, 3)

				return func() (*int, error) {
					return taker.Pull(blk)
				}
			},
		},
		{
			name: "ReduceTransformer",
			generator: func(src stream.Source[int], blk stream.BlockingType) func() (*int, error) {
				reducer := func(acc []int, next int) ([]int, []int) {
					return append(acc, next), nil
				}
				transformer := stream.NewReduceTransformer(src, reducer)

				return func() (*int, error) {
					return transformer.Pull(blk)
				}
			},
		},
	}
}

// TestTransformerErrorHandling tests stream transformers for consistent error handling.
func TestTransformerErrorHandling(t *testing.T) {
	t.Parallel()

	for _, testCase := range CartesianProduct(getTransformErrorTestCases(), getTransformerIntOutputTestCase()) {
		t.Run(fmt.Sprintf("%s %s", testCase.Second.name, testCase.First.name), func(t *testing.T) {
			t.Parallel()

			if testCase.First.expectedError == nil {
				t.Errorf("BAD TEST: Should expect an error")
			}

			source := stream.SourceFunc[int](func(block stream.BlockingType) (*int, error) {
				if block != testCase.First.block {
					t.Errorf("expected Pull(%v), got Pull(%v)", testCase.First.block, block)
				}

				return nil, testCase.First.sourceError
			})

			puller := testCase.Second.generator(source, testCase.First.block)

			val, err := puller()

			if val != nil {
				if err == nil {
					t.Errorf("Got non-nil value %v when error expected", val)
				} else {
					t.Errorf("Got non-nil value %v with non-nil error %v", val, err)
				}
			}

			if err == nil || err.Error() != testCase.First.expectedError.Error() {
				t.Errorf("expected %v, got %v", testCase.First.expectedError, err)
			}
		})
	}
}

// Pair represents a pair of values of types A and B.
type Pair[A, B any] struct {
	First  A
	Second B
}

// CartesianProduct generates the Cartesian product of two slices and returns a slice of Pair structs.
func CartesianProduct[A, B any](a []A, b []B) []Pair[A, B] {
	result := make([]Pair[A, B], 0, len(a)*len(b))

	for _, elemA := range a {
		for _, elemB := range b {
			result = append(result, Pair[A, B]{First: elemA, Second: elemB})
		}
	}

	return result
}

// ExampleReducer demonstrates a complete pipeline of producers, transformers, and consumers.
func ExampleReducer() {
	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	mapper := stream.NewMapper(source, func(n int) int { return n * n })     // Square each number
	filter := stream.NewFilter(mapper, func(n int) bool { return n%2 == 0 }) // Keep even squares
	consumer := stream.NewReducer(0, func(acc, next int) int { return acc + next })

	result, _ := consumer.Reduce(filter)
	fmt.Println(result) // Output: 20
}
