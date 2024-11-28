package stream_test

import (
	"errors"
	"fmt"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

const (
	DataPullErrorSrcErrStr = "Data pull failed: source error"
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

// TestDataPullError validates DataPullError formatting.
func TestDataPullError(t *testing.T) {
	t.Parallel()

	dataPullErr := &stream.DataPullError{Err: ErrTestOriginalError}

	expectedMsg := "Data pull failed: original error"
	if dataPullErr.Error() != expectedMsg {
		t.Errorf("expected %q, got %q", expectedMsg, dataPullErr.Error())
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
			expectedError: &stream.DataPullError{Err: stream.ErrNoDataYet},
		},
		{
			name:          "ErrorHandling",
			sourceError:   ErrTestOriginalError,
			expectedError: &stream.DataPullError{Err: ErrTestOriginalError},
		},
	}
}

// TestSliceSinkAppendError validates error handling in SliceSink.Append.
func TestSliceSinkAppendError(t *testing.T) {
	t.Parallel()

	for _, testCase := range getSinkErrorTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := stream.SourceFunc[int](func(block stream.BlockingType) (*int, error) {
				if block != stream.Blocking {
					t.Errorf("expected Pull(Blocking), got Pull(NonBlocking)")
				}

				return nil, testCase.sourceError
			})

			dummyDest := []int{}
			sink := stream.NewSliceSink(&dummyDest)

			val, err := sink.Append(source)

			if testCase.expectedError != nil && val != nil {
				if err == nil {
					t.Errorf("Got non-nil value %v when error expected", val)
				} else {
					t.Errorf("Got non-nil value %v with non-nil error %v", val, err)
				}
			}

			switch {
			case testCase.expectedError == nil && err != nil:
				t.Errorf("Got non-nil error %v when none expected", err)
			case testCase.expectedError != nil && err == nil:
				t.Errorf("Got no error when expecting %v", testCase.expectedError)
			case testCase.expectedError != nil && err != nil:
				if testCase.expectedError.Error() != err.Error() {
					t.Errorf("Got error %v when expecting error %v", err, testCase.expectedError)
				}
			}
		})
	}
}

// TestMapperErrorHandling tests Mapper error handling when input source returns errors.
func TestMapperErrorHandling(t *testing.T) {
	t.Parallel()

	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	mapper := stream.NewMapper(source, func(n int) int { return n * 2 })

	_, err := mapper.Pull(stream.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Errorf("expected data pull error, got %v", err)
	}
}

// TestFilterErrorHandling tests Filter error handling when input source returns errors.
func TestFilterErrorHandling(t *testing.T) {
	t.Parallel()

	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	filter := stream.NewFilter(source, func(n int) bool { return n%2 == 0 })

	_, err := filter.Pull(stream.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Errorf("expected data pull error, got %v", err)
	}
}

// TestTakerErrorHandling tests error handling in Taker.
func TestTakerErrorHandling(t *testing.T) {
	t.Parallel()

	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		return nil, stream.ErrEndOfData
	})
	taker := stream.NewTaker(source, 3)

	_, err := taker.Pull(stream.Blocking)
	if !errors.Is(err, stream.ErrEndOfData) {
		t.Errorf("expected ErrEndOfData, got %v", err)
	}
}

// TestReduceTransformerErrorHandling tests ReduceTransformer error handling.
func TestReduceTransformerErrorHandling(t *testing.T) {
	t.Parallel()

	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	reducer := func(acc []int, next int) ([]int, []int) {
		return append(acc, next), nil
	}
	transformer := stream.NewReduceTransformer(source, reducer)

	_, err := transformer.Pull(stream.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Errorf("expected data pull error, got %v", err)
	}
}

// TestReducerErrorHandling tests Reducer error handling when source returns errors.
func TestReducerErrorHandling(t *testing.T) {
	t.Parallel()

	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	reducer := stream.NewReducer(0, func(acc, next int) int { return acc + next })

	_, err := reducer.Reduce(source)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Errorf("expected data pull error, got %v", err)
	}
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
