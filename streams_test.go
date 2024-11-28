package streams_test

import (
	"errors"
	"fmt"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/streams"
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
	source := streams.NewSliceSource(data)

	for _, expected := range data {
		value, err := source.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if value == nil {
			t.Fatalf("expected %d, got nil", expected)
		} else if *value != expected {
			t.Fatalf("expected %d, got %d", expected, *value)
		}
	}

	value, err := source.Pull(streams.Blocking)
	if !errors.Is(err, streams.ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Fatalf("expected nil, got %v", value)
	}
}

// TestMapper validates the behavior of Mapper.
func TestMapper(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)
	mapper := streams.NewMapper(source, func(n int) int { return n * 2 }) // Double each value

	expected := []int{2, 4, 6, 8, 10}
	for _, exp := range expected {
		value, err := mapper.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if value == nil {
			t.Fatalf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Fatalf("expected %d, got %d", exp, *value)
		}
	}

	value, err := mapper.Pull(streams.Blocking)
	if !errors.Is(err, streams.ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Fatalf("expected nil, got %v", value)
	}
}

// TestFilter validates the behavior of Filter.
func TestFilter(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)
	filter := streams.NewFilter(source, func(n int) bool { return n%2 == 0 }) // Even numbers

	expected := []int{2, 4}
	for _, exp := range expected {
		value, err := filter.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if value == nil {
			t.Fatalf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Fatalf("expected %d, got %d", exp, *value)
		}
	}

	value, err := filter.Pull(streams.Blocking)
	if !errors.Is(err, streams.ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Fatalf("expected nil, got %v", value)
	}
}

// TestReducer validates the behavior of Reducer.
func TestReducer(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)
	consumer := streams.NewReducer(0, func(acc, next int) int { return acc + next })

	result, err := consumer.Reduce(source)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != 15 { // Sum of 1 to 5
		t.Fatalf("expected 15, got %d", result)
	}
}

// TestReduceTransformer validates the behavior of ReduceTransformer.
func TestReduceTransformer(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)

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

	transformer := streams.NewReduceTransformer(source, reducer)

	expected := []int{6, 9}
	for _, exp := range expected {
		value, err := transformer.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if value == nil {
			t.Fatalf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Fatalf("expected %d, got %d", exp, value)
		}
	}

	value, err := transformer.Pull(streams.Blocking)
	if !errors.Is(err, streams.ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}

	if value != nil {
		t.Fatalf("expected nil, got %v", value)
	}
}

// ExampleReducer demonstrates a complete pipeline of producers, transformers, and consumers.
func ExampleReducer() {
	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)

	mapper := streams.NewMapper(source, func(n int) int { return n * n })     // Square each number
	filter := streams.NewFilter(mapper, func(n int) bool { return n%2 == 0 }) // Keep even squares
	consumer := streams.NewReducer(0, func(acc, next int) int { return acc + next })

	result, _ := consumer.Reduce(filter)
	fmt.Println(result) // Output: 20
}

// TestDataPullError validates DataPullError formatting.
func TestDataPullError(t *testing.T) {
	t.Parallel()

	dataPullErr := &streams.DataPullError{Err: ErrTestOriginalError}

	expectedMsg := "Data pull failed: original error"
	if dataPullErr.Error() != expectedMsg {
		t.Fatalf("expected %q, got %q", expectedMsg, dataPullErr.Error())
	}
}

// TestSliceSinkAppendError validates error handling in SliceSink.Append.
func TestSliceSinkAppendError(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	sink := streams.NewSliceSink(&data)

	_, err := sink.Append(source)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Fatalf("expected data pull error, got %v", err)
	}
}

// TestMapperErrorHandling tests Mapper error handling when input source returns errors.
func TestMapperErrorHandling(t *testing.T) {
	t.Parallel()

	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	mapper := streams.NewMapper(source, func(n int) int { return n * 2 })

	_, err := mapper.Pull(streams.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Fatalf("expected data pull error, got %v", err)
	}
}

// TestFilterErrorHandling tests Filter error handling when input source returns errors.
func TestFilterErrorHandling(t *testing.T) {
	t.Parallel()

	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	filter := streams.NewFilter(source, func(n int) bool { return n%2 == 0 })

	_, err := filter.Pull(streams.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Fatalf("expected data pull error, got %v", err)
	}
}

// TestTakerErrorHandling tests error handling in Taker.
func TestTakerErrorHandling(t *testing.T) {
	t.Parallel()

	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, streams.ErrEndOfData
	})
	taker := streams.NewTaker(source, 3)

	_, err := taker.Pull(streams.Blocking)
	if !errors.Is(err, streams.ErrEndOfData) {
		t.Fatalf("expected ErrEndOfData, got %v", err)
	}
}

// TestReduceTransformerErrorHandling tests ReduceTransformer error handling.
func TestReduceTransformerErrorHandling(t *testing.T) {
	t.Parallel()

	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	reducer := func(acc []int, next int) ([]int, []int) {
		return append(acc, next), nil
	}
	transformer := streams.NewReduceTransformer(source, reducer)

	_, err := transformer.Pull(streams.Blocking)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Fatalf("expected data pull error, got %v", err)
	}
}

// TestReducerErrorHandling tests Reducer error handling when source returns errors.
func TestReducerErrorHandling(t *testing.T) {
	t.Parallel()

	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		return nil, ErrTestSourceError
	})
	reducer := streams.NewReducer(0, func(acc, next int) int { return acc + next })

	_, err := reducer.Reduce(source)
	if err == nil || err.Error() != DataPullErrorSrcErrStr {
		t.Fatalf("expected data pull error, got %v", err)
	}
}
