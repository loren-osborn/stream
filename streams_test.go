package streams_test

import (
	"fmt"
	"testing"

	"github.com/loren-osborn/streams"
)

// TestSliceSource validates the behavior of SliceSource.
func TestSliceSource(t *testing.T) {
	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)

	for i := 0; i < len(data); i++ {
		value, err := source.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if value == nil {
			t.Fatalf("expected %d, got nil", data[i])
		} else if *value != data[i] {
			t.Fatalf("expected %d, got %d", data[i], *value)
		}
	}

	v, err := source.Pull(streams.Blocking)
	if err != streams.EndOfData {
		t.Fatalf("expected EndOfData, got %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

// TestMapper validates the behavior of Mapper.
func TestMapper(t *testing.T) {
	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)
	mapper := streams.NewMapper(source, func(n int) int { return n * 2 }) // Double each value

	expected := []int{2, 4, 6, 8, 10}
	for i := 0; i < len(expected); i++ {
		value, err := mapper.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if value == nil {
			t.Fatalf("expected %d, got nil", expected[i])
		} else if *value != expected[i] {
			t.Fatalf("expected %d, got %d", expected[i], *value)
		}
	}

	v, err := mapper.Pull(streams.Blocking)
	if err != streams.EndOfData {
		t.Fatalf("expected EndOfData, got %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

// TestFilter validates the behavior of Filter.
func TestFilter(t *testing.T) {
	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)
	filter := streams.NewFilter(source, func(n int) bool { return n%2 == 0 }) // Even numbers

	expected := []int{2, 4}
	for i := 0; i < len(expected); i++ {
		value, err := filter.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if value == nil {
			t.Fatalf("expected %d, got nil", expected[i])
		} else if *value != expected[i] {
			t.Fatalf("expected %d, got %d", expected[i], *value)
		}
	}

	v, err := filter.Pull(streams.Blocking)
	if err != streams.EndOfData {
		t.Fatalf("expected EndOfData, got %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

// TestReducer validates the behavior of Reducer.
func TestReducer(t *testing.T) {
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
	data := []int{1, 2, 3, 4, 5}
	source := streams.NewSliceSource(data)

	itCount := 0
	reducer := func(acc []int, next int) ([]int, []int) {
		itCount += 1
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
	for i := 0; i < len(expected); i++ {
		value, err := transformer.Pull(streams.Blocking)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if value == nil {
			t.Fatalf("expected %d, got nil", expected[i])
		} else if *value != expected[i] {
			t.Fatalf("expected %d, got %d", expected[i], value)
		}
	}

	v, err := transformer.Pull(streams.Blocking)
	if err != streams.EndOfData {
		t.Fatalf("expected EndOfData, got %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
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
