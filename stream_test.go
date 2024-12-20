package stream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

// ErrTestOriginalError is a synthetic original error used for testing.
var ErrTestOriginalError = errors.New("original error")

// TestSliceSource validates the behavior of SliceSource.
func TestSliceSource(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	for _, expected := range data {
		value, err := source.Pull(context.Background())
		assertError(t, err, nil)

		if value == nil {
			t.Errorf("expected %d, got nil", expected)
		} else if *value != expected {
			t.Errorf("expected %d, got %d", expected, *value)
		}
	}

	value, err := source.Pull(context.Background())
	assertError(t, err, io.EOF)

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
		value, err := mapper.Pull(context.Background())
		assertError(t, err, nil)

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, *value)
		}
	}

	value, err := mapper.Pull(context.Background())
	assertError(t, err, io.EOF)

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
		value, err := filter.Pull(context.Background())
		assertError(t, err, nil)

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, *value)
		}
	}

	value, err := filter.Pull(context.Background())
	assertError(t, err, io.EOF)

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

	result, err := consumer.Reduce(context.Background(), source)
	assertError(t, err, nil)

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
		value, err := transformer.Pull(context.Background())
		assertError(t, err, nil)

		if value == nil {
			t.Errorf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Errorf("expected %d, got %d", exp, value)
		}
	}

	value, err := transformer.Pull(context.Background())
	assertError(t, err, io.EOF)

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
			name:          "EOF",
			sourceError:   io.EOF,
			expectedError: nil,
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
	generator func(context.Context, stream.Source[T]) (any, error) // returning any, because we only care about nil-ness
}

func getSinkOutputTestCase() []sinkOutputTestCase[int] {
	return []sinkOutputTestCase[int]{
		{
			name: "SliceSink",
			generator: func(ctx context.Context, src stream.Source[int]) (any, error) {
				dummyDest := []int{}
				sink := stream.NewSliceSink(&dummyDest)

				return sink.Append(ctx, src)
			},
		},
		{
			name: "Reducer",
			generator: func(ctx context.Context, src stream.Source[int]) (any, error) {
				reducer := stream.NewReducer(
					nil,
					func(acc []int, next int) []int {
						if acc == nil {
							acc = []int{0}
						}

						return []int{acc[0] + next}
					},
				)

				return reducer.Reduce(ctx, src)
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

			outerCtx := context.Background()

			source := stream.SourceFunc[int](func(ctx context.Context) (*int, error) {
				if outerCtx != ctx {
					t.Errorf("Pulled contexts didn't match. Outer: %v, Inner: %v", outerCtx, ctx)
				}

				return nil, testCase.First.sourceError
			}, nil)

			val, err := testCase.Second.generator(outerCtx, source)

			if testCase.First.expectedError != nil && !(reflect.ValueOf(val).IsNil()) {
				if err == nil {
					t.Errorf("Got non-nil value %v when error expected", val)
				} else {
					t.Errorf("Got non-nil value %v with non-nil error %v", val, err)
				}
			}

			assertErrorString(t, err, testCase.First.expectedError)
		})
	}
}

type transformErrorTestCase struct {
	name          string
	ctxGen        func() context.Context
	sourceError   error
	expectedError error
}

func getTransformErrorTestCases() []transformErrorTestCase {
	return []transformErrorTestCase{
		{
			name:          "EOF blocking",
			ctxGen:        context.Background,
			sourceError:   io.EOF,
			expectedError: io.EOF,
		},
		{
			name:          "ErrorHandling blocking",
			ctxGen:        context.Background,
			sourceError:   ErrTestOriginalError,
			expectedError: fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
		},
	}
}

type transformerOutputTestCase[T any] struct {
	name      string
	generator func(stream.Source[T], context.Context) func() (*T, error)
}

func getTransformerIntOutputTestCase() []transformerOutputTestCase[int] {
	return []transformerOutputTestCase[int]{
		{
			name: "Mapper",
			generator: func(src stream.Source[int], ctx context.Context) func() (*int, error) {
				mapper := stream.NewMapper(src, func(n int) int { return n * 2 })

				return func() (*int, error) {
					return mapper.Pull(ctx)
				}
			},
		},
		{
			name: "Filter",
			generator: func(src stream.Source[int], ctx context.Context) func() (*int, error) {
				filter := stream.NewFilter(src, func(n int) bool { return n%2 == 0 })

				return func() (*int, error) {
					return filter.Pull(ctx)
				}
			},
		},
		{
			name: "Taker",
			generator: func(src stream.Source[int], ctx context.Context) func() (*int, error) {
				taker := stream.NewTaker(src, 3)

				return func() (*int, error) {
					return taker.Pull(ctx)
				}
			},
		},
		{
			name: "ReduceTransformer",
			generator: func(src stream.Source[int], ctx context.Context) func() (*int, error) {
				reducer := func(acc []int, next int) ([]int, []int) {
					return append(acc, next), nil
				}
				transformer := stream.NewReduceTransformer(src, reducer)

				return func() (*int, error) {
					return transformer.Pull(ctx)
				}
			},
		},
		{
			name: "Spooler",
			generator: func(src stream.Source[int], ctx context.Context) func() (*int, error) {
				spooler := stream.NewSpooler(src)

				return func() (*int, error) {
					return spooler.Pull(ctx)
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

			outerCtx := testCase.First.ctxGen()

			source := stream.SourceFunc[int](func(ctx context.Context) (*int, error) {
				if outerCtx != ctx {
					t.Errorf("Pulled contexts didn't match. Outer: %v, Inner: %v", outerCtx, ctx)
				}

				return nil, testCase.First.sourceError
			}, nil)

			puller := testCase.Second.generator(source, outerCtx)

			val, err := puller()

			if val != nil {
				if err == nil {
					t.Errorf("Got non-nil value %v when error expected", val)
				} else {
					t.Errorf("Got non-nil value %v with non-nil error %v", val, err)
				}
			}

			assertErrorString(t, err, testCase.First.expectedError)
		})
	}
}

// TestSourceFuncCancelCtx tests cancellation of a context while SourceFunc pulls from its lambda.
func TestSourceFuncCancelCtx(t *testing.T) {
	t.Parallel()

	sourceClosed := false
	ctx, cancel := context.WithCancel(context.Background())

	source := stream.SourceFunc(
		func(context.Context) (*int, error) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				t.Errorf("context %v should not yet be canceled; got %v", ctx, ctxErr)
			}

			defer cancel() // cancel context on exit

			outVal := 1

			if sourceClosed {
				t.Errorf("source should not be closed yet")
			}

			return &outVal, nil
		},
		func() {
			sourceClosed = true
		},
	)

	if sourceClosed {
		t.Errorf("source should not be closed yet")
	}

	resultPtr, outErr := source.Pull(ctx)

	if !sourceClosed {
		t.Errorf("source should now be closed")
	}

	if resultPtr != nil {
		t.Errorf("resultPtr should be nil, but was %v", *resultPtr)
	}

	assertErrorString(t, outErr, fmt.Errorf("operation canceled: %w", ctx.Err()))
}

// TestNewDropperBasic tests the basic functionality of the NewDropper function.
func TestNewDropperBasic(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5, 6, 7}
	source := stream.NewSliceSource(data)
	dropper := stream.NewDropper(source, 3) // Skip the first 3 elements

	expected := []int{4, 5, 6, 7}
	for _, exp := range expected {
		value, err := dropper.Pull(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if value == nil {
			t.Fatalf("expected %d, got nil", exp)
		} else if *value != exp {
			t.Fatalf("expected %d, got %d", exp, *value)
		}
	}

	value, err := dropper.Pull(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	if value != nil {
		t.Fatalf("expected nil, got %v", value)
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

// assertError validates that `got` is the error we `want`.
func assertError(t *testing.T, got, want error) {
	t.Helper()

	if want == nil {
		if got != nil {
			t.Errorf("expected no error, got %v", got)
		}
	} else {
		if !errors.Is(got, want) {
			t.Errorf("expected error %v, got %v", want, got)
		}
	}
}

// assertErrorString validates that `got` is the error we `want`.
func assertErrorString(t *testing.T, got, want error) {
	t.Helper()

	//nolint:nestif // *FIXME*: for now
	if want == nil {
		if got != nil {
			t.Errorf("expected no error, got %v", got)
		}
	} else {
		if !errors.Is(got, want) {
			if (got == nil) || (got.Error() != want.Error()) {
				t.Errorf("expected error %v, got %v", want, got)
			}
		}
	}
}

// ExampleReducer demonstrates a complete pipeline of producers, transformers, and consumers.
func ExampleReducer() {
	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	mapper := stream.NewMapper(source, func(n int) int { return n * n })     // Square each number
	filter := stream.NewFilter(mapper, func(n int) bool { return n%2 == 0 }) // Keep even squares
	consumer := stream.NewReducer(0, func(acc, next int) int { return acc + next })

	result, _ := consumer.Reduce(context.Background(), filter)
	fmt.Println(result) // Output: 20
}
