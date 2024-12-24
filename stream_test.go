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

type errorTestCase struct {
	name              string
	sourceError       error
	expectedError     error
	expectedSinkError bool
}

func getErrorTestCases() []errorTestCase {
	return []errorTestCase{
		{
			name:              "EOF",
			sourceError:       io.EOF,
			expectedError:     io.EOF,
			expectedSinkError: false,
		},
		{
			name:              "ErrorHandling",
			sourceError:       ErrTestOriginalError,
			expectedError:     fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
			expectedSinkError: true,
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

	for _, testCase := range CartesianProduct(getErrorTestCases(), getSinkOutputTestCase()) {
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

			if !testCase.First.expectedSinkError {
				assertErrorString(t, err, nil)

				return
			}

			if !(reflect.ValueOf(val).IsNil()) {
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

	for _, testCase := range CartesianProduct(getErrorTestCases(), getTransformerIntOutputTestCase()) {
		t.Run(fmt.Sprintf("%s %s", testCase.Second.name, testCase.First.name), func(t *testing.T) {
			t.Parallel()

			if testCase.First.expectedError == nil {
				t.Errorf("BAD TEST: Should expect an error")
			}

			outerCtx := context.Background()

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

// rawSourceFunc is a Source implementation backed by raw function calls.
type rawSourceFunc[T any] struct {
	srcFunc   func(context.Context) (*T, error)
	closeFunc func()
}

// Pull calls the underlying source function to retrieve the next element.
// It returns an error if the context is canceled or the source is exhausted.
func (rsf *rawSourceFunc[T]) Pull(ctx context.Context) (*T, error) {
	return rsf.srcFunc(ctx)
}

// Close releases the resources associated with the source function.
func (rsf *rawSourceFunc[T]) Close() {
	rsf.closeFunc()
}

// HelperCancelCtxOnPull tests cancellation of a context while something from a source.
func HelperCancelCtxOnPull[TOut any](
	t *testing.T,
	pullerFactory func(src stream.Source[int]) func(ctx context.Context) (*TOut, error),
) {
	t.Helper()

	sourceClosed := false
	ctx, cancel := context.WithCancel(context.Background())

	source := &rawSourceFunc[int]{
		srcFunc: func(context.Context) (*int, error) {
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
		closeFunc: func() {
			sourceClosed = true
		},
	}

	puller := pullerFactory(source)

	if sourceClosed {
		t.Errorf("source should not be closed yet")
	}

	resultPtr, outErr := puller(ctx)

	if !sourceClosed {
		t.Errorf("source should now be closed")
	}

	if resultPtr != nil {
		t.Errorf("resultPtr should be nil, but was %v", *resultPtr)
	}

	assertErrorString(t, outErr, fmt.Errorf("operation canceled: %w", ctx.Err()))
}

// TestSliceSinkCancelCtx tests cancellation of a context while SliceSink pulls from its source.
func TestSliceSinkCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*[]int, error) {
		destSlice := make([]int, 0, 4)

		slicesink := stream.NewSliceSink[int](&destSlice)

		return func(ctx context.Context) (*[]int, error) {
			return slicesink.Append(ctx, src)
		}
	})
}

// TestMapperCancelCtx tests cancellation of a context while Mapper pulls from its source.
func TestMapperCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		mapper := stream.NewMapper(src, func(a int) int { return a })

		return func(ctx context.Context) (*int, error) {
			return mapper.Pull(ctx)
		}
	})
}

// TestFilterCancelCtx tests cancellation of a context while Filter pulls from its source.
func TestFilterCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		filter := stream.NewFilter(src, func(_ int) bool { return true })

		return func(ctx context.Context) (*int, error) {
			return filter.Pull(ctx)
		}
	})
}

// TestTakerCancelCtx tests cancellation of a context while Taker pulls from its source.
func TestTakerCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		taker := stream.NewTaker(src, 5)

		return func(ctx context.Context) (*int, error) {
			return taker.Pull(ctx)
		}
	})
}

// TestReduceTransformerCancelCtx tests cancellation of a context while ReduceTransformer pulls from its source.
func TestReduceTransformerCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		reduceTransformer := stream.NewReduceTransformer(
			src,
			func(last []int, next int) ([]int, []int) { return append(last, next), []int{} },
		)

		return func(ctx context.Context) (*int, error) {
			return reduceTransformer.Pull(ctx)
		}
	})
}

// TestReducerCancelCtx tests cancellation of a context while Reducer pulls from its source.
func TestReducerCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		reducer := stream.NewReducer[int, int](0, func(acc int, next int) int { return acc + next })

		return func(ctx context.Context) (*int, error) {
			val, err := reducer.Reduce(ctx, src)

			if val != 0 {
				t.Errorf("val should be 0, but was %v", val)
			}

			return nil, err //nolint:wrapcheck
		}
	})
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

//nolint:funlen // **FIXME**
func TestTakerCloseOnEOF(t *testing.T) {
	t.Parallel()

	// Define mockSourceData to track Close() calls.
	type mockSourceData struct {
		items      []int
		closed     bool
		pullIndex  int
		closeCalls int
	}

	// Create a mock source with 5 items
	mockData := &mockSourceData{
		items:      []int{1, 2, 3, 4, 5},
		closed:     false,
		pullIndex:  0,
		closeCalls: 0,
	}

	mock := &rawSourceFunc[int]{
		srcFunc: func(_ context.Context) (*int, error) {
			if mockData.closed {
				panic("Pull called on a closed source")
			}

			if mockData.pullIndex >= len(mockData.items) {
				return nil, io.EOF
			}

			item := mockData.items[mockData.pullIndex]
			mockData.pullIndex++

			return &item, nil
		},
		closeFunc: func() {
			if mockData.closed {
				panic("Close called multiple times")
			}

			mockData.closed = true
			mockData.closeCalls++
		},
	}

	// Create a Taker that limits to 3 items
	taker := stream.NewTaker(mock, 3)

	ctx := context.Background()

	// Pull items and verify outputs
	expectedItems := []int{1, 2, 3}
	for _, expected := range expectedItems {
		item, err := taker.Pull(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if item == nil || *item != expected {
			t.Fatalf("expected %d, got %v", expected, item)
		}
	}

	// Verify taker emits EOF and calls Close() on the source
	item, err := taker.Pull(ctx)

	if !errors.Is(err, io.EOF) { // Correctly check for EOF with errors.Is
		t.Fatalf("expected EOF, got %v", err)
	}

	if item != nil {
		t.Fatalf("expected nil, got %v", item)
	}

	// Check if the mock source Close() was called exactly once
	if !mockData.closed {
		t.Fatalf("source was not closed")
	}

	if mockData.closeCalls != 1 {
		t.Fatalf("expected Close() to be called once, got %d calls", mockData.closeCalls)
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
