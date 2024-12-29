package stream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
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

// ErrorTestBehaviorFlags defines named behviours and expectations for more readable test cases.
type ErrorTestBehaviorFlags int

// Define behaviors and expectations for TestSinkErrorHandling and TestTransformerErrorHandling tests.
const (
	PreCancelContext         ErrorTestBehaviorFlags = 1 << iota // Cancel context before starting test
	ExpectCloseInsteadOfPull                                    // Expect Close() to be called instead of Pull()
	ExpectPullCall                                              // Expect Pull() to be called
	// CancelWithinPullCall indicates context should cancel (Pull() should return cancelation error).
	CancelWithinPullCall
	DeferCancelFromPullCall   // Pull() will return dummy data with nil error, but cancel context in defer.
	ExpectCloseAfterPull      // Expect Close to be called after return from Pull()
	ExpectPredicateCall       // Expect Predicate to be called (if there is one)
	CancelWithinPredicateCall // Predicate will cancel context (by calling predicateLambda).
	ExpectCloseAfterPredicate // Expect Close() to be called after predicate
)

type emittedLambdas struct {
	ctxGen          func() context.Context
	pullLambda      func(context.Context) (*int, error)
	closeLambda     func() error
	predicateLambda func()
	finalLambda     func()
}

type errorTestCase struct {
	name              string
	lambdaEmitter     func(*testing.T) *emittedLambdas
	expectedError     error
	expectedSinkError bool
	needPredicate     bool
}

//nolint:funlen,gocognit,cyclop // **FIXME**
func getErrorTestCases(t *testing.T) []errorTestCase {
	t.Helper()

	type contextKeyTestName struct{}

	lambdaEmitterFactory := func(flags ErrorTestBehaviorFlags, pullErr error) func(*testing.T) *emittedLambdas {
		return func(t *testing.T) *emittedLambdas {
			t.Helper()

			testName := t.Name()
			cancelableCtx, cancel := context.WithCancel(context.Background())
			finalCtx := context.WithValue(cancelableCtx, contextKeyTestName{}, testName)
			pullCallCount := 0
			closeCallCount := 0
			predicateCallCount := 0

			if flags&PreCancelContext != 0 {
				cancel()
			}

			return &emittedLambdas{
				ctxGen: func() context.Context {
					return finalCtx
				},
				pullLambda: func(ctx context.Context) (*int, error) {
					if (flags&ExpectCloseInsteadOfPull != 0) || (flags&ExpectPullCall == 0) {
						t.Errorf("In Pull() call when not expected")
					}

					if flags&CancelWithinPullCall != 0 {
						cancel()
					}

					defer (func() {
						if flags&DeferCancelFromPullCall != 0 {
							cancel()
						}
					})()

					pullCallCount++
					if pullCallCount > 1 {
						t.Errorf("Pull() unexpectedly called %d times.", pullCallCount)
					}

					if closeCallCount > 0 {
						t.Errorf("Close() unexpectedly called (%d times) before Pull().", closeCallCount)
					}

					if predicateCallCount > 0 {
						t.Errorf("predicate unexpectedly called (%d times) before Pull().", predicateCallCount)
					}

					if pullCallCount+closeCallCount+predicateCallCount > 16 {
						t.Fatalf("Runaway execution detected Pull().")
					}

					anyTestName := ctx.Value(contextKeyTestName{})
					if innerTestName, ok := anyTestName.(string); !ok || testName != innerTestName {
						t.Errorf("Didn't get expected context %v, got %v instead", finalCtx, ctx)
					}

					if pullErr == nil {
						val := 1

						return &val, nil
					}

					return nil, pullErr
				},
				closeLambda: func() error {
					if flags&(ExpectCloseInsteadOfPull|ExpectCloseAfterPull|ExpectCloseAfterPredicate) == 0 {
						t.Errorf("In Close() call when not expected")
					}

					closeCallCount++

					if closeCallCount > 1 {
						t.Errorf("Close() unexpectedly called %d times (more than one).", closeCallCount)
					}

					return nil
				},
				predicateLambda: func() {
					predicateCallCount++

					if predicateCallCount > 1 {
						t.Errorf("predicate unexpectedly called %d times (more than one).", predicateCallCount)
					}

					if flags&CancelWithinPredicateCall != 0 {
						cancel()
					}
				},
				finalLambda: func() {
					expectedCloseCalls := 0
					if flags&(ExpectCloseInsteadOfPull|ExpectCloseAfterPull|ExpectCloseAfterPredicate) != 0 {
						expectedCloseCalls = 1
					}
					if closeCallCount != expectedCloseCalls {
						t.Errorf("Close() called %d times when %d expected", closeCallCount, expectedCloseCalls)
					}

					// Ensure we actually called Pull() if ExpectPullCall is set.
					if flags&ExpectPullCall != 0 && pullCallCount == 0 {
						t.Errorf("Expected Pull() call but none was made")
					}

					// Ensure we actually called Predicate if ExpectPredicateCall is set.
					if flags&ExpectPredicateCall != 0 && predicateCallCount == 0 {
						t.Errorf("Expected predicate call but none was made")
					}
				},
			}
		}
	}

	return []errorTestCase{
		{
			name:              "Pre-canceled",
			lambdaEmitter:     lambdaEmitterFactory(PreCancelContext|ExpectCloseInsteadOfPull, nil),
			expectedError:     fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedSinkError: true,
			needPredicate:     false,
		},
		{
			name:              "Canceled In Pull with unwrapped Error",
			lambdaEmitter:     lambdaEmitterFactory(ExpectPullCall|CancelWithinPullCall|ExpectCloseAfterPull, context.Canceled),
			expectedError:     fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedSinkError: true,
			needPredicate:     false,
		},
		{
			name: "Canceled In Pull with wrapped Error",
			lambdaEmitter: lambdaEmitterFactory(
				ExpectPullCall|CancelWithinPullCall|ExpectCloseAfterPull,
				fmt.Errorf("operation canceled: %w", context.Canceled),
			),
			expectedError:     fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedSinkError: true,
			needPredicate:     false,
		},
		{
			name:              "Canceled after Pull return",
			lambdaEmitter:     lambdaEmitterFactory(ExpectPullCall|DeferCancelFromPullCall|ExpectCloseAfterPull, nil),
			expectedError:     fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedSinkError: true,
			needPredicate:     false,
		},
		{
			name: "Canceled in predicate",
			lambdaEmitter: lambdaEmitterFactory(
				ExpectPullCall|ExpectPredicateCall|CancelWithinPredicateCall|ExpectCloseAfterPredicate,
				nil,
			),
			expectedError:     fmt.Errorf("operation canceled: %w", context.Canceled),
			expectedSinkError: true,
			needPredicate:     true,
		},
		{
			name:              "EOF",
			lambdaEmitter:     lambdaEmitterFactory(ExpectPullCall, io.EOF),
			expectedError:     io.EOF,
			expectedSinkError: false,
			needPredicate:     false,
		},
		{
			name:              "ErrorHandling",
			lambdaEmitter:     lambdaEmitterFactory(ExpectPullCall, ErrTestOriginalError),
			expectedError:     fmt.Errorf("data pull failed: %w", ErrTestOriginalError),
			expectedSinkError: true,
			needPredicate:     false,
		},
	}
}

type sinkOutputTestCase[T any] struct {
	name string
	// returning any, because we only care about nil-ness
	generator    func(context.Context, stream.Source[T], func()) (any, error)
	hasPredicate bool
}

func getSinkOutputTestCase() []sinkOutputTestCase[int] {
	return []sinkOutputTestCase[int]{
		{
			name: "SliceSink",
			generator: func(ctx context.Context, src stream.Source[int], _ func()) (any, error) {
				dummyDest := []int{}
				sink := stream.NewSliceSink(&dummyDest)

				return sink.Append(ctx, src)
			},
			hasPredicate: false,
		},
		{
			name: "Reducer",
			generator: func(ctx context.Context, src stream.Source[int], inPredicate func()) (any, error) {
				reducer := stream.NewReducer(
					nil,
					func(acc []int, next int) []int {
						inPredicate()
						if acc == nil {
							acc = []int{0}
						}

						return []int{acc[0] + next}
					},
				)

				return reducer.Reduce(ctx, src)
			},
			hasPredicate: true,
		},
	}
}

// TestSinkErrorHandling validates error handling in SliceSink.Append() and Reducer.Reduce().
func TestSinkErrorHandling(t *testing.T) {
	t.Parallel()

	for _, tCase := range CartesianProduct(getErrorTestCases(t), getSinkOutputTestCase()) {
		testCase := tCase
		if testCase.First.needPredicate && !testCase.Second.hasPredicate {
			continue // Invalid test
		}

		t.Run(fmt.Sprintf("%s %s", testCase.Second.name, testCase.First.name), func(t *testing.T) {
			t.Parallel()

			lambdas := testCase.First.lambdaEmitter(t)
			outerCtx := lambdas.ctxGen()
			source := &rawSourceFunc[int]{
				srcFunc:   lambdas.pullLambda,
				closeFunc: lambdas.closeLambda,
			}

			val, err := testCase.Second.generator(outerCtx, source, lambdas.predicateLambda)

			lambdas.finalLambda()

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
	name         string
	generator    func(stream.Source[T], context.Context, func()) func() (*T, error)
	hasPredicate bool
}

//nolint:funlen // **FIXME**
func getTransformerIntOutputTestCase() []transformerOutputTestCase[int] {
	return []transformerOutputTestCase[int]{
		{
			name: "Mapper",
			generator: func(src stream.Source[int], ctx context.Context, inPredicate func()) func() (*int, error) {
				mapper := stream.NewMapper(src, func(n int) int {
					inPredicate()

					return n * 2
				})

				return func() (*int, error) {
					return mapper.Pull(ctx)
				}
			},
			hasPredicate: true,
		},
		{
			name: "Filter",
			generator: func(src stream.Source[int], ctx context.Context, inPredicate func()) func() (*int, error) {
				filter := stream.NewFilter(src, func(n int) bool {
					inPredicate()

					return n%2 != 0
				})

				return func() (*int, error) {
					return filter.Pull(ctx)
				}
			},
			hasPredicate: true,
		},
		{
			name: "Dropper",
			generator: func(src stream.Source[int], ctx context.Context, _ func()) func() (*int, error) {
				taker := stream.NewDropper(src, 3)

				return func() (*int, error) {
					return taker.Pull(ctx)
				}
			},
			hasPredicate: false,
		},
		{
			name: "DropWhile",
			generator: func(src stream.Source[int], ctx context.Context, inPredicate func()) func() (*int, error) {
				taker := stream.NewDropWhile(src, func(val int) bool {
					inPredicate()

					return val < 3
				})

				return func() (*int, error) {
					return taker.Pull(ctx)
				}
			},
			hasPredicate: true,
		},
		{
			name: "Taker",
			generator: func(src stream.Source[int], ctx context.Context, _ func()) func() (*int, error) {
				taker := stream.NewTaker(src, 3)

				return func() (*int, error) {
					return taker.Pull(ctx)
				}
			},
			hasPredicate: false,
		},
		{
			name: "TakerWhile",
			generator: func(src stream.Source[int], ctx context.Context, inPredicate func()) func() (*int, error) {
				taker := stream.NewTakeWhile(src, func(val int) bool {
					inPredicate()

					return val < 3
				})

				return func() (*int, error) {
					return taker.Pull(ctx)
				}
			},
			hasPredicate: true,
		},
		{
			name: "ReduceTransformer",
			generator: func(src stream.Source[int], ctx context.Context, inPredicate func()) func() (*int, error) {
				reducer := func(acc []int, next int) ([]int, []int) {
					inPredicate()

					return append(acc, next), nil
				}
				transformer := stream.NewReduceTransformer(src, reducer)

				return func() (*int, error) {
					return transformer.Pull(ctx)
				}
			},
			hasPredicate: true,
		},
		{
			name: "Spooler",
			generator: func(src stream.Source[int], ctx context.Context, _ func()) func() (*int, error) {
				spooler := stream.NewSpooler(src)

				return func() (*int, error) {
					return spooler.Pull(ctx)
				}
			},
			hasPredicate: false,
		},
	}
}

// TestTransformerErrorHandling tests stream transformers for consistent error handling.
func TestTransformerErrorHandling(t *testing.T) {
	t.Parallel()

	for _, tCase := range CartesianProduct(getErrorTestCases(t), getTransformerIntOutputTestCase()) {
		testCase := tCase
		if testCase.First.needPredicate && !testCase.Second.hasPredicate {
			continue // Invalid test
		}

		t.Run(fmt.Sprintf("%s %s", testCase.Second.name, testCase.First.name), func(t *testing.T) {
			t.Parallel()

			if testCase.First.expectedError == nil {
				t.Errorf("BAD TEST: Should expect an error")
			}

			lambdas := testCase.First.lambdaEmitter(t)
			outerCtx := lambdas.ctxGen()
			source := &rawSourceFunc[int]{
				srcFunc:   lambdas.pullLambda,
				closeFunc: lambdas.closeLambda,
			}

			puller := testCase.Second.generator(source, outerCtx, lambdas.predicateLambda)

			val, err := puller()

			lambdas.finalLambda()

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
		func() error {
			sourceClosed = true

			return nil
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
	closeFunc func() error
}

// Pull calls the underlying source function to retrieve the next element.
// It returns an error if the context is canceled or the source is exhausted.
func (rsf *rawSourceFunc[T]) Pull(ctx context.Context) (*T, error) {
	return rsf.srcFunc(ctx)
}

// Close releases the resources associated with the source function.
func (rsf *rawSourceFunc[T]) Close() error {
	return rsf.closeFunc()
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
		closeFunc: func() error {
			sourceClosed = true

			return nil
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

			return nil, err //nolint:wrapcheck // mock
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
			t.Errorf("expected no error, got %#v", got)
		}
	} else {
		if !errors.Is(got, want) {
			if (got == nil) || (got.Error() != want.Error()) {
				t.Errorf("expected error \n\t%#v, got \n\t%#v", want, got)
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
		closeFunc: func() error {
			if mockData.closed {
				panic("Close called multiple times")
			}

			mockData.closed = true
			mockData.closeCalls++

			return nil
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

// wrappedError is a custom error type for wrapping another error.
type wrappedError struct {
	inner error
}

func (we wrappedError) Error() string {
	return "wrapped error: " + we.inner.Error()
}

func (we wrappedError) Unwrap() error {
	return we.inner
}

var errOriginal = errors.New("original error")

func TestSourceFuncErrorWrapping(t *testing.T) {
	t.Parallel()

	// Create a SourceFunc with a srcFunc that wraps its error.
	src := stream.SourceFunc(func(_ context.Context) (*int, error) {
		return nil, wrappedError{inner: errOriginal}
	}, nil)

	// Call Pull and assert behavior
	item, err := src.Pull(context.Background())

	// Ensure no item is returned
	if item != nil {
		t.Fatalf("expected item to be nil, got %v", item)
	}

	// Ensure an error is returned
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	// Ensure the error is of type wrappedError
	var we wrappedError
	if !errors.As(err, &we) {
		t.Fatalf("expected error to be of type wrappedError, got %T", err)
	}

	// Ensure the error wraps the original error
	if !errors.Is(err, errOriginal) {
		t.Fatalf("expected error to wrap errOriginal, but it did not")
	}
}

//nolint:funlen,gocognit // **FIXME**
func TestNewTakeAndDropWhile(t *testing.T) {
	t.Parallel()

	testables := []struct {
		name      string
		underTest func(stream.Source[int], func(int) bool) stream.Source[int]
	}{
		{
			name: "TakeWhile",
			underTest: func(src stream.Source[int], pred func(int) bool) stream.Source[int] {
				return stream.NewTakeWhile(src, pred)
			},
		},
		{
			name: "DropWhile",
			underTest: func(src stream.Source[int], pred func(int) bool) stream.Source[int] {
				return stream.NewDropWhile(src, pred)
			},
		},
	}

	tests := []struct {
		name      string
		input     []int
		predicate func(int) bool
		expected  [][]int
	}{
		{
			name:      "Basic operation",
			input:     []int{1, 2, 3, 4, 5},
			predicate: func(n int) bool { return n < 4 },
			expected:  [][]int{{1, 2, 3}, {4, 5}},
		},
		{
			name:      "All pass",
			input:     []int{1, 2, 3},
			predicate: func(_ int) bool { return true },
			expected:  [][]int{{1, 2, 3}, {}},
		},
		{
			name:      "None pass",
			input:     []int{1, 2, 3},
			predicate: func(_ int) bool { return false },
			expected:  [][]int{{}, {1, 2, 3}},
		},
	}

	for testablesIdxLoop, testablesCaseLoop := range testables {
		for _, tcLoop := range tests {
			// Shadow copies:
			testablesIdx := testablesIdxLoop
			testablesCase := testablesCaseLoop
			testcCase := tcLoop

			t.Run(strings.Join([]string{testablesCase.name, testcCase.name}, " "), func(t *testing.T) {
				t.Parallel()

				// Create a source and the SUT transformer
				source := stream.NewSliceSource(testcCase.input)
				sysUnderTest := testablesCase.underTest(source, testcCase.predicate)

				ctx := context.Background()

				var result []int

				for {
					item, err := sysUnderTest.Pull(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						t.Fatalf("unexpected error: %v", err)
					}

					if item != nil {
						result = append(result, *item)
					}
				}

				if !equalSlices(result, testcCase.expected[testablesIdx]) {
					t.Fatalf("expected %v, got %v", testcCase.expected[testablesIdx], result)
				}

				item, err := sysUnderTest.Pull(ctx)
				if item != nil || !errors.Is(err, io.EOF) {
					t.Fatalf("expected EOF, got item=%v, err=%v", item, err)
				}
			})
		}
	}
}

var errTestCloseError = errors.New("test close error")

// TestSourceCloseError validates error propagation from Close() in SourceFunc.
//
//nolint:funlen // **FIXME**
func TestSourceCloseError(t *testing.T) {
	t.Parallel()

	expectedError := errTestCloseError

	testSources := []struct {
		name             string
		srcGen           func(func(), func(), *int, error) stream.Source[int]
		eofImpliesClosed bool
		hasPredicate     bool
	}{
		{
			name: "SourceFunc",
			srcGen: func(fn func(), _ func(), ip *int, err error) stream.Source[int] {
				return stream.SourceFunc(
					func(context.Context) (*int, error) {
						fn()

						return ip, err
					},
					func() error {
						return expectedError
					},
				)
			},
			eofImpliesClosed: false,
			hasPredicate:     false,
		},
		{
			name: "Mapper",
			srcGen: func(fn1 func(), fn2 func(), ip *int, err error) stream.Source[int] {
				mockSrc := &rawSourceFunc[int]{
					srcFunc: func(context.Context) (*int, error) {
						fn1()

						return ip, err
					},
					closeFunc: func() error {
						return expectedError
					},
				}

				return stream.NewMapper(mockSrc, func(v int) int {
					fn2()

					return v + 1
				})
			},
			eofImpliesClosed: true,
			hasPredicate:     true,
		},
		{
			name: "Filter",
			srcGen: func(fn1 func(), fn2 func(), ip *int, err error) stream.Source[int] {
				mockSrc := &rawSourceFunc[int]{
					srcFunc: func(context.Context) (*int, error) {
						fn1()

						return ip, err
					},
					closeFunc: func() error {
						return expectedError
					},
				}

				return stream.NewFilter(mockSrc, func(_ int) bool {
					fn2()

					return true
				})
			},
			eofImpliesClosed: true,
			hasPredicate:     true,
		},
		{
			name: "TakeWhile",
			srcGen: func(fn1 func(), fn2 func(), ip *int, err error) stream.Source[int] {
				mockSrc := &rawSourceFunc[int]{
					srcFunc: func(context.Context) (*int, error) {
						fn1()

						return ip, err
					},
					closeFunc: func() error {
						return expectedError
					},
				}

				return stream.NewTakeWhile(mockSrc, func(_ int) bool {
					fn2()

					return true
				})
			},
			eofImpliesClosed: true,
			hasPredicate:     true,
		},
		{
			name: "ReduceTransformer",
			srcGen: func(fn1 func(), fn2 func(), ip *int, err error) stream.Source[int] {
				mockSrc := &rawSourceFunc[int]{
					srcFunc: func(context.Context) (*int, error) {
						fn1()

						return ip, err
					},
					closeFunc: func() error {
						return expectedError
					},
				}

				return stream.NewReduceTransformer(mockSrc, func(acc []int, next int) ([]int, []int) {
					fn2()

					return append(acc, next), nil
				})
			},
			eofImpliesClosed: true,
			hasPredicate:     true,
		},
		{
			name: "Spool",
			srcGen: func(fn1 func(), _ func(), ip *int, err error) stream.Source[int] {
				mockSrc := &rawSourceFunc[int]{
					srcFunc: func(context.Context) (*int, error) {
						fn1()

						return ip, err
					},
					closeFunc: func() error {
						return expectedError
					},
				}

				return stream.NewSpooler(mockSrc)
			},
			eofImpliesClosed: true,
			hasPredicate:     false,
		},
	}

	one := 1

	for _, ts := range testSources {
		testSrc := ts

		t.Run((testSrc.name + ": Error closing after EOF"), func(t *testing.T) {
			t.Parallel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)

			// Pull should trigger Close() since it returns EOF
			val, err := source.Pull(context.Background())
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			// Error should be wrapped
			expectedWrappedErr := fmt.Errorf(
				"error closing source: %w",
				errors.Join(fmt.Errorf("error closing source: %w", expectedError), io.EOF))

			if testSrc.eofImpliesClosed {
				// Instead, we're testing that the Pull() caller received the EOF, assumed
				// the Source[T] was already closed, so didn't call Close() again, and
				// didn't emit the error.
				expectedWrappedErr = io.EOF
			}

			assertErrorString(t, err, expectedWrappedErr)
		})

		t.Run((testSrc.name + ": Error closing after canceled context"), func(t *testing.T) {
			t.Parallel()

			// Pull should trigger Close() since it cancels the context
			cancelableCtx, cancel := context.WithCancel(context.Background())
			source := testSrc.srcGen(cancel, func() {}, nil, io.EOF)

			val, err := source.Pull(cancelableCtx)
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			// Error should be wrapped
			expectedWrappedErr := fmt.Errorf(
				"error closing source while canceling: %w",
				errors.Join(fmt.Errorf("error closing source: %w", expectedError), context.Canceled))
			assertErrorString(t, err, expectedWrappedErr)
		})

		t.Run((testSrc.name + ": Error closing after precanceled context"), func(t *testing.T) {
			t.Parallel()

			// Pull should trigger Close() since it cancels the context
			cancelableCtx, cancel := context.WithCancel(context.Background())

			cancel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)

			val, err := source.Pull(cancelableCtx)
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			// Error should be wrapped
			expectedWrappedErr := fmt.Errorf(
				"error closing source while canceling: %w",
				errors.Join(fmt.Errorf("error closing source: %w", expectedError), context.Canceled))
			assertErrorString(t, err, expectedWrappedErr)
		})

		if testSrc.hasPredicate {
			t.Run((testSrc.name + ": Error closing after canceled context in predicate"), func(t *testing.T) {
				t.Parallel()

				// Pull should trigger Close() since it cancels the context
				cancelableCtx, cancel := context.WithCancel(context.Background())
				source := testSrc.srcGen(func() {}, cancel, &one, nil)

				val, err := source.Pull(cancelableCtx)
				if val != nil {
					t.Errorf("expected nil value, got %v", val)
				}

				// Error should be wrapped
				expectedWrappedErr := fmt.Errorf(
					"error closing source while canceling: %w",
					errors.Join(fmt.Errorf("error closing source: %w", expectedError), context.Canceled))
				assertErrorString(t, err, expectedWrappedErr)
			})
		}

		t.Run((testSrc.name + ": Error directly closing"), func(t *testing.T) {
			t.Parallel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)

			// Call Close directly
			err := source.Close()
			expectedDirectErr := fmt.Errorf("error closing source: %w", expectedError)
			assertErrorString(t, err, expectedDirectErr)
		})
	}
}

// Helper function to compare two slices for equality.
func equalSlices[T comparable](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
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
