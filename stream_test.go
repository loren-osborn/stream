package stream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	//nolint:depguard // We want to test the stream package.
	"github.com/loren-osborn/stream"
)

// ErrTestOriginalError is a synthetic original error used for testing.
var ErrTestOriginalError = errors.New("original error")

// ===================
//    Helper Logic
// ===================

// pullAndCheckSequence is a small helper that pulls from a Source[T] until
// it either finds an error or hits the end of `expected`. It then checks
// that the source returns (nil, io.EOF) afterward.
func pullAndCheckSequence[T comparable](t *testing.T, src stream.Source[T], expected []T) {
	t.Helper()

	for _, exp := range expected {
		value, err := src.Pull(context.Background())
		assertError(t, err, nil)

		if value == nil {
			t.Errorf("expected %v, got nil", exp)

			continue
		}

		if *value != exp {
			t.Errorf("expected %v, got %v", exp, *value)
		}
	}

	// Check that subsequent pull yields EOF
	value, err := src.Pull(context.Background())

	assertError(t, err, io.EOF)

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

// ===================
//  Basic Source Tests
// ===================

// TestSliceSource validates the behavior of SliceSource.
func TestSliceSource(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)
	pullAndCheckSequence(t, source, data)
}

// ================================
//  Example: Combined Transformers
// ================================
//
// Demonstrates how to unify single-value transformations (Mapper, Filter, etc.)
// into a single table-based test while preserving coverage checks.

func TestBasicTransformers(t *testing.T) {
	t.Parallel()

	type transformerTestCase struct {
		name     string
		data     []int
		makeSUT  func(stream.Source[int]) stream.Source[int]
		expected []int
	}

	testCases := []transformerTestCase{
		{
			name: "MapperDouble",
			data: []int{1, 2, 3, 4, 5},
			makeSUT: func(src stream.Source[int]) stream.Source[int] {
				return stream.NewMapper(src, func(n int) int {
					return n * 2
				})
			},
			expected: []int{2, 4, 6, 8, 10},
		},
		{
			name: "FilterEven",
			data: []int{1, 2, 3, 4, 5},
			makeSUT: func(src stream.Source[int]) stream.Source[int] {
				return stream.NewFilter(src, func(n int) bool {
					return n%2 == 0
				})
			},
			expected: []int{2, 4},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := stream.NewSliceSource(testCase.data)
			transformed := testCase.makeSUT(source)

			pullAndCheckSequence(t, transformed, testCase.expected)
		})
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

		// Return an accumulation every 3rd element, otherwise emit
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
			t.Errorf("expected %d, got %d", exp, *value)
		}
	}

	value, err := transformer.Pull(context.Background())

	assertError(t, err, io.EOF)

	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

// ErrorTestBehaviorFlags defines named behaviors and expectations for more readable test cases.
type ErrorTestBehaviorFlags int

// Behaviors for TestSinkErrorHandling and TestTransformerErrorHandling tests.
const (
	PreCancelContext ErrorTestBehaviorFlags = 1 << iota
	ExpectCloseInsteadOfPull
	ExpectPullCall
	CancelWithinPullCall
	DeferCancelFromPullCall
	ExpectCloseAfterPull
	ExpectPredicateCall
	CancelWithinPredicateCall
	ExpectCloseAfterPredicate
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

// getErrorTestCases returns a list of test cases for error handling behaviors.
//
//nolint:funlen,gocognit,cyclop // test scaffolding
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
					defer func() {
						if flags&DeferCancelFromPullCall != 0 {
							cancel()
						}
					}()

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
						t.Fatalf("Runaway execution detected in Pull().")
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
					if flags&ExpectPullCall != 0 && pullCallCount == 0 {
						t.Errorf("Expected Pull() call but none was made")
					}
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
	name         string
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
			continue // Invalid test combination
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
				// We expect no error
				assertErrorString(t, err, nil)

				return
			}

			// We expect an error
			if !reflect.ValueOf(val).IsNil() {
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

//nolint:funlen // test code
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

			defer cancel()

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
func (rsf *rawSourceFunc[T]) Pull(ctx context.Context) (*T, error) {
	return rsf.srcFunc(ctx)
}

// Close releases the resources associated with the source function.
func (rsf *rawSourceFunc[T]) Close() error {
	return rsf.closeFunc()
}

// HelperCancelCtxOnPull tests cancellation of a context from a given puller factory.
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
			defer cancel()

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
		sink := stream.NewSliceSink[int](&destSlice)

		return func(ctx context.Context) (*[]int, error) {
			return sink.Append(ctx, src)
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

// TestReduceTransformerCancelCtx tests cancellation while ReduceTransformer pulls.
func TestReduceTransformerCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		reduceTransformer := stream.NewReduceTransformer(
			src,
			func(last []int, next int) ([]int, []int) {
				return append(last, next), []int{}
			},
		)

		return func(ctx context.Context) (*int, error) {
			return reduceTransformer.Pull(ctx)
		}
	})
}

// TestReducerCancelCtx tests cancellation while Reducer pulls from its source.
func TestReducerCancelCtx(t *testing.T) {
	t.Parallel()

	HelperCancelCtxOnPull(t, func(src stream.Source[int]) func(ctx context.Context) (*int, error) {
		reducer := stream.NewReducer[int, int](0, func(acc, next int) int {
			return acc + next
		})

		return func(ctx context.Context) (*int, error) {
			val, err := reducer.Reduce(ctx, src)
			if val != 0 {
				t.Errorf("val should be 0, but was %v", val)
			}

			return nil, err //nolint: wrapcheck // testing
		}
	})
}

// TestNewDropperBasic tests the basic functionality of the NewDropper function.
func TestNewDropperBasic(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5, 6, 7}
	source := stream.NewSliceSource(data)
	dropper := stream.NewDropper(source, 3) // Skip first 3 elements

	expected := []int{4, 5, 6, 7}
	pullAndCheckSequence(t, dropper, expected)
}

// Pair represents a pair of values of types A and B.
type Pair[A, B any] struct {
	First  A
	Second B
}

// CartesianProduct generates the Cartesian product of two slices.
func CartesianProduct[A, B any](a []A, b []B) []Pair[A, B] {
	result := make([]Pair[A, B], 0, len(a)*len(b))

	for _, elemA := range a {
		for _, elemB := range b {
			result = append(result, Pair[A, B]{First: elemA, Second: elemB})
		}
	}

	return result
}

// assertError validates that `got` matches `want` via errors.Is.
func assertError(t *testing.T, got, want error) {
	t.Helper()

	if want == nil {
		if got != nil {
			t.Errorf("expected no error, got %v", got)
		}

		return
	}

	if !errors.Is(got, want) {
		t.Errorf("expected error %v, got %v", want, got)
	}
}

// assertErrorString checks for error equality or wrapping by comparing messages.
func assertErrorString(t *testing.T, got, want error) {
	t.Helper()

	switch {
	case want == nil && got != nil:
		t.Errorf("expected no error, got %#v", got)
	case want == nil && got == nil:
		// OK, no error expected and none got
	case want != nil && !errors.Is(got, want):
		if got == nil || got.Error() != want.Error() {
			t.Errorf("expected error \n\t%#v, got \n\t%#v", want, got)
		}
	}
}

// TestTakerCloseOnEOF verifies that Taker closes its source after hitting the item limit or EOF.
//
//nolint:funlen // test code
func TestTakerCloseOnEOF(t *testing.T) {
	t.Parallel()

	type mockSourceData struct {
		items      []int
		closed     bool
		pullIndex  int
		closeCalls int
	}

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

	taker := stream.NewTaker(mock, 3)
	ctx := context.Background()

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

	item, err := taker.Pull(ctx)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	if item != nil {
		t.Fatalf("expected nil, got %v", item)
	}

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

// TestSourceFuncErrorWrapping verifies that SourceFunc properly handles wrapped errors.
func TestSourceFuncErrorWrapping(t *testing.T) {
	t.Parallel()

	src := stream.SourceFunc(
		func(_ context.Context) (*int, error) {
			return nil, wrappedError{inner: errOriginal}
		},
		nil,
	)

	item, err := src.Pull(context.Background())
	if item != nil {
		t.Fatalf("expected item to be nil, got %v", item)
	}

	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	var we wrappedError
	if !errors.As(err, &we) {
		t.Fatalf("expected error to be of type wrappedError, got %T", err)
	}

	if !errors.Is(err, errOriginal) {
		t.Fatalf("expected error to wrap errOriginal, but it did not")
	}
}

// TestNewTakeAndDropWhile tests the behavior of TakeWhile and DropWhile.
//
//nolint:funlen,gocognit // test code
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
			testablesIdx := testablesIdxLoop
			testablesCase := testablesCaseLoop
			testCase := tcLoop

			t.Run(strings.Join([]string{testablesCase.name, testCase.name}, " "), func(t *testing.T) {
				t.Parallel()

				source := stream.NewSliceSource(testCase.input)
				sysUnderTest := testablesCase.underTest(source, testCase.predicate)

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

				if !equalSlices(result, testCase.expected[testablesIdx]) {
					t.Fatalf("expected %v, got %v", testCase.expected[testablesIdx], result)
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
//nolint:funlen // test code
func TestSourceCloseError(t *testing.T) {
	t.Parallel()

	mockGen := func(fn1 func(), ip *int, err error) stream.Source[int] {
		return &rawSourceFunc[int]{
			srcFunc: func(context.Context) (*int, error) {
				fn1()

				return ip, err
			},
			closeFunc: func() error {
				return errTestCloseError
			},
		}
	}

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
						return errTestCloseError
					},
				)
			},
			eofImpliesClosed: false,
			hasPredicate:     false,
		},
		{
			name: "Mapper",
			srcGen: func(fn1 func(), fn2 func(), ip *int, err error) stream.Source[int] {
				return stream.NewMapper(mockGen(fn1, ip, err), func(v int) int {
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
				return stream.NewFilter(mockGen(fn1, ip, err), func(_ int) bool {
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
				return stream.NewTakeWhile(mockGen(fn1, ip, err), func(_ int) bool {
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
				return stream.NewReduceTransformer(mockGen(fn1, ip, err), func(acc []int, next int) ([]int, []int) {
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
				return stream.NewSpooler(mockGen(fn1, ip, err))
			},
			eofImpliesClosed: true,
			hasPredicate:     false,
		},
	}

	one := 1

	for _, ts := range testSources {
		testSrc := ts

		t.Run(testSrc.name+": Error closing after EOF", func(t *testing.T) {
			t.Parallel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)

			val, err := source.Pull(context.Background())
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			expectedWrappedErr := fmt.Errorf(
				"error closing source: %w",
				errors.Join(fmt.Errorf("error closing source: %w", errTestCloseError), io.EOF),
			)

			if testSrc.eofImpliesClosed {
				expectedWrappedErr = io.EOF
			}

			assertErrorString(t, err, expectedWrappedErr)
		})

		t.Run(testSrc.name+": Error closing after canceled context", func(t *testing.T) {
			t.Parallel()

			cancelableCtx, cancel := context.WithCancel(context.Background())
			source := testSrc.srcGen(cancel, func() {}, nil, io.EOF)

			val, err := source.Pull(cancelableCtx)
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			expectedWrappedErr := fmt.Errorf(
				"error closing source while canceling: %w",
				errors.Join(fmt.Errorf("error closing source: %w", errTestCloseError), context.Canceled),
			)
			assertErrorString(t, err, expectedWrappedErr)
		})

		t.Run(testSrc.name+": Error closing after precanceled context", func(t *testing.T) {
			t.Parallel()

			cancelableCtx, cancel := context.WithCancel(context.Background())
			cancel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)

			val, err := source.Pull(cancelableCtx)
			if val != nil {
				t.Errorf("expected nil value, got %v", val)
			}

			expectedWrappedErr := fmt.Errorf(
				"error closing source while canceling: %w",
				errors.Join(fmt.Errorf("error closing source: %w", errTestCloseError), context.Canceled),
			)

			assertErrorString(t, err, expectedWrappedErr)
		})

		if testSrc.hasPredicate {
			t.Run(testSrc.name+": Error closing after canceled context in predicate", func(t *testing.T) {
				t.Parallel()

				cancelableCtx, cancel := context.WithCancel(context.Background())
				source := testSrc.srcGen(func() {}, cancel, &one, nil)

				val, err := source.Pull(cancelableCtx)
				if val != nil {
					t.Errorf("expected nil value, got %v", val)
				}

				expectedWrappedErr := fmt.Errorf(
					"error closing source while canceling: %w",
					errors.Join(fmt.Errorf("error closing source: %w", errTestCloseError), context.Canceled),
				)
				assertErrorString(t, err, expectedWrappedErr)
			})
		}

		t.Run(testSrc.name+": Error directly closing", func(t *testing.T) {
			t.Parallel()

			source := testSrc.srcGen(func() {}, func() {}, nil, io.EOF)
			err := source.Close()
			expectedDirectErr := fmt.Errorf("error closing source: %w", errTestCloseError)

			assertErrorString(t, err, expectedDirectErr)
		})
	}
}

// TestPreCanceledSliceSource ensures pre-canceled context returns the appropriate error.
func TestPreCanceledSliceSource(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	source := stream.NewSliceSource(data)

	cancelableCtx, cancel := context.WithCancel(context.Background())
	cancel()

	expectedError := fmt.Errorf("operation canceled: %w", context.Canceled)

	val, err := source.Pull(cancelableCtx)
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	assertErrorString(t, err, expectedError)
}

// TestTakerTerminationCloseErr checks behavior when Taker closes a source with an error.
func TestTakerTerminationCloseErr(t *testing.T) {
	t.Parallel()

	source := &rawSourceFunc[int]{
		srcFunc: func(context.Context) (*int, error) {
			one := 1

			return &one, nil
		},
		closeFunc: func() error {
			return errTestCloseError
		},
	}

	taker := stream.NewTaker(source, 0)
	expectedErr := fmt.Errorf(
		"error closing source: %w",
		errors.Join(fmt.Errorf("error closing source: %w", errTestCloseError), io.EOF),
	)

	val, err := taker.Pull(context.Background())
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	assertErrorString(t, err, expectedErr)
}

// TestPreCanceledSinkCloseErr checks behavior when a sink closes a pre-canceled context.
func TestPreCanceledSinkCloseErr(t *testing.T) {
	t.Parallel()

	source := &rawSourceFunc[int]{
		srcFunc: func(context.Context) (*int, error) {
			one := 1

			return &one, nil
		},
		closeFunc: func() error {
			return errTestCloseError
		},
	}

	cancelableCtx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("SliceSink", func(t *testing.T) {
		t.Parallel()

		dummyDest := []int{}
		sink := stream.NewSliceSink(&dummyDest)
		expectedError := fmt.Errorf(
			"error closing source while canceling: %w",
			errors.Join(errTestCloseError, context.Canceled),
		)

		val, err := sink.Append(cancelableCtx, source)
		if val != nil {
			t.Errorf("expected nil, got %v", val)
		}

		assertErrorString(t, err, expectedError)
	})

	t.Run("Reducer", func(t *testing.T) {
		t.Parallel()

		sink := stream.NewReducer(0, func(acc, next int) int { return acc + next })
		expectedError := fmt.Errorf(
			"error closing source while canceling: %w",
			errors.Join(errTestCloseError, context.Canceled),
		)

		val, err := sink.Reduce(cancelableCtx, source)
		if val != 0 {
			t.Errorf("expected nil, got %v", val)
		}

		assertErrorString(t, err, expectedError)
	})
}

// equalSlices is a helper function to compare two slices for equality.
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

	mapper := stream.NewMapper(source, func(n int) int {
		return n * n
	})

	filter := stream.NewFilter(mapper, func(n int) bool {
		return n%2 == 0
	})

	consumer := stream.NewReducer(0, func(acc, next int) int {
		return acc + next
	})

	result, _ := consumer.Reduce(context.Background(), filter)
	fmt.Println(result) // Output: 20
}
