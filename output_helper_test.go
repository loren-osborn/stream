package stream_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	//nolint:depguard // We are testing the package below.
	"github.com/loren-osborn/stream"
)

// MockCloser is a mock implementation of io.Closer for testing.
//
// Close() increments CloseCalls atomically.
type MockCloser struct {
	CloseCalls int32
	CustomData string // Demonstrates type-specific fields
	ResultErr  error
}

func (m *MockCloser) Close() error {
	atomic.AddInt32(&m.CloseCalls, 1)

	return m.ResultErr
}

// GetCloseCalls is a helper to safely retrieve CloseCalls in tests.
func (m *MockCloser) GetCloseCalls() int32 {
	return atomic.LoadInt32(&m.CloseCalls)
}

func assertProperHelperCleanup(t *testing.T, helper *stream.MultiOutputHelper[*MockCloser], numManagers int) {
	t.Helper()

	if err := helper.Close(); err != nil {
		t.Errorf("EXPECTED nil error, but got: %#v", err)
	}

	for i := range numManagers {
		if status := helper.ManagerState(i); status != stream.MOHelperClosed {
			t.Errorf("EXPECTED manager %d to be MOHelperClosed, but saw: %v", i, status)
		}
	}
}

// TestHelperWithGenericClosers tests multiple managers with an io.Closer.
//nolint: funlen,gocognit,cyclop // **FIXME**
func TestHelperWithGenericClosers(t *testing.T) {
	t.Parallel()

	const numManagers = 5

	type managerItem struct {
		closer   *MockCloser
		ctx      *context.Context
		cancelFn func()
	}

	managerInfo := make([]managerItem, numManagers)

	// Create closers for each manager
	helper := stream.NewMultiOutputHelper(numManagers, func(index int) *MockCloser {
		closer := &MockCloser{
			CloseCalls: 0,
			CustomData: fmt.Sprintf("Manager %d", index),
			ResultErr:  nil,
		}
		managerInfo[index].closer = closer

		return closer
	})

	t.Logf("Created MultiOutputHelper with %d output managers", numManagers)

	// 1) Initially, all managers should be uninitialized
	expectedBeforeState := stream.MOHelperUninitialized

	for range 2 {
		for index := range numManagers {
			if state := helper.ManagerState(index); state != expectedBeforeState {
				t.Fatalf("EXPECTED manager %d to be %v, saw %v", index, expectedBeforeState, state)
			}

			ctx, cancel := context.WithCancel(context.Background())
			managerInfo[index].ctx = &ctx
			managerInfo[index].cancelFn = cancel

			if err := helper.ManagerSetContext(index, ctx); err != nil {
				t.Fatalf("EXPECTED to set context for manager %d: failed: %v", index, err)
			}

			t.Logf("Set initial cancelable context for manager %d", index)

			if state := helper.ManagerState(index); state != stream.MOHelperWaiting {
				t.Fatalf("EXPECTED manager %d to be MOHelperWaiting, saw %d", index, state)
			}
		}
		// second iteration, expect MOHelperWaiting
		expectedBeforeState = stream.MOHelperWaiting
	}

	// 2) Test each manager closing in turn. We expect that all managers with
	//    index <= 'index' are closed afterwards.
	for index, outer := range managerInfo {
		// We use two different ways to close:
		if index%2 == 0 {
			// Canceling the context should cause it to close.
			outer.cancelFn()
			t.Logf("Canceled context for manager %d", index)

			// Expecting response as soon as we switch contexts:
			// being generous with 10 millsec delay
			time.Sleep(10 * time.Millisecond)
		} else {
			// Or explicitly calling ManagerClose
			if err := helper.ManagerClose(index); err != nil {
				t.Errorf("EXPECTED nil error closing manager %d, got %v", index, err)
			}

			t.Logf("Closed manager %d", index)
		}

		if state := helper.ManagerState(index); state != stream.MOHelperClosed {
			t.Errorf("EXPECTED manager %d to be %v, saw %v", index, stream.MOHelperClosed, state)
		} else {
			t.Logf("Manager %d met expectation status %v", index, state)
		}

		// Now verify that managers [0..index] are closed, and managers [index+1..end] are still waiting.
		for innerIndex := range numManagers {
			state := helper.ManagerState(innerIndex)
			closerCalls := managerInfo[innerIndex].closer.GetCloseCalls()

			expectedState := stream.MOHelperWaiting
			unexpectedCallsFn := func(v int32) bool { return v != 0 }
			expectedCallsString := "not to be called yet"

			if innerIndex <= index {
				expectedState = stream.MOHelperClosed
				unexpectedCallsFn = func(v int32) bool { return v < 1 }
				expectedCallsString = "to be called at least once"
			}

			if state != expectedState {
				t.Errorf("EXPECTED manager %d to be %v, saw %v", innerIndex, expectedState, state)
			}
			// Because we called close one way or another, we expect 1 close
			// for that manager.
			if unexpectedCallsFn(closerCalls) {
				t.Errorf("EXPECTED closer %d %s, got %d", innerIndex, expectedCallsString, closerCalls)
			}
		}
	}

	assertProperHelperCleanup(t, helper, numManagers)
}

// TestHelperConsensusWithGenericClosers verifies that the consensus context
// is canceled only when all outputs are closed.
func TestHelperConsensusWithGenericClosers(t *testing.T) {
	t.Parallel()

	const numManagers = 3

	helper := stream.NewMultiOutputHelper(numManagers, func(_ int) *MockCloser {
		return &MockCloser{
			CloseCalls: 0,
			CustomData: "foo",
			ResultErr:  nil,
		}
	})

	consensusCtx := helper.ConsensusContext()

	// Initially, it should not be canceled
	if consensusCtx.Err() != nil {
		t.Fatalf("EXPECTED consensus context to be active, found err=%v", consensusCtx.Err())
	}

	// Close each manager in turn, waiting for it to finish, then check consensus
	for index := range numManagers {
		// Before close, should still be active
		if consensusCtx.Err() != nil {
			t.Fatalf("EXPECTED consensusCtx to still be active, got err=%v [Manager %d]", consensusCtx.Err(), index)
		}

		// Close manager index
		if err := helper.ManagerClose(index); err != nil {
			t.Errorf("EXPECTED no error closing manager %d, got %v", index, err)
		}
	}

	// After all are closed, the consensusCtx should be canceled
	if ctxErr := consensusCtx.Err(); ctxErr == nil || !errors.Is(ctxErr, context.Canceled) {
		t.Errorf("EXPECTED consensusCtx to be canceled with %v, got %v", context.Canceled, ctxErr)
	}

	assertProperHelperCleanup(t, helper, numManagers)
}

// TestEdgeCases checks behavior for out-of-bound indices, repeated closes, etc.
//nolint: funlen,cyclop // **README**
func TestEdgeCases(t *testing.T) {
	t.Parallel()

	newHelper := func(t *testing.T) *stream.MultiOutputHelper[*MockCloser] {
		t.Helper()

		result := stream.NewMultiOutputHelper(2, func(_ int) *MockCloser {
			return &MockCloser{
				CloseCalls: 0,
				CustomData: "foo",
				ResultErr:  nil,
			}
		})

		return result
	}

	// Helper to test panics
	expectPanic := func(t *testing.T, funcThatPanics func(), expected string) {
		t.Helper()

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("EXPECTED panic: %s, got none", expected)
			} else if msg, ok := r.(string); !ok || msg != expected {
				t.Errorf("EXPECTED panic: %s, got: %v", expected, r)
			}
		}()
		funcThatPanics()
	}

	// Attempt to set context on invalid index
	t.Run("ManagerSetContext panics on invalid index", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		expectPanic(t, func() {
			_ = helper.ManagerSetContext(99, context.Background())
		}, "invalid manager index: 99")
	})

	// Attempt to set context on invalid index
	t.Run("ManagerSetContext panics on nil context", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		expectPanic(t, func() {
			_ = helper.ManagerSetContext(0, nil)
		}, "Attempting to set invalid nil context for outputID 0")
	})

	// Attempt to close invalid index
	t.Run("ManagerClose panics on invalid index", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		expectPanic(t, func() {
			_ = helper.ManagerClose(99)
		}, "invalid manager index: 99")
	})

	// Setting context on a valid manager
	t.Run("ManagerSetContext does not panic on valid index", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		err := helper.ManagerSetContext(0, context.Background()) // Should not panic
		if err != nil {
			t.Errorf("EXPECTED nil error, but got: %#v", err)
		}

		assertProperHelperCleanup(t, helper, 2)
	})

	// Closing manager 0
	t.Run("ManagerClose does not panic on valid index", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		err := helper.ManagerClose(0) // Should not panic
		if err != nil {
			t.Errorf("EXPECTED nil error, but got: %#v", err)
		}

		assertProperHelperCleanup(t, helper, 2)
	})

	// Closing manager 0 with proxy
	t.Run("ManagerClose does not panic on valid index", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)
		manager := helper.Manager(0)

		err := manager.Close()
		if err != nil {
			t.Errorf("EXPECTED nil error, but got: %#v", err)
		}

		expectPanic(t, func() {
			_ = helper.Manager(99)
		}, "invalid manager index: 99")

		assertProperHelperCleanup(t, helper, 2)
	})

	// Closing manager 0
	t.Run("ManagerClose returnes wrapped error", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		(*helper.ManagerOutput(0)).ResultErr = errTestCloseError

		err := helper.ManagerClose(0) // Should not panic
		expectedWrappedErr := fmt.Errorf(
			"error closing output: %w",
			errTestCloseError,
		)

		assertErrorString(t, err, expectedWrappedErr)

		secondCloseErr := helper.ManagerClose(0) // Should not error

		assertErrorString(t, secondCloseErr, nil)

		assertProperHelperCleanup(t, helper, 2)
	})

	// Canceling context for manager 0
	t.Run("ManagerClose returnes wrapped error", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)
		manager := helper.Manager(0)

		if err := (*manager.Output()).ResultErr; err != nil {
			t.Errorf("Unexpected error %v already set as ResultErr", err)
		}

		(*helper.ManagerOutput(0)).ResultErr = errTestCloseError

		if err := (*manager.Output()).ResultErr; !errors.Is(err, errTestCloseError) {
			t.Errorf("Unexpected error %v already set as ResultErr", err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		if err := manager.SetContext(ctx); err != nil {
			t.Errorf("Unexpected error %v setting manager context", err) // should not be possible to be non-nil.
		}

		if callCount := (*helper.ManagerOutput(0)).GetCloseCalls(); callCount != 0 {
			t.Errorf("EXPECTED manager %d's closer to be called 0 time, saw %d times", 0, callCount)
		}

		cancel()
		time.Sleep(10 * time.Millisecond)

		if state := helper.ManagerState(0); state != stream.MOHelperClosed {
			t.Errorf("EXPECTED manager %d to be %v, saw %v", 0, stream.MOHelperClosed, state)
		}

		if state := manager.State(); state != stream.MOHelperClosed {
			t.Errorf("EXPECTED manager %d to be %v, saw %v", 0, stream.MOHelperClosed, state)
		}

		if outputID := manager.OutputID(); outputID != 0 {
			t.Errorf("EXPECTED manager outputID %d to be %v, saw %v", 0, 0, outputID)
		}

		if callCount := (*helper.ManagerOutput(0)).GetCloseCalls(); callCount != 1 {
			t.Errorf("EXPECTED manager %d's closer to be called 1 time, saw %d times", 0, callCount)
		}

		err := helper.ManagerClose(0) // Should return previous error
		expectedWrappedErr := fmt.Errorf(
			"error closing output: %w",
			errTestCloseError,
		)

		assertErrorString(t, err, expectedWrappedErr)

		secondCloseErr := helper.ManagerClose(0) // Should not error

		assertErrorString(t, secondCloseErr, nil)

		assertProperHelperCleanup(t, helper, 2)
	})

	// Wait for the goroutine to finish
	t.Run("helper.Close() completes without panic", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		assertProperHelperCleanup(t, helper, 2)
	})

	// Repeated close
	t.Run("Repeated ManagerClose does not panic", func(t *testing.T) {
		t.Parallel()

		helper := newHelper(t)

		err := helper.ManagerClose(0) // Should not panic
		if err != nil {
			t.Errorf("EXPECTED nil error, but got: %#v", err)
		}

		precanceledCtx, cancel := context.WithCancel(context.Background())

		cancel()

		// test that Set pre-canceled context after close also gives a nil error:
		err = helper.ManagerSetContext(0, precanceledCtx)
		assertErrorString(t, err, nil)

		// test that Set active context after close also gives correct error:
		err = helper.ManagerSetContext(0, context.Background())
		assertErrorString(t, err, stream.ErrActiveContextClosedOutput)

		assertProperHelperCleanup(t, helper, 2)
	})
}

// ErrTestErrorCloserZero is a synthetic error for closing output #0.
var ErrTestErrorCloserZero = errors.New("closer error #0")

// ErrTestErrorCloserOne is a synthetic error for closing output #1.
var ErrTestErrorCloserOne = errors.New("closer error #1")

// ErrTestErrorCloserTwo is a synthetic error for closing output #2.
var ErrTestErrorCloserTwo = errors.New("closer error #2")

// ErrTestErrorCloserThree is a synthetic error for closing output #3.
var ErrTestErrorCloserThree = errors.New("closer error #3")

// ErrTestErrorCloserFour is a synthetic error for closing output #4.
var ErrTestErrorCloserFour = errors.New("closer error #4")

// TestHelperCloseAggregatesErrorsWhenContextCanceled is a single test that
// has multiple sub-tests for zero, one, and multiple manager errors.
// It uses context cancellation (rather than ManagerClose) to trigger
// each manager's closer, expecting that any returned errors get
// aggregated and returned by MultiOutputHelper.Close().
//nolint: funlen // **FIXME**
func TestHelperCloseAggregatesErrorsWhenContextCanceled(t *testing.T) {
	t.Parallel()

	// We'll define sub-tests for 4 scenarios:
	testCases := []struct {
		name        string
		numManagers int
		errors      []error // Each manager's ResultErr
		expectedErr error   // Exact combined error we expect from helper.Close()
	}{
		{
			name:        "ZeroErrors",
			numManagers: 3,
			errors:      []error{nil, nil, nil},
			// Expect no error at all
			expectedErr: nil,
		},
		{
			name:        "SingleError",
			numManagers: 3,
			// Only manager #1 has an error; others are nil
			errors: []error{nil, ErrTestErrorCloserOne, nil},
			// Expect a single error with manager 1's text
			expectedErr: fmt.Errorf(
				"aggregate close error(s): %w",
				errors.Join(fmt.Errorf("error closing output: %w", ErrTestErrorCloserOne)),
			),
		},
		{
			name:        "MultipleErrors",
			numManagers: 3,
			// Two managers have an error; one is nil
			errors: []error{
				ErrTestErrorCloserZero,
				nil,
				ErrTestErrorCloserTwo,
			},
			// Expect both error messages combined in some aggregator format
			expectedErr: fmt.Errorf(
				"aggregate close error(s): %w",
				errors.Join(
					fmt.Errorf("error closing output: %w", ErrTestErrorCloserZero),
					fmt.Errorf("error closing output: %w", ErrTestErrorCloserTwo),
				),
			),
		},
		{
			name:        "MixedNilAndNonNil",
			numManagers: 5,
			// Some nil, some errors
			errors: []error{
				ErrTestErrorCloserZero,
				nil,
				nil,
				ErrTestErrorCloserThree,
				ErrTestErrorCloserFour,
			},
			// We only list non-nil errors in the aggregator
			expectedErr: fmt.Errorf(
				"aggregate close error(s): %w",
				errors.Join(
					fmt.Errorf("error closing output: %w", ErrTestErrorCloserZero),
					fmt.Errorf("error closing output: %w", ErrTestErrorCloserThree),
					fmt.Errorf("error closing output: %w", ErrTestErrorCloserFour),
				),
			),
		},
	}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			// 1) Create a new MultiOutputHelper with custom errors
			helper := stream.NewMultiOutputHelper(tCase.numManagers, func(idx int) *MockCloser {
				return &MockCloser{
					CloseCalls: 0,
					CustomData: "Stuff",
					ResultErr:  tCase.errors[idx],
				}
			})

			// 2) Set a cancelable context for each manager, then cancel it
			//    to simulate "closing by context".
			for i := range tCase.numManagers {
				ctx, cancel := context.WithCancel(context.Background())
				if err := helper.ManagerSetContext(i, ctx); err != nil {
					t.Fatalf("ManagerSetContext(%d) failed: %v", i, err)
				}

				cancel()
			}

			// 3) Wait a short moment for goroutines to finish
			//    closing the closers via context cancellation.
			time.Sleep(10 * time.Millisecond)

			// 4) Call helper.Close() to gather aggregator error(s).
			aggregatedErr := helper.Close()

			// 5) Compare aggregatedErr with the expected error
			assertErrorString(t, aggregatedErr, tCase.expectedErr)
		})
	}
}
