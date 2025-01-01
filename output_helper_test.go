package stream_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

// MockCloser is a mock implementation of io.Closer for testing.
type MockCloser struct {
	CloseCalls int32
	CustomData string // Demonstrates type-specific fields
}

func (m *MockCloser) Close() error {
	atomic.AddInt32(&m.CloseCalls, 1)

	return nil
}

//nolint: funlen,gocognit,cyclop // **FIXME**
func TestHelperWithGenericClosers(t *testing.T) {
	t.Parallel()

	const numManagers = 5
	managerInfo := make([]struct {
		closer   *MockCloser
		ctx      *context.Context
		cancelFn func()
	}, numManagers)

	// Create closers for each manager
	helper := stream.NewMultiOutputHelper(numManagers, func(index int) *MockCloser {
		closer := &MockCloser{
			CloseCalls: 0,
			CustomData: fmt.Sprintf("Manager %d", index),
		}
		managerInfo[index].closer = closer

		return closer
	})

	for index := range managerInfo {
		if state := helper.ManagerState(index); state != stream.MOHelperUninitialized {
			t.Fatalf("Expected manager %d to be uninitialized, saw %#v", index, state)
		}

		ctx, cancel := context.WithCancel(context.Background())
		managerInfo[index].ctx = &ctx
		managerInfo[index].cancelFn = cancel

		if err := helper.ManagerSetContext(ctx, index); err != nil {
			t.Fatalf("failed to set context for manager %d: %v", index, err)
		}

		if state := helper.ManagerState(index); state != stream.MOHelperWaiting {
			t.Fatalf("Expected manager %d to be waiting, saw %#v", index, state)
		}
	}

	for index, outer := range managerInfo {
		for j, info := range managerInfo {
			expectedCalls := int32(0)

			if j < index {
				expectedCalls = 1
			}

			if info.closer.CloseCalls != expectedCalls {
				t.Errorf("expected closer %d to be called %d times, got %d", index, expectedCalls, info.closer.CloseCalls)
			}
		}

		// we're testing both ways a manager can get closed:
		if index%2 == 0 {
			outer.cancelFn() // if a manager's context gets canceled, it should close
		} else {
			err := helper.ManagerClose(index, false) // if it is closed directly, it should cleanup properly
			if err != nil {
				t.Errorf("expected no error closing manager %d, got %#v", index, err)
			}
		}

		for j, info := range managerInfo {
			expectedCalls := int32(0)
			if j <= index {
				expectedCalls = 1

				if state := helper.ManagerState(j); state != stream.MOHelperWaiting {
					t.Fatalf("Expected manager %d to be closed, saw %#v", index, state)
				}
			}

			if info.closer.CloseCalls != expectedCalls {
				t.Errorf("expected closer %d to be called %d times, got %d", index, expectedCalls, info.closer.CloseCalls)
			}
		}
	}
}

func TestHelperConsensusWithGenericClosers(t *testing.T) {
	t.Parallel()

	const numManagers = 3

	// Create closers for each manager
	helper := stream.NewMultiOutputHelper(numManagers, func(_ int) *MockCloser {
		return &MockCloser{
			CloseCalls: 0,
			CustomData: "foo",
		}
	})

	// Retrieve the consensus context
	consensusCtx := helper.ConsensusContext()

	// Close all managers
	for i := range numManagers {
		if ctxErr := consensusCtx.Err(); ctxErr != nil {
			t.Errorf("expected nil error, got %#v", ctxErr)
		}

		helper.ManagerClose(i, false)
	}

	// Verify that consensus context is canceled
	if ctxErr := consensusCtx.Err(); ctxErr == nil || !errors.Is(ctxErr, context.Canceled) {
		t.Errorf("expected error %#v, got %#v", context.Canceled, ctxErr)
	}
}
