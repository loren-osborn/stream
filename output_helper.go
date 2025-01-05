// Package stream provides tools for lazy evaluation and data transformation pipelines.
package stream

// This file implements fork and related features like spool and partition.

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
)

// MultiOutputHelperState indicates the state of a particular manager for an output.
type MultiOutputHelperState int

const (
	// MOHelperUninitialized indicates output manager has no associated context.
	MOHelperUninitialized MultiOutputHelperState = iota

	// MOHelperWaiting indicates output manager is waiting for context to cancel.
	MOHelperWaiting

	// MOHelperClosed indicates output manager has closed.
	MOHelperClosed
)

// OutputManager manages the cancelation of the context for a single output.
type OutputManager[T io.Closer] struct {
	mu            sync.RWMutex
	state         MultiOutputHelperState
	ctxChan       chan context.Context
	closer        T
	closedAlready bool
	wGroup        sync.WaitGroup
}

// MultiOutputHelper is a helper that assists with cancelation of multiple output Sources.
type MultiOutputHelper[T io.Closer] struct {
	managers     []OutputManager[T]
	consensusCtx func() context.Context
	cancelFn     func()
	Logger       *log.Logger
}

// NewMultiOutputHelper creates a new MultiOutputHelper.
//
// Parameter `outputs` is the number of outputs (managers) to create.
//
// Parameter `initializer` is a function that returns a T (which implements io.Closer)
// for the given output index.
func NewMultiOutputHelper[T io.Closer](outputs int, initializer func(int) T) *MultiOutputHelper[T] {
	managers := make([]OutputManager[T], outputs)

	for i := range outputs {
		managers[i].closer = initializer(i)
		managers[i].ctxChan = make(chan context.Context, 1)
	}

	cancelableCtx, cancel := context.WithCancel(context.Background())

	return &MultiOutputHelper[T]{
		managers:     managers,
		consensusCtx: func() context.Context { return cancelableCtx },
		cancelFn:     cancel,
		Logger:       log.Default(),
	}
}

func (moh *MultiOutputHelper[T]) validateOutputID(outputID int) {
	if (outputID < 0) || (outputID >= len(moh.managers)) {
		panic(fmt.Sprintf("invalid manager index: %d", outputID))
	}
}

// ManagerState returns the state of the manager for outputID.  This is concurrency-safe.
func (moh *MultiOutputHelper[T]) ManagerState(outputID int) MultiOutputHelperState {
	moh.validateOutputID(outputID)
	m := &moh.managers[outputID]
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.state
}

// ManagerSetContext sets the active context for the specified outputID.
//
// If the manager is in MOHelperUninitialized state, we launch a goroutine that
// monitors for context cancelation. If the manager is in MOHelperWaiting state,
// we update it with a new context. If it is closed, this method is no-op.
func (moh *MultiOutputHelper[T]) ManagerSetContext(newCtx context.Context, outputID int) error {
	moh.validateOutputID(outputID)

	if newCtx == nil {
		panic(fmt.Sprintf("Attempting to set invalid nil context for outputID %d", outputID))
	}

	manager := &moh.managers[outputID]

	manager.mu.Lock()
	defer manager.mu.Unlock()

	switch manager.state {
	case MOHelperClosed:
		return nil
	case MOHelperUninitialized:
		manager.state = MOHelperWaiting

		moh.startManagerGoroutine(newCtx, outputID)
	case MOHelperWaiting:
		// Replace the old context with a new one.
		manager.ctxChan <- newCtx
	}

	return nil
}

// startManagerGoroutine launches the goroutine that watches for cancelation.
func (moh *MultiOutputHelper[T]) startManagerGoroutine(initialCtx context.Context, outputID int) {
	moh.managers[outputID].wGroup.Add(1)

	go func() {
		defer moh.managers[outputID].wGroup.Done()
		// moh.Logger.Printf("starting goroutine to listen for context cancelation for manager %d", outputID)
		//nolint: contextcheck
		currentCtx, curCancelFunc := context.WithCancel(initialCtx)

		for {
			select {
			case <-currentCtx.Done():
				// moh.Logger.Printf("cancel detected for output manager %d...", outputID)
				curCancelFunc()
				<-currentCtx.Done()
				// moh.Logger.Printf("context Done channel clear for output manager %d...", outputID)
				moh.markClosed(outputID)

				return
			case newCtx := <-moh.managers[outputID].ctxChan:
				curCancelFunc()
				<-currentCtx.Done()
				// if newCtx == nil {
				// 	moh.Logger.Printf("received request to close goroutine for output manager %d...", outputID)
				// } else {
				// 	moh.Logger.Printf("received new listening context for output manager %d...", outputID)
				// }

				if newCtx == nil {
					// We interpret a closed channel or nil as a signal to close
					moh.markClosed(outputID)
					// moh.Logger.Printf("closing goroutine for output manager %d...", outputID)

					return
				}
				// Switch to a new context

				// moh.Logger.Printf("switching listening context for output manager %d...", outputID)

				//nolint: fatcontext
				currentCtx, curCancelFunc = context.WithCancel(newCtx)
			}
		}
	}()
}

// markClosed sets the manager's state to MOHelperClosed, closes the closer (if any),
// and checks whether the consensus context should be canceled.
func (moh *MultiOutputHelper[T]) markClosed(outputID int) {
	manager := &moh.managers[outputID]

	alreadyClosed := func() bool {
		manager.mu.Lock()

		defer manager.mu.Unlock()

		if manager.state == MOHelperClosed {
			return true
		}

		manager.state = MOHelperClosed

		return false
	}

	if alreadyClosed() {
		return
	}

	moh.updateConsensusCtx()

	// Attempt to close the output's closer, if present.
	if !manager.closedAlready {
		manager.closedAlready = true
		if err := manager.closer.Close(); err != nil {
			moh.Logger.Printf("error closing output: %v", err)
		}
	}
}

// ManagerClose closes context monitoring for outputID.
//
// If the manager is uninitialized, we simply mark it closed and optionally
// skip calling closer. If the manager is waiting, we close the ctxChan, which
// signals the manager goroutine to exit. This method returns nil on success.
//
// If callCloser is false, we reset manager.closer to nil to skip the final close call.
func (moh *MultiOutputHelper[T]) ManagerClose(outputID int, callCloser bool) error {
	moh.validateOutputID(outputID)
	manager := &moh.managers[outputID]
	afterUnlock := func() {}

	manager.mu.Lock()
	deferFunc := func() {
		manager.mu.Unlock()
		afterUnlock()
	}

	defer deferFunc()

	switch manager.state {
	case MOHelperClosed:
		return nil
	case MOHelperUninitialized:
		manager.state = MOHelperClosed

		afterUnlock = func() {
			moh.updateConsensusCtx()

			if !callCloser {
				manager.closedAlready = true
			}

			if !manager.closedAlready {
				manager.closedAlready = true
				if err := manager.closer.Close(); err != nil {
					moh.Logger.Printf("error closing output: %v", err)
				}
			}
		}

	case MOHelperWaiting:
		// The goroutine will exit once ctxChan is closed.
		if !callCloser {
			manager.closedAlready = true
		}

		close(manager.ctxChan)
		// We do NOT directly set state here, because the goroutine
		// will do it in markClosed().

		// but we have to wait for it:
		afterUnlock = func() {
			manager.wGroup.Wait()
		}

	default:
	}

	return nil
}

// updateConsensusCtx checks if all Managers are closed, and if so, cancels the
// consensus context.
func (moh *MultiOutputHelper[T]) updateConsensusCtx() {
	// If already canceled, no need to check anything.
	if moh.consensusCtx().Err() != nil {
		return
	}

	for i := range moh.managers {
		if moh.ManagerState(i) != MOHelperClosed {
			return
		}
	}

	moh.cancelFn()
}

// ConsensusContext returns the consensus context for all outputs. It
// is only canceled when all outputs are closed.
func (moh *MultiOutputHelper[T]) ConsensusContext() context.Context {
	return moh.consensusCtx()
}

// Close performs any final resource cleanup.
func (moh *MultiOutputHelper[T]) Close() error {
	for i := range moh.managers {
		if moh.ManagerState(i) != MOHelperClosed {
			moh.ManagerClose(i, false)
		}
	}

	return nil
}
