// Package stream provides tools for lazy evaluation and data transformation pipelines.
package stream

// This file implements fork and related features like spool and partition.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrActiveContextClosedOutput indicates that you are attempting to assign a
// non-canceled context to an MultiOutputHelper's output that has already been
// closed.
var ErrActiveContextClosedOutput = errors.New("assigning non-canceled context to already closed output")

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
	helper       *MultiOutputHelper[T]
	outputID     int
	mu           sync.RWMutex
	state        MultiOutputHelperState
	ctxChan      chan context.Context
	output       T
	closeFn      func()
	closeErrChan chan error
	wGroup       sync.WaitGroup
}

// MultiOutputHelper is a helper that assists with cancelation of multiple output Sources.
type MultiOutputHelper[T io.Closer] struct {
	managers     []OutputManager[T]
	consensusCtx func() context.Context
	cancelFn     func()
}

// NewMultiOutputHelper creates a new MultiOutputHelper.
//
// Parameter `outputs` is the number of outputs (managers) to create.
//
// Parameter `initializer` is a function that returns a T (which implements io.Closer)
// for the given output index.
func NewMultiOutputHelper[T io.Closer](outputs int, initializer func(int) T) *MultiOutputHelper[T] {
	managers := make([]OutputManager[T], outputs)
	cancelableCtx, cancel := context.WithCancel(context.Background())
	result := &MultiOutputHelper[T]{
		managers:     managers,
		consensusCtx: func() context.Context { return cancelableCtx },
		cancelFn:     cancel,
	}

	for idx := range outputs {
		managers[idx].helper = result
		managers[idx].outputID = idx
		managers[idx].output = initializer(idx)
		managers[idx].ctxChan = make(chan context.Context, 1)
		managers[idx].closeErrChan = make(chan error, 1)
		managers[idx].closeFn = sync.OnceFunc(func() {
			managers[idx].mu.Lock()

			go func() {
				func() {
					defer managers[idx].mu.Unlock()

					managers[idx].state = MOHelperClosed
				}()

				result.updateConsensusCtx()

				if err := managers[idx].output.Close(); err != nil {
					managers[idx].closeErrChan <- fmt.Errorf(
						"error closing output: %w",
						err,
					)
				}

				close(managers[idx].closeErrChan)
			}()
		})
	}

	return result
}

func (moh *MultiOutputHelper[T]) validateOutputID(outputID int) {
	if (outputID < 0) || (outputID >= len(moh.managers)) {
		panic(fmt.Sprintf("invalid manager index: %d", outputID))
	}
}

// Manager returns the output manager proxy for outputID.
func (moh *MultiOutputHelper[T]) Manager(outputID int) *OutputManager[T] {
	moh.validateOutputID(outputID)

	return &moh.managers[outputID]
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
//nolint: revive // Intentional argument order. outputID is the destination, newCtx is what's being assigned.
func (moh *MultiOutputHelper[T]) ManagerSetContext(outputID int, newCtx context.Context) error {
	moh.validateOutputID(outputID)

	if newCtx == nil {
		panic(fmt.Sprintf("Attempting to set invalid nil context for outputID %d", outputID))
	}

	manager := &moh.managers[outputID]

	manager.mu.Lock()
	defer manager.mu.Unlock()

	switch manager.state {
	case MOHelperClosed:
		if newCtx.Err() == nil {
			return ErrActiveContextClosedOutput
		}

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

		currentCtx, curCancelFunc := context.WithCancel(initialCtx)

		drainCurrentChannel := func() {
			curCancelFunc()
			<-currentCtx.Done()
		}

		for {
			select {
			case <-currentCtx.Done():
				drainCurrentChannel()

				moh.managers[outputID].closeFn()

				//nolint: govet // I think this is handled, but want a second opion.
				return
			case newCtx := <-moh.managers[outputID].ctxChan:
				drainCurrentChannel()

				if newCtx == nil {
					moh.managers[outputID].closeFn()

					return
				}
				// Switch to a new context

				//nolint: fatcontext,contextcheck,govet // I think this is handled, but want a second opion.
				currentCtx, curCancelFunc = context.WithCancel(newCtx)
			}
		}
	}()
}

// ManagerClose closes context monitoring for outputID.
//
// If the manager is uninitialized, we simply mark it closed. If the manager is
// waiting, we close the ctxChan, which signals the manager goroutine to exit.
// This method returns nil on success.
func (moh *MultiOutputHelper[T]) ManagerClose(outputID int) error {
	moh.validateOutputID(outputID)
	manager := &moh.managers[outputID]

	func() {
		manager.mu.Lock()

		defer manager.mu.Unlock()

		switch manager.state {
		case MOHelperUninitialized:
			manager.state = MOHelperClosed
			go manager.closeFn()
		case MOHelperWaiting: // The goroutine will exit once ctxChan is closed.
			close(manager.ctxChan)
			// We do NOT directly set state here, because the goroutine
			// will do it in markClosed().
		case MOHelperClosed:
			fallthrough
		default:
			assertf(manager.state == MOHelperClosed, "INTERNAL ERROR: Unhandled state %v", manager.state)
		}
	}()

	manager.wGroup.Wait()

	return <-manager.closeErrChan
}

// ManagerOutput returns the manager's output.
func (moh *MultiOutputHelper[T]) ManagerOutput(outputID int) *T {
	moh.validateOutputID(outputID)

	return &moh.managers[outputID].output
}

// updateConsensusCtx checks if all Managers are closed, and if so, cancels the
// consensus context.
func (moh *MultiOutputHelper[T]) updateConsensusCtx() {
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
	errList := make([]error, 0, len(moh.managers))

	for i := range moh.managers {
		err := moh.ManagerClose(i)
		if err != nil {
			errList = append(errList, err)
		}
	}

	if len(errList) > 0 {
		return fmt.Errorf("aggregate close error(s): %w", errors.Join(errList...))
	}

	return nil
}

// Close closes context monitoring for output.
//
// If the manager is uninitialized, we simply mark it closed. If the manager is
// waiting, we close the ctxChan, which signals the manager goroutine to exit.
// This method returns nil on success.
func (om *OutputManager[T]) Close() error {
	return om.helper.ManagerClose(om.outputID)
}

// Output returns the manager's output.
func (om *OutputManager[T]) Output() *T {
	return om.helper.ManagerOutput(om.outputID)
}

// OutputID returns the manager's outputID.
func (om *OutputManager[T]) OutputID() int {
	return om.outputID
}

// SetContext sets the active context for output.
//
// If the manager is in MOHelperUninitialized state, we launch a goroutine that
// monitors for context cancelation. If the manager is in MOHelperWaiting state,
// we update it with a new context. If it is closed, this method is no-op.
func (om *OutputManager[T]) SetContext(newCtx context.Context) error {
	return om.helper.ManagerSetContext(om.outputID, newCtx)
}

// State returns the state of the manager's output.
func (om *OutputManager[T]) State() MultiOutputHelperState {
	return om.helper.ManagerState(om.outputID)
}
