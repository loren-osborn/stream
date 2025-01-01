// Package stream provides tools for lazy evaluation and data transformation pipelines.
package stream

// This file implements fork and related features like spool and partition.

import (
	"context"
	"io"
	"log"
	"sync"
)

// MultiOutputHelperState indicates the state of a particular manager for an output.
type MultiOutputHelperState int

// MOHelperUninitialized indicates output manager has no associated context.
const (
	MOHelperUninitialized MultiOutputHelperState = iota
	MOHelperWaiting                              // MOHelperWaiting indicates output manager is waiting
	// for context to cancel.
	MOHelperClosed // MOHelperClosed indicates output manager has closed
)

// OutputManager manages the cancelation of the context for a single output.
type OutputManager[T io.Closer] struct {
	mu      sync.RWMutex
	state   MultiOutputHelperState
	ctxChan chan *context.Context
	closer  *T
}

// MultiOutputHelper a helper that assists with cancelation of multiple output Sources.
type MultiOutputHelper[T io.Closer] struct {
	managers     []OutputManager[T]
	consensusCtx *context.Context
	cancelFn     func()
}

// NewMultiOutputHelper creates a new MultiOutputHelper.
func NewMultiOutputHelper[T io.Closer](outputs int, initializer func(int) T) *MultiOutputHelper[T] {
	managers := make([]OutputManager[T], outputs)

	for i := range outputs {
		closer := initializer(i)
		managers[i].closer = &closer
		managers[i].ctxChan = make(chan *context.Context, 1)
	}

	cancelableCtx, cancel := context.WithCancel(context.Background())

	return &MultiOutputHelper[T]{
		managers:     managers,
		consensusCtx: &cancelableCtx,
		cancelFn:     cancel,
	}
}

// ManagerState returns the state of the manager for outputID.
func (moh *MultiOutputHelper[T]) ManagerState(outputID int) MultiOutputHelperState {
	moh.managers[outputID].mu.RLock()

	defer moh.managers[outputID].mu.RUnlock()

	return moh.managers[outputID].state
}

// ManagerSetContext sets the active context for outputID.
func (moh *MultiOutputHelper[T]) ManagerSetContext(newCtx context.Context, outputID int) error {
	moh.managers[outputID].mu.Lock()

	defer moh.managers[outputID].mu.Unlock()

	if moh.managers[outputID].state == MOHelperClosed {
		return nil
	}

	if moh.managers[outputID].state == MOHelperUninitialized {
		moh.managers[outputID].state = MOHelperWaiting

		goroutine := func() {
			currentCtx := newCtx
			for {
				select {
				case <-currentCtx.Done():
					moh.managers[outputID].mu.Lock()
					moh.managers[outputID].state = MOHelperClosed
					moh.managers[outputID].mu.Unlock()
					moh.updateConsensusCtx()

					if moh.managers[outputID].closer != nil {
						if err := (*moh.managers[outputID].closer).Close(); err != nil {
							log.Printf("error closing output: %v", err)
						}
					}

					return
				case newCtx := <-moh.managers[outputID].ctxChan:
					currentCtx = *newCtx
				case <-moh.managers[outputID].ctxChan:
					moh.managers[outputID].mu.Lock()
					moh.managers[outputID].state = MOHelperClosed
					moh.managers[outputID].mu.Unlock()
					moh.updateConsensusCtx()

					if moh.managers[outputID].closer != nil {
						if err := (*moh.managers[outputID].closer).Close(); err != nil {
							log.Printf("error closing output: %v", err)
						}
					}

					return
				}
			}
		}
		go goroutine()
	} else if moh.managers[outputID].state == MOHelperWaiting {
		moh.managers[outputID].ctxChan <- &newCtx
	}

	return nil
}

// ManagerClose closes context monitoring for outputID.
func (moh *MultiOutputHelper[T]) ManagerClose(outputID int, callCloser bool) error {
	moh.managers[outputID].mu.Lock()

	defer moh.managers[outputID].mu.Unlock()

	if moh.managers[outputID].state == MOHelperClosed {
		return nil
	}

	if moh.managers[outputID].state == MOHelperUninitialized {
		moh.managers[outputID].state = MOHelperClosed
		moh.updateConsensusCtx()

		if !callCloser {
			moh.managers[outputID].closer = nil
		}

		if moh.managers[outputID].closer != nil {
			if err := (*moh.managers[outputID].closer).Close(); err != nil {
				log.Printf("error closing output: %v", err)
			}
		}
	} else if moh.managers[outputID].state == MOHelperWaiting {
		if !callCloser {
			moh.managers[outputID].closer = nil
		}

		close(moh.managers[outputID].ctxChan)
	}

	return nil
}

// updateConsensusCtx checks if all Managers are closed, and if so, cancels
// moh.consensusCtx.
func (moh *MultiOutputHelper[T]) updateConsensusCtx() {
	if (*moh.consensusCtx).Err() != nil {
		return // already canceled
	}

	for i := range len(moh.managers) {
		if moh.ManagerState(i) != MOHelperClosed {
			return // not all closed yet
		}
	}

	moh.cancelFn()
}

// ConsensusContext returns the consensus context for all outputs. It
// is only canceled when all outputs are closed.
func (moh *MultiOutputHelper[T]) ConsensusContext() context.Context {
	return *moh.consensusCtx
}
