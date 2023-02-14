package mapreduce

import (
	"context"
	"sync"
)

// workerpool implements a worker pool
type workerpool[I, O any] struct {
	inCh          chan I
	outCh         chan O
	errCh         chan error
	size          int
	do            func(I) (O, error)
	status        workerpoolStatus
	doneWithInput *sync.WaitGroup
	mu            *sync.Mutex
}

type workerpoolStatus string

const wpNew = workerpoolStatus("New")
const wpStarted = workerpoolStatus("Started")
const wpStopped = workerpoolStatus("Stopped")

// newWorkerpool creates a Workerpool and returns a pointer to it
func newWorkerpool[I, O any](size int, do func(input I) (O, error)) *workerpool[I, O] {
	inCh := make(chan I)
	outCh := make(chan O)
	errCh := make(chan error)
	var doneWithInput sync.WaitGroup
	doneWithInput.Add(size)
	var mu sync.Mutex
	pool := workerpool[I, O]{inCh, outCh, errCh, size, do, wpNew, &doneWithInput, &mu}
	return &pool
}

// start starts the workerpool
func (pool *workerpool[I, O]) start(ctx context.Context) {
	pool.mu.Lock()
	if pool.status == wpStarted {
		return
	}
	pool.status = wpStarted
	pool.mu.Unlock()
	for i := 0; i < pool.size; i++ {
		// launch the workers of the pool
		// these workers complete either when pool.inCh is closed or if the context signals (because of timeout or because it is cancelled)
		go func() {
			defer pool.doneWithInput.Done()
			for {
				select {
				case input, more := <-pool.inCh:
					if !more {
						return
					}
					output, e := pool.do(input)
					if e != nil {
						pool.errCh <- e
						continue
					}
					pool.outCh <- output
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

// stop stops the workerpool
// After the Workerpool is stopped no other input value can be processed
func (pool *workerpool[I, O]) stop() {
	pool.mu.Lock()
	if pool.status == wpStopped {
		return
	}
	pool.status = wpStopped
	pool.mu.Unlock()
	// close the input channel
	close(pool.inCh)
	// wait for all the values sent to the input channel to go through the processing made by the pool
	pool.doneWithInput.Wait()
	// close the output and the error channels
	close(pool.outCh)
	close(pool.errCh)
}

// process sends one value to the workerpool to be processed by the first available worker.
func (pool *workerpool[I, O]) process(input I) {
	pool.inCh <- input
}

// getStatus returns the status of the workerpool
func (pool *workerpool[I, O]) getStatus() workerpoolStatus {
	var st workerpoolStatus
	pool.mu.Lock()
	st = pool.status
	pool.mu.Unlock()
	return st
}
