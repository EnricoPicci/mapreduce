// this test is in the workerpool package since we want to test the internal status of the pool
package mapreduce

import (
	"context"
	"sync"
	"testing"
	"time"
)

// This tests checks that, if a context timeout is triggered, all workers of the workerpool are terminated
// (as long as the do function they call is able to handle a context timeout signal and terminate).
// Each worker simulates to have work to do that lasts a certain duration (workDuration).
// The work performed by the workers is defined by the function do.
// A context timeout is triggered after a period (timeout) where timeout < workDuration. Since timeout < workDuration the workers receive the timeout signal
// before they are actually able to complete their work.
// When a worker starts, it starts processing the tasks calling the do function.
// When the do function starts, it sets the flag doStarted to true. When the do finishes its job it sets the flag taskComplete to true.
// The flags doStarted and taskComplete are shared among all workers for simplicity. We do not care that they are shared since we want to test that
// at least one worker actually started launching one do execution, but no do ever completed.
// When the MapReduce function returns, which occurs after all the workers of the pool have been closed
// (which means that all the channels internal to the pool have been closed), we wait for a period (testDelay) where testDelay > workDuration.
// Since testDelay > workDuration, if the worker has not been stopped, then the worker would be able to set the flag taskComplete to true.
// After testDelay is passed, the test checks that the flag doStarted is true (to ensure that the workers actually started) and that the flag
// taskComplete is false (to ensure that they have been terminated)
func TestWorkerpoolWithTimeout(t *testing.T) {
	// parameters to constuct the pool
	// pool size
	poolSize := 1000
	// do function
	workDuration := 100 * time.Millisecond
	timeout := workDuration / 10
	testDelay := workDuration * 10
	// a context with a timeout that is triggered with a not so short delay
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	doStarted := false
	taskComplete := false
	var testMu sync.Mutex
	do := func(input int) (int, error) {
		testMu.Lock()
		doStarted = true
		testMu.Unlock()
		timer := time.NewTimer(workDuration)
		select {
		// we simulate the work of the worker with a timer
		case <-timer.C: // timer fired, i.e. the worker has performed its task
			taskComplete = true
			return 0, nil
		case <-ctx.Done(): // the timeout signal is received
			return 0, ctx.Err()
		}
	}
	// construct the pool
	pool := newWorkerpool(poolSize, do)
	// start the pool
	pool.start(ctx)

	// number of input values that will be sent to the pool to be processed
	numOfInputSentToPool := 1000000
	// launch a goroutine that sends the input values to the pool.  If the context timeouts, the pool is stopped
	go func() {
		defer pool.stop()
		for i := 0; i < numOfInputSentToPool; i++ {
			select {
			case pool.inCh <- i:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	// slices where we collect the results of the processing performed by the pool and the potential errors received from the pool
	resultsReceived := []int{}
	errorsReceived := []error{}
	// launch 2 goroutines.
	// One reads the results of the processing from the output channel of the pool.
	// The other reads the errors that the pool may send
	var wg sync.WaitGroup
	wg.Add(2)
	processResult := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for res := range pool.outCh {
			resultsReceived = append(resultsReceived, res)
		}
	}
	processErrors := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for err := range pool.errCh {
			errorsReceived = append(errorsReceived, err)
		}
	}
	go processResult(&wg)
	go processErrors(&wg)

	// Wait for processResult and processErrors goroutines to complete
	wg.Wait()

	// wait to make sure that, if the dos have not been terminated by the context timeout, they have the time to set the taskComplete to true
	time.Sleep(testDelay)

	// check the results of the test
	testMu.Lock()
	if !doStarted {
		t.Error("The do function has never started, i.e. the workerpool did not start its work")
	}
	testMu.Unlock()
	if taskComplete {
		t.Error("The do function was not terminated by the context timeout")
	}
	if len(resultsReceived) > 0 {
		t.Error("The worker pool should not send any result since the timeout occurs before the do function can complete its work")
	}
	// the number of goroutines running the do function is equal to the size of the pool - when the context fires, then
	// all these goroutines write an error in the error queue
	if len(errorsReceived) != poolSize {
		t.Errorf("The worker pool should send the same number of errors as the poolSize, instead has sent %v errors", len(errorsReceived))
	}
	expectedPoolStatus := wpStopped
	gotStatus := pool.getStatus()
	if expectedPoolStatus != gotStatus {
		t.Errorf("Expected pool status (the pool should have been closed as consequence of the context timeout triggering) %v - got %v",
			expectedPoolStatus, gotStatus)
	}
}
