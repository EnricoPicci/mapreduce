package mapreduce

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// In this test a timeout is triggered and the reduce is stopped.
// The timeout is very short.
// We check that the status of the pool is "stopped".
// This is an internal test to make sure thare are no pending goroutines held by the pool
func TestMapReduceWithTimeoutPoolStatus(t *testing.T) {
	// a short shortTimeout
	shortTimeout := time.Duration(100)
	// a context with a timeout that is triggered with a short delay
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()

	// parameters to constuct the pool
	poolSize := 10
	// construct the pool
	pool := newWorkerpool(poolSize, mapStringToInt)
	pool.start(ctx)

	// values that have to be reduced
	numOfValuesToReduce := 200000
	valuesToReduce := sliceOfIntegersAsStrings(numOfValuesToReduce)
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func(_ctx context.Context) {
		defer pool.stop()
		for _, v := range valuesToReduce {
			select {
			default:
				pool.process(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	// Since the timeout is fired before the end of the processing, the errors slice should contain 1 error
	_, err := reduce(ctx, pool, sumNumbers, accInitialValue)

	// the context signal is triggered very soon in the test, so we wait for some time to give the pool the possibility to shut down all the workers
	// and get to a stopped state
	time.Sleep(time.Millisecond)

	// check the results of the test
	expectedError := context.DeadlineExceeded
	gotError := err
	if expectedError != gotError {
		t.Errorf("Expected error %v - got %v", expectedError, gotError)
	}
	expectedPoolStatus := wpStopped
	gotStatus := pool.getStatus()
	if expectedPoolStatus != gotStatus {
		t.Errorf("Expected pool status (the pool should have been closed as consequence of the context timeout triggering) %v - got %v",
			expectedPoolStatus, gotStatus)
	}
}

var numGeneratingError = 5
var convError = errors.New("Error occurred while converting string to int")

// this is the function that is passed to the worker pool to perform the processing
func mapStringToInt(input string) (int, error) {
	n, _ := strconv.Atoi(input)
	if n == numGeneratingError {
		return 0, convError
	}
	return n, nil
}

// define the reducer function
func sumNumbers(acc int, val int) (int, error) {
	acc = acc + val
	return acc, nil
}

func sliceOfIntegersAsStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := range inputValues {
		inputValues[i] = strconv.Itoa(i)
	}
	return inputValues
}

// Test that the workerpool status is "stopped" when the context timeout is fired.
// The timeout is not so short, so the workers of the pool are started when the timeout occurs. Checks that at least one worker has started.
// The worker function has a task to perform that is much longer than the timeout, so it checks that no worker has completed its task
// before the timeout is triggered.
// Then checks that the reduce function returns an error stating that the context has signalled.
func TestMapReduceWithTimeoutManyWorkersRunning(t *testing.T) {
	workDuration := 100 * time.Millisecond
	timeout := workDuration / 10
	testDelay := workDuration * 10

	mapperStarted := false
	taskComplete := false

	var testMu sync.Mutex

	// a context with a timeout that is triggered with a not so short delay
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	do := func(input int) (int, error) {
		testMu.Lock()
		mapperStarted = true
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

	poolSize := 1000
	// construct the pool
	pool := newWorkerpool(poolSize, do)
	pool.start(ctx)

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	valuesToReduce := make([]int, 1000000)
	for i := range valuesToReduce {
		valuesToReduce[i] = i
	}
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func(_ctx context.Context) {
		defer pool.stop()
		for _, v := range valuesToReduce {
			select {
			case pool.inCh <- v:
				continue
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	// Reduce the results into an accumulator
	// Since the timeout is fired before the end of the processing, the errors slice should contain 1 error
	_, err := reduce(ctx, pool, sumNumbers, accInitialValue)

	// wait to make sure that, if the mappers have not been terminated by the context timeout, they have the time to set the taskComplete to true
	time.Sleep(testDelay)

	// check the results of the test
	testMu.Lock()
	if !mapperStarted {
		t.Error("The mapper function has never started, i.e. the workerpool did not start its work")
	}
	testMu.Unlock()
	if taskComplete {
		t.Error("The mapper function was not terminated by the context timeout")
	}
	if err == nil {
		t.Error("MapReduce has not returned an error after the context timeout was triggered")
	}
	if err != ctx.Err() {
		t.Errorf("MapReduce should have returned a context error - got %v", err)
	}
	expectedPoolStatus := wpStopped
	gotStatus := pool.getStatus()
	if expectedPoolStatus != gotStatus {
		t.Errorf("Expected pool status (the pool should have been closed as consequence of the context timeout triggering) %v - got %v",
			expectedPoolStatus, gotStatus)
	}
}
