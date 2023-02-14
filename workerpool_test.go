package mapreduce

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// TestPool simulates to create a worker pool and let it process a series of input values.
// For each input value the pool returns a result in its OutCh. If an error occurs, the error is returned in the pool's ErrCh.
// In this example the pool receives some integers and has to convert them to strings.
// The converter function simulates an error for a certain value.
// The test checks that all the strings expected are received on the output channel and that all the errors expected are received on the error channel
func TestPool(t *testing.T) {
	// this is the error value sent if an error occurs
	conversionError := errors.New("Error occurred while processing as worker in a workers pool")
	// the error is generated when the
	numberGeneratingError := 4

	// parameters to constuct the pool
	poolSize := 1000
	do := func(in int) (string, error) {
		// simulates that for certain values an error is returned
		if in == numberGeneratingError {
			return "", conversionError
		}
		return fmt.Sprintf("%v", in), nil
	}
	// construct the pool
	pool := newWorkerpool(poolSize, do)
	// start the pool
	ctx := context.Background()
	pool.start(ctx)

	// number of input values that will be sent to the pool to be processed
	numOfInputSentToPool := 1000000
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func() {
		defer pool.stop()
		for i := 0; i < numOfInputSentToPool; i++ {
			pool.process(i)
		}
	}()

	// slices where we collect the results of the processing performed by the pool and the potential errors received from the pool
	resultsReceived := []string{}
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

	// check the results of the test
	expectedNumOfErrors := 1
	gotNumOfErrors := len(errorsReceived)
	expectedNumOfResults := numOfInputSentToPool - expectedNumOfErrors
	gotNumOfResults := len(resultsReceived)
	if expectedNumOfResults != gotNumOfResults {
		t.Errorf("Expected number of results %v - got %v", expectedNumOfResults, gotNumOfResults)
	}
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfResults, gotNumOfErrors)
	}

	// check that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfInputSentToPool*(numOfInputSentToPool-1)/2 - numberGeneratingError
	gotSum := 0
	for _, v := range resultsReceived {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		gotSum = gotSum + n
	}
	if expectedSum != gotSum {
		t.Errorf("Expected sum of the numbers received %v - got %v", expectedSum, gotSum)
	}
}

// TestPoolOneWorker test a pool with just 1 worker
func TestPoolOneWorker(t *testing.T) {
	// this is the error value sent if an error occurs
	conversionError := errors.New("Error occurred while processing as worker in a workers pool")
	// the error is generated when the
	numberGeneratingError := 2

	// parameters to constuct the pool
	poolSize := 1
	do := func(in int) (string, error) {
		// simulates that for certain values an error is returned
		if in == numberGeneratingError {
			return "", conversionError
		}
		return fmt.Sprintf("%v", in), nil
	}
	// construct the pool
	pool := newWorkerpool(poolSize, do)
	// start the pool
	ctx := context.Background()
	pool.start(ctx)

	// number of input values that will be sent to the pool to be processed
	numOfInputSentToPool := 30
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func() {
		defer pool.stop()
		for i := 0; i < numOfInputSentToPool; i++ {
			pool.process(i)
		}
	}()

	// slices where we collect the results of the processing performed by the pool and the potential errors received from the pool
	resultsReceived := []string{}
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

	// check the results of the test
	expectedNumOfErrors := 1
	gotNumOfErrors := len(errorsReceived)
	expectedNumOfResults := numOfInputSentToPool - expectedNumOfErrors
	gotNumOfResults := len(resultsReceived)
	if expectedNumOfResults != gotNumOfResults {
		t.Errorf("Expected number of results %v - got %v", expectedNumOfResults, gotNumOfResults)
	}
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfResults, gotNumOfErrors)
	}

	// chech that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfInputSentToPool*(numOfInputSentToPool-1)/2 - numberGeneratingError
	gotSum := 0
	for _, v := range resultsReceived {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		gotSum = gotSum + n
	}
	if expectedSum != gotSum {
		t.Errorf("Expected sum of the numbers received %v - got %v", expectedSum, gotSum)
	}
}
