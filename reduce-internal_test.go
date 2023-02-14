package mapreduce

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
)

// Tests the reduce function.
// It looks at how the internals work: it creates a workerpool and launches the feed function, as goroutine, to feed the workerpool.
// The workerpool transform a slice of integers in a stream of strings (representing the integers) and the reducer reduces the stream of
// strings exiting the worker pool into a slice of strings (so basically a slice of integers is reduced to a slice of corresponding strings).
// One value in the input slice generates an error in during the mapping.
func TestReduce(t *testing.T) {
	// this is the error value sent if an error occurs
	conversionError := errors.New("Error occurred while converting (as worker of a worker pool)")
	// the error is generated when the
	numberGeneratingError := 1

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
	numOfInputSentToPool := 3
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	feed := func() {
		defer pool.stop()
		for i := 0; i < numOfInputSentToPool; i++ {
			pool.process(i)
		}
	}
	go feed()

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := []string{}
	// define the reducer function
	reducer := func(acc []string, res string) ([]string, error) {
		acc = append(acc, res)
		return acc, nil
	}
	// Reduce the results into an accumulator
	resultsReceived, err := reduce(context.Background(), pool, reducer, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := 1
	reduceErr := err.(MapReduceError)
	gotNumOfErrors := len(reduceErr.Errors)
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
