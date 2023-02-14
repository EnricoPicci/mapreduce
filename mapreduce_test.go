package mapreduce_test

import (
	"context"
	"errors"
	"testing"

	"github.com/EnricoPicci/mapreduce"
)

// In this test a slice of strings representing integers is reduced to a number which is the sum of all the integers using the MapReduce function.
// The test is run with different levels of concurrency.
func TestMapReduceSumOfNumbers(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 1000
	valuesToReduce := FromZeroToN_asStrings(numOfValuesToReduce)

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0

	// expected results
	// the sum of the results received (once each result is converted back to a number) depends on the number of values to reduce
	// since such values are an incresing sequence of integers starting with 0
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce * (numOfValuesToReduce - 1) / 2

	testCases := []struct {
		name       string
		concurrent int
	}{
		{"concurrent_1", 1},
		{"concurrent_1", 10},
		{"concurrent_1", 1000},
		{"concurrent_1", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			concurrent := tc.concurrent
			// Reduce the results into an accumulator
			sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, MapStringToInt, SumNumbers, accInitialValue)

			// check the results of the test
			if err != nil {
				t.Errorf("Expected no error - got %v", err)
			}

			gotSum := sum
			if expectedSum != gotSum {
				t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
			}
		})
	}
}

// In this test a slice of strings representing integers is reduced to a number which is the sum of all the integers using the MapReduce function.
// For one specific number though, the mapper function returns an error.
// The test is run with different levels of concurrency.
func TestMapReduceSumOfNumbersWithError(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 1000
	valuesToReduce := FromZeroToN_asStrings(numOfValuesToReduce)
	numGeneratingError := 5

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0

	// expected results
	expectedNumOfErrors := 1
	// the sum of the results received (once each result is converted back to a number) depends on the number of values to reduce
	// since such values are an incresing sequence of integers starting with 0
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - numGeneratingError

	testCases := []struct {
		name       string
		concurrent int
	}{
		{"concurrent_1", 1},
		{"concurrent_1", 10},
		{"concurrent_1", 1000},
		{"concurrent_1", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			concurrent := tc.concurrent
			// Reduce the results into an accumulator
			sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, MapStringToIntWithError(numGeneratingError), SumNumbers, accInitialValue)

			// check the results of the test
			var mapReduceErr mapreduce.MapReduceError
			if !errors.As(err, &mapReduceErr) {
				t.Fatalf("The error '%v' of type %T is not of type MapReduceError", err, err)
			}
			errors := mapReduceErr.Errors
			gotNumOfErrors := len(errors)
			if expectedNumOfErrors != gotNumOfErrors {
				t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
			}

			gotSum := sum
			if expectedSum != gotSum {
				t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
			}
		})
	}
}

// In this test a series of empty strings is passed to a mapper which expects a string to be converted to an integer.
// Therefore all the values passed to the mapper generate an error.
// The test is run with different levels of concurrency.
func TestMapReduceAllErrors(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 100000
	valuesToReduce := SliceOfEmptyStrings(numOfValuesToReduce)

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0

	// expected results
	expectedNumOfErrors := numOfValuesToReduce
	expectedSum := 0

	testCases := []struct {
		name       string
		concurrent int
	}{
		{"concurrent_1", 1},
		{"concurrent_1", 10},
		{"concurrent_1", 1000},
		{"concurrent_1", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			concurrent := tc.concurrent
			// Reduce the results into an accumulator
			sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, MapStringToInt, SumNumbers, accInitialValue)

			// check the results of the test
			reduceErr := err.(mapreduce.MapReduceError)
			errors := reduceErr.Errors
			gotNumOfErrors := len(errors)
			if expectedNumOfErrors != gotNumOfErrors {
				t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
			}

			gotSum := sum
			if expectedSum != gotSum {
				t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
			}

		})
	}
}

// In this test the reducer function returns an error under a specific condition.
// The test checks that the error returned by the reducer is returned in the result of MapReduce and that the value returned is the
// result of the reducer function ignoring the value that has caused the error.
// The test is run with different levels of concurrency.
func TestMapReduce_ReduceError(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 10000
	valuesToReduce := FromZeroToN_asStrings(numOfValuesToReduce)
	numGeneratingErrorInReducer := 3

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0

	// expected results
	expectedNumOfErrors := 1
	// the sum of the results received (once each result is converted back to a number) depends on the number of values to reduce
	// since such values are an incresing sequence of integers starting with 0
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - numGeneratingErrorInReducer

	testCases := []struct {
		name       string
		concurrent int
	}{
		{"concurrent_1", 1},
		{"concurrent_1", 10},
		{"concurrent_1", 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			concurrent := tc.concurrent
			// Reduce the results into an accumulator
			sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, MapStringToInt,
				SumNumbersWithError(numGeneratingErrorInReducer), accInitialValue)

			// check the results of the test
			reduceErr := err.(mapreduce.MapReduceError)
			errors := reduceErr.Errors
			gotNumOfErrors := len(errors)
			if expectedNumOfErrors != gotNumOfErrors {
				t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
			}

			gotSum := sum
			if expectedSum != gotSum {
				t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
			}

		})
	}
}

// In this test both the mapper and the reducer functions return an error under a specific conditions.
// The test checks that the errors returned by the map and the reducer are returned in the result of MapReduce and that the value returned is the
// result of the reducer function ignoring the values that has caused the errors.
// The test is run with different levels of concurrency.
func TestMapReduce_Map_And_ReduceError(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 10000
	valuesToReduce := FromZeroToN_asStrings(numOfValuesToReduce)
	numGeneratingErrorInMapper := 3
	numGeneratingErrorInReducer := 5

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0

	// expected results
	expectedNumOfErrors := 2
	// the sum of the results received (once each result is converted back to a number) depends on the number of values to reduce
	// since such values are an incresing sequence of integers starting with 0
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - numGeneratingErrorInMapper - numGeneratingErrorInReducer

	testCases := []struct {
		name       string
		concurrent int
	}{
		{"concurrent_1", 1},
		{"concurrent_1", 10},
		{"concurrent_1", 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			concurrent := tc.concurrent
			// Reduce the results into an accumulator
			sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce,
				MapStringToIntWithError(numGeneratingErrorInMapper),
				SumNumbersWithError(numGeneratingErrorInReducer), accInitialValue)

			// check the results of the test
			reduceErr := err.(mapreduce.MapReduceError)
			errors := reduceErr.Errors
			gotNumOfErrors := len(errors)
			if expectedNumOfErrors != gotNumOfErrors {
				t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
			}

			gotSum := sum
			if expectedSum != gotSum {
				t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
			}

		})
	}
}
