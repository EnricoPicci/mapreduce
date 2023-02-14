// this file contains the functions which are shared among various tests

package mapreduce_test

import (
	"fmt"
	"strconv"
)

// MapStringToIntWithError returns a function that converts a string to an integer but, for a specific number, returns an error
func MapStringToIntWithError(numError int) func(input string) (int, error) {
	return func(input string) (int, error) {
		n, err := strconv.Atoi(input)
		if err != nil {
			panic(err)
		}
		if n == numError {
			return 0, fmt.Errorf("Error while mapping %v to int", n)
		}
		return n, nil
	}
}

var numberGeneratingError = 4

// MapIntToString converts an int to a string
func MapIntToString(input int) (string, error) {
	if input == numberGeneratingError {
		return "", fmt.Errorf("Error while mapping %v to string", input)
	}
	return fmt.Sprintf("%v", input), nil
}

// SumNumbers sums the number received to the accumulator received
func SumNumbers(acc int, val int) (int, error) {
	acc = acc + val
	return acc, nil
}

// SumNumbersWithError returns a function that sums the number received to the accumulator received but returns an error for a specific value
func SumNumbersWithError(numError int) func(acc int, val int) (int, error) {
	return func(acc int, val int) (int, error) {
		if val == numError {
			return acc, fmt.Errorf("Error while mapping %v to int", val)
		}
		acc = acc + val
		return acc, nil
	}
}

// FromZeroToN returns a slice of integers from 0 to n
func FromZeroToN(n int) []int {
	inputValues := make([]int, n)
	for i := range inputValues {
		inputValues[i] = i
	}
	return inputValues
}

// FromZeroToN returns a slice of integers as strings from 0 to n
func FromZeroToN_asStrings(n int) []string {
	inputValues := make([]string, n)
	for i := range inputValues {
		inputValues[i] = strconv.Itoa(i)
	}
	return inputValues
}

// MapStringToInt returns an integer if the input string can be converted to an integer or an error if the string can not be converted to an integer
func MapStringToInt(input string) (int, error) {
	n, err := strconv.Atoi(input)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func SliceOfEmptyStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := 0; i < numOfValuesSentToPool; i++ {
		inputValues[i] = ""
	}
	return inputValues
}
