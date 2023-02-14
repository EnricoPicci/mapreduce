package mapreduce_test

import (
	"context"
	"fmt"
	"strconv"

	"github.com/EnricoPicci/mapreduce"
)

// ExampleMapReduce provides an example of how to use MapReduce function.
// In this example we start from a slice of strings representing integer values, we map the slice to a slice of integers
// and we reduce the mapped slice to the number which is the sum of all the integers
func ExampleMapReduce() {
	// values that have to be mapped and then reduced
	valuesToMapReduce := []string{"1", "3", "6", "1", "3", "6"}

	// mapper function
	mapStringToInt := func(input string) (int, error) {
		return strconv.Atoi(input)
	}
	// reducer function
	sumNumbers := func(acc int, val int) (int, error) {
		acc = acc + val
		return acc, nil
	}

	// number of concurrent goroutines that do the mapping
	concurrent := 3
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToMapReduce, mapStringToInt, sumNumbers, accInitialValue)

	fmt.Println(sum)
	fmt.Println(err)
	// Output:
	// 20
	// <nil>

}
