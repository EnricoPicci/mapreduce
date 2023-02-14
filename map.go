// implements the concurrent Map function

package mapreduce

import (
	"context"
	"sort"
)

// Map all elements of a slice into a new slice applying the mapper function.
// Returns the new slice or an error if an error occurs in the transformation.
// Uses MapReduce passing a reducer that reduces to a new slice all the mapped values.
// The order of the mapped slice is not guaranteed to be the same as the original slide unless concurrent is 1
func Map[I, O any](
	ctx context.Context,
	concurrent int,
	inputValues []I,
	mapper func(I) (O, error),
) ([]O, error) {
	reducer := func(acc []O, val O) ([]O, error) {
		acc = append(acc, val)
		return acc, nil
	}
	initialValue := []O{}
	return MapReduce(ctx, concurrent, inputValues, mapper, reducer, initialValue)
}

type orderedVal[T any] struct {
	i int
	v T
}

// MapOrdered returns a slice containing the mapped values maintaining the order of the input slice
func MapOrdered[I, O any](
	ctx context.Context,
	concurrent int,
	inputValues []I,
	mapper func(I) (O, error),
) ([]O, error) {
	orderedInputValues := make([]orderedVal[I], len(inputValues))
	for i, inputVal := range inputValues {
		orderedInputValues[i] = orderedVal[I]{i, inputVal}
	}
	orderedMapper := func(orderedInputVal orderedVal[I]) (orderedVal[O], error) {
		v, err := mapper(orderedInputVal.v)
		if err != nil {
			return orderedVal[O]{}, err
		}
		return orderedVal[O]{orderedInputVal.i, v}, nil
	}

	reducer := func(acc []orderedVal[O], val orderedVal[O]) ([]orderedVal[O], error) {
		acc = append(acc, val)
		return acc, nil
	}
	initialValue := []orderedVal[O]{}
	oValues, err := MapReduce(ctx, concurrent, orderedInputValues, orderedMapper, reducer, initialValue)
	sort.Slice(oValues, func(i, j int) bool {
		return oValues[i].i < oValues[j].i
	})
	orderedMappedValues := make([]O, len(oValues))
	for i, orderedOVal := range oValues {
		orderedMappedValues[i] = orderedOVal.v
	}
	return orderedMappedValues, err
}
