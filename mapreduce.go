// Package mapreduce implements the concurrent MapReduce function

package mapreduce

import (
	"context"
	"fmt"
)

// A MapReduceError is an error returned by the MapReduce function. It wraps the errors that may have occurred during the map and reduce operations.
// The Errors slice can contain values of type either MapError or ReduceError.
// It implements the error interface.
type MapReduceError struct {
	// errors that have been returned by the mapper function
	Errors []error
}

func (err MapReduceError) Error() string {
	return fmt.Sprintf("%v errors while map-reducing", len(err.Errors))
}

// A MapError is wrapper around an error returned by the mapper function.
// Use type assertion or errors.As function to check if the error is of type MapError.
type MapError struct {
	Err error
}

func (e MapError) Error() string {
	return "Map error: " + e.Err.Error()
}
func (e MapError) Unwrap() error {
	return e.Err
}

// A ReduceError is wrapper around an error returned by the mapper function.
// Use type assertion or errors.As function to check if the error is of type ReduceError.
type ReduceError struct {
	Err error
}

func (e ReduceError) Error() string {
	return "Reduce error: " + e.Err.Error()
}
func (e ReduceError) Unwrap() error {
	return e.Err
}

// MapReduce process all the inputValues and returns a reduced result.
// First the values of inputValues are mapped using the mapper function.
// Then all mapped values are reduced using the reducer function.
// If errors occur, a MapReduceError wrapping all the errors is returned.
// If ctx is cancelled or times-out, then the context error is returned.
func MapReduce[I, O, R any](
	ctx context.Context,
	concurrent int,
	inputValues []I,
	mapper func(I) (O, error),
	reducer func(R, O) (R, error),
	initialValue R,
) (R, error) {
	// create and start the pool
	pool := newWorkerpool(concurrent, mapper)
	pool.start(ctx)

	// launch the feed of the workers pool as a goroutine
	go feed(ctx, inputValues, pool)

	// run the reduce step
	acc, err := reduce(ctx, pool, reducer, initialValue)

	return acc, err
}

// feed sends the input values to the pool.
// When all the values have been sent or the context signals, the pool is stopped
func feed[I, O any](ctx context.Context, inputValues []I, pool *workerpool[I, O]) {
	defer pool.stop()
	for _, v := range inputValues {
		select {
		case pool.inCh <- v:
			continue
		case <-ctx.Done():
			return
		}
	}
}

// reduce the results returned by the processing of the pool into an accumulator. Returns the accumulator and a slice of errors, if errors occur
func reduce[I, O, R any](ctx context.Context, pool *workerpool[I, O], reducer func(R, O) (R, error), acc R) (R, error) {
	errors := []error{}
	var err error

	for {
		select {
		case res, more := <-pool.outCh:
			if !more {
				goto end
			}
			var _err error
			acc, _err = reducer(acc, res)
			if _err != nil {
				errors = append(errors, MapError{_err})
			}
		case _err, more := <-pool.errCh:
			if more {
				errors = append(errors, _err)
			}
		case <-ctx.Done():
			return acc, ctx.Err()
		}
	}

end:
	if len(errors) > 0 {
		err = MapReduceError{errors}
	}

	return acc, err
}
