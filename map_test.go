package mapreduce_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/EnricoPicci/mapreduce"
)

// function used as mapper in the test
var intToStringReverseOrder = func(val int) (string, error) {
	// wait for a duration which decreases when the input val increases
	time.Sleep(10*time.Millisecond - time.Duration(val)*time.Millisecond)
	return fmt.Sprint(val), nil
}

// Tests that a slice of integers is mapped to a slice of strings
func TestMapSliceOfInts(t *testing.T) {
	// values that have to be mapped
	numOfvaluesToMap := 10
	valuesToMap := FromZeroToN(numOfvaluesToMap)

	concurrent := numOfvaluesToMap / 2

	// Map to a slice of strings
	mappedSlice, err := mapreduce.Map(context.Background(), concurrent, valuesToMap, intToStringReverseOrder)

	// check that the mapped slice is correct
	expectedSliceLen := numOfvaluesToMap
	gotSliceLen := len(mappedSlice)
	if expectedSliceLen != gotSliceLen {
		t.Errorf("Expected length %v - got %v", expectedSliceLen, gotSliceLen)
	}
	expectedOrderedSlice := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	// sort the mapped slice since the order is not guaranteed given that concurrency is > 1
	sort.Strings(mappedSlice)
	for i, gotVal := range mappedSlice {
		expectedVal := expectedOrderedSlice[i]
		if expectedVal != gotVal {
			t.Errorf("Expected (at position %v) val %v - got %v", i, expectedVal, gotVal)
		}
	}

	// check that there is no error
	if err != nil {
		t.Errorf("Expected no error - got %v", err)
	}
}

// Test that the order of the mapped slice is not the same as the original slice
func TestMapSliceOfIntsUnordered(t *testing.T) {
	// values that have to be mapped
	numOfvaluesToMap := 8
	valuesToMap := FromZeroToN(numOfvaluesToMap)

	// we need as many concurrent goroutines as the values if we want this test to succeed since the test assumes the
	// order of the mapped slice is the reverse as the order of the input slice - the order is reverse since the mapper function
	// has a delay which decreases the higher the value of the integer that it has to map
	concurrent := numOfvaluesToMap

	// Map to a slice of strings
	mappedSlice, err := mapreduce.Map(context.Background(), concurrent, valuesToMap, intToStringReverseOrder)

	// check that the mapped slice order is the reverse of the starting slice, since the
	// mapper function applies a delay which decreases with the increase of the integer value passed to it as parameter
	// the order is reverse
	expectedOrderedSlice := []string{"7", "6", "5", "4", "3", "2", "1", "0"}
	for i, gotVal := range mappedSlice {
		expectedVal := expectedOrderedSlice[i]
		if expectedVal != gotVal {
			t.Errorf("Expected (at position %v) val %v - got %v", i, expectedVal, gotVal)
		}
	}
	expectedSliceLen := numOfvaluesToMap
	gotSliceLen := len(mappedSlice)
	if expectedSliceLen != gotSliceLen {
		t.Errorf("Expected length %v - got %v", expectedSliceLen, gotSliceLen)
	}

	// check that there is no error
	if err != nil {
		t.Errorf("Expected no error - got %v", err)
	}
}

// Test that the order of the mapped slice is the same as the original slice since concurrency is set to 1 (i.e. no concurrency)
func TestMapSliceOfIntsOrderedWithConcurrent1(t *testing.T) {
	// values that have to be mapped
	numOfvaluesToMap := 8
	valuesToMap := FromZeroToN(numOfvaluesToMap)

	// if concurrency is 1, then the order of the mapped slice is the same as the order of the input slice
	concurrent := 1

	// Map to a slice of strings
	mappedSlice, err := mapreduce.Map(context.Background(), concurrent, valuesToMap, intToStringReverseOrder)

	// check that the mapped slice order is the reverse of the starting slice, since the
	// mapper function applies a delay which decreases with the increase of the integer value passed to it as parameter
	// the order is reverse
	expectedOrderedSlice := []string{"0", "1", "2", "3", "4", "5", "6", "7"}
	for i, gotVal := range mappedSlice {
		expectedVal := expectedOrderedSlice[i]
		if expectedVal != gotVal {
			t.Errorf("Expected (at position %v) val %v - got %v", i, expectedVal, gotVal)
		}
	}
	expectedSliceLen := numOfvaluesToMap
	gotSliceLen := len(mappedSlice)
	if expectedSliceLen != gotSliceLen {
		t.Errorf("Expected length %v - got %v", expectedSliceLen, gotSliceLen)
	}

	// check that there is no error
	if err != nil {
		t.Errorf("Expected no error - got %v", err)
	}
}

// Tests that, if the context signals (e.g. because of timeout) an error is returned
func TestMapContextSignals(t *testing.T) {
	// values that have to be mapped
	numOfvaluesToMap := 10
	valuesToMap := FromZeroToN(numOfvaluesToMap)

	concurrent := numOfvaluesToMap / 2

	delay := time.Millisecond
	var mapWithDelay = func(val int) (string, error) {
		time.Sleep(delay)
		return fmt.Sprint(val), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), delay/10)
	defer cancel()

	// Map to a slice of strings
	mappedSlice, err := mapreduce.Map(ctx, concurrent, valuesToMap, mapWithDelay)

	// check that the mapped slice is empty since the context signals before the mapping completes
	expectedSliceLen := 0
	gotSliceLen := len(mappedSlice)
	if expectedSliceLen != gotSliceLen {
		t.Errorf("Expected length %v - got %v", expectedSliceLen, gotSliceLen)
	}
	// check that there is an error
	if err == nil {
		t.Error("An error is expected")
	}
}

// Test that with MarOrdered function the order of the mapped slice is maintained
func TestMapUnordered(t *testing.T) {
	// values that have to be mapped
	numOfvaluesToMap := 8
	valuesToMap := FromZeroToN(numOfvaluesToMap)

	// set maximum concurrency to have all the values of the input slice processed concurrently
	concurrent := numOfvaluesToMap

	// Map to a slice of strings using MapOrdered function
	mappedSlice, err := mapreduce.MapOrdered(context.Background(), concurrent, valuesToMap, intToStringReverseOrder)

	// check that the mapped slice order is the same as that of the starting slice
	expectedOrderedSlice := []string{"0", "1", "2", "3", "4", "5", "6", "7"}
	for i, gotVal := range mappedSlice {
		expectedVal := expectedOrderedSlice[i]
		if expectedVal != gotVal {
			t.Errorf("Expected (at position %v) val %v - got %v", i, expectedVal, gotVal)
		}
	}
	expectedSliceLen := numOfvaluesToMap
	gotSliceLen := len(mappedSlice)
	if expectedSliceLen != gotSliceLen {
		t.Errorf("Expected length %v - got %v", expectedSliceLen, gotSliceLen)
	}

	// check that there is no error
	if err != nil {
		t.Errorf("Expected no error - got %v", err)
	}
}
