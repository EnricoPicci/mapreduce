# mapreduce

mapreduce is a package that contains a concurrent implementation of **MapReduce** and **Map** functions based on Go generics.

More details on this implementation can be found in an article going to published on Medium.

## MapReduce function

[MapReduce](./mapreduce.go#MapReduce) is a function that transforms a slice of values applying a _mapper_ function and then reduces the _mapped_ values to a single _reduced_ result using a _reducer_ function. Returns an error if an error is encountered.

## Map function

[Map](./map.go#Map) is a function that takes a slice of values as input, transforms each value using a _mapper_ function and then returns a slice of mapped values. If errors occur in the mapping operations, an error wrapping all the errors encountered is returned.

The mapped slice does not necessarly keep the same ordering as the input slice.

[Map](./map.go#Map) uses [MapReduce](#mapreduce-function).

## MapOrdered

[MapOrdered](./map.go#MapOrdered) works like [Map](#map-function) but keeps in the returned slice the same ordering of the input slice.
