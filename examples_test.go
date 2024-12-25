// nolint: testpackage // This is to keep the example code in sync with the API
package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// ExampleNewFilter is the first example from the package documentation header.
func ExampleNewFilter() {
	var result []string

	data := []int{1, 2, 3, 4, 5}
	source := NewSliceSource(data)

	squareMapper := NewMapper(source, func(n int) int { return n * n })     // Square each number
	filter := NewFilter(squareMapper, func(n int) bool { return n%2 == 0 }) // Keep even numbers
	toStrMapper := NewMapper(filter, strconv.Itoa)                          // Convert to string
	sink := NewSliceSink(&result)

	outResult, err := sink.Append(context.Background(), toStrMapper)
	if err != nil {
		return // Fail
	}

	fmt.Println(strings.Join(*outResult, ", ")) // Output: 4, 16
}

// ExampleNewReducer is the second example from the package documentation header.
func ExampleNewReducer() {
	data := []int{1, 2, 3, 4, 5}
	source := NewSliceSource(data)
	reducer := NewReducer(0, func(acc, next int) int { return acc + next })
	result, _ := reducer.Reduce(context.Background(), source)
	fmt.Println(result) // Output: 15
}
