package streams_test

import (
	"fmt"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/streams"
)

func TestSieveSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		primes   []int
		expected int
	}{
		{[]int{2, 3}, 6},              // 2 * 3              =    6
		{[]int{2, 3, 5}, 30},          // 2 * 3 * 5          =   30
		{[]int{2, 3, 5, 7}, 210},      // 2 * 3 * 5 * 7      =  210
		{[]int{2, 3, 5, 7, 11}, 2310}, // 2 * 3 * 5 * 7 * 11 = 2310
	}

	for _, tt := range tests {
		result := calculateSieveSize(tt.primes)
		if result != tt.expected {
			t.Errorf("calculateSieveSize(%v): expected %d, got %d", tt.primes, tt.expected, result)
		}
	}
}

func TestFindNonDivisibleNumbers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		primes    []int
		sieveSize int
		expected  []int
	}{
		{[]int{2, 3, 5}, 30, []int{1, 7, 11, 13, 17, 19, 23, 29}},
		{[]int{2, 3}, 6, []int{1, 5}},
		{[]int{2, 3, 5, 7}, 210, []int{
			1, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67,
			71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 121, 127, 131, 137,
			139, 143, 149, 151, 157, 163, 167, 169, 173, 179, 181, 187,
			191, 193, 197, 199, 209,
		}},
	}

	for idx, testCase := range tests {
		result1 := predictSieveCapacity(testCase.primes, testCase.sieveSize)
		if result1 != len(testCase.expected) {
			t.Errorf("case %d: predictSieveCapacity(%v, %d): expected %v, got %v",
				idx, testCase.primes, testCase.sieveSize, len(testCase.expected), result1)
		}

		result2 := findNonDivisibleNumbers(testCase.primes, testCase.sieveSize)
		if !slicesEqual(result2, testCase.expected) {
			t.Errorf("case %d: findNonDivisibleNumbers(%v, %d): expected %v, got %v",
				idx, testCase.primes, testCase.sieveSize, testCase.expected, result2)
		}
	}
}

// slicesEqual compares to arrays for equality.
func slicesEqual[T comparable](aSlice, bSlice []T) bool {
	if len(aSlice) != len(bSlice) {
		return false
	}

	for i, aVal := range aSlice {
		if aVal != bSlice[i] {
			return false
		}
	}

	return true
}

// calculateSieveSize calculates the sieve size for a list of primes. This is
// far from the most efficient way to do this, but it's serving as an example of
// how to use streams.
func calculateSieveSize(smallPrimes []int) int {
	source := streams.NewSliceSource(smallPrimes)

	consumer := streams.NewReducer(1, func(acc, next int) int { return acc * next })

	result, err := consumer.Reduce(source)
	if err != nil {
		panic(fmt.Sprintf("Got unexpected error: %v", err))
	}

	return result
}

// predictSieveCapacity calculates the number of members in the sieve.
func predictSieveCapacity(smallPrimes []int, sieveSize int) int {
	source := streams.NewSliceSource(smallPrimes)

	consumer := streams.NewReducer(sieveSize, func(acc, next int) int { return acc * (next - 1) / next })

	result, err := consumer.Reduce(source)
	if err != nil {
		panic(fmt.Sprintf("Got unexpected error: %v", err))
	}

	return result
}

// findNonDivisibleNumbers calculates the members of the sieve.
func findNonDivisibleNumbers(smallPrimes []int, sieveSize int) []int {
	sieveCapacity := predictSieveCapacity(smallPrimes, sieveSize)
	nonDivNums := make([]int, 0, sieveCapacity)
	sink := streams.NewSliceSink(&nonDivNums)
	counter := 0
	source := streams.SourceFunc[int](func(_ streams.BlockingType) (*int, error) {
		if counter < sieveSize {
			val := counter
			counter++

			return &val, nil
		}

		return nil, streams.ErrEndOfData
	})
	filter := streams.NewFilter(source, func(val int) bool {
		for _, factor := range smallPrimes {
			if val%factor == 0 {
				return false
			}
		}

		return true
	})

	destSlice, err := sink.Append(filter)
	if &nonDivNums != destSlice {
		panic(fmt.Sprintf("Append returned wrong array slice: %v, expected %v", destSlice, &nonDivNums))
	}

	if err != nil {
		panic(fmt.Sprintf("Append returned unexpected error: %v, expected nil", err))
	}

	return nonDivNums
}
