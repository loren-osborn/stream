package streams_test

import (
	"errors"
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
			71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 121, 127, 131,
			137, 139, 143, 149, 151, 157, 163, 167, 169, 173, 179, 181, 187,
			191, 193, 197, 199, 209,
		}},
	}

	for idx, testCase := range tests {
		result1 := predictSieveCapacity(testCase.primes)
		if result1 != len(testCase.expected) {
			t.Errorf("case %d: predictSieveCapacity(%v): expected %v, got %v",
				idx, testCase.primes, len(testCase.expected), result1)
		}

		result2 := findNonDivisibleNumbers(testCase.primes, testCase.sieveSize)
		if !slicesEqual(result2, testCase.expected) {
			t.Errorf("case %d: findNonDivisibleNumbers(%v, %d): expected %v, got %v",
				idx, testCase.primes, testCase.sieveSize, testCase.expected, result2)
		}
	}
}

func TestPrimeStream(t *testing.T) {
	t.Parallel()

	primes := []int{2, 3, 5}
	sieveSize := calculateSieveSize(primes)
	nonDivisible := findNonDivisibleNumbers(primes, sieveSize)

	stream, err := NewPrimeStream(primes, sieveSize, nonDivisible)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedPrimes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}
	for _, expected := range expectedPrimes {
		val, err := stream.Pull(streams.Blocking) // Blocking mode
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if val == nil {
			t.Fatalf("Expected prime %d, got nil", expected)
		}

		if *val != expected {
			t.Errorf("Expected prime %d, got %d", expected, *val)
		}
	}

	// Non-blocking behavior should return nil (49 is not prime)
	val, err := stream.Pull(streams.NonBlocking)
	if val != nil {
		t.Errorf("Expected (nil, data not ready) in non-blocking mode, got (%v, %v)", val, err)
	}

	if !errors.Is(err, streams.ErrNoDataYet) {
		t.Fatalf("expected ErrNoDataYet, got %v", err)
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
func predictSieveCapacity(smallPrimes []int) int {
	source := streams.NewSliceSource(smallPrimes)

	consumer := streams.NewReducer(1, func(acc, next int) int { return acc * (next - 1) })

	result, err := consumer.Reduce(source)
	if err != nil {
		panic(fmt.Sprintf("Got unexpected error: %v", err))
	}

	return result
}

// findNonDivisibleNumbers calculates the members of the sieve.
func findNonDivisibleNumbers(smallPrimes []int, sieveSize int) []int {
	sieveCapacity := predictSieveCapacity(smallPrimes)
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

// PrimeStream emits a list of prime integers.
type PrimeStream struct {
	smallPrimes []int
	sieveSize   int
	sieve       []int
	newPrimes   []int
	counter     int // start at 0 - len(smallPrimes)
}

// NewPrimeStream creates a new PrimeStream to generate primes from a fixed sieve.
// While not the most efficient prime generator, it gives an opportunity to demo
// non-blocking stream behavior.
func NewPrimeStream(smallPrimes []int, sieveSize int, sieve []int) (*PrimeStream, error) {
	return &PrimeStream{
		smallPrimes: smallPrimes,
		sieveSize:   sieveSize,
		sieve:       sieve,
		newPrimes:   make([]int, 0, 4*len(smallPrimes)),
		counter:     0 - len(smallPrimes),
	}, nil
}

// Pull emits the next prime element.
func (ps *PrimeStream) Pull(blocks streams.BlockingType) (*int, error) {
	if ps.counter < 0 {
		idx := len(ps.smallPrimes) + ps.counter
		ps.counter++

		return &(ps.smallPrimes[idx]), nil
	}

	getPotentialPrime := func(c int) int {
		sieveIndex := c % len(ps.sieve)
		sieveBlock := (c - sieveIndex) / len(ps.sieve)

		return (ps.sieveSize * sieveBlock) + ps.sieve[sieveIndex]
	}

	for getPotentialPrime(ps.counter) <= ps.smallPrimes[len(ps.smallPrimes)-1] {
		ps.counter++
	}

	potentialPrime := getPotentialPrime(ps.counter)
	ps.counter++

	for _, possibleFactor := range ps.newPrimes {
		if possibleFactor*possibleFactor > potentialPrime {
			break
		}

		if potentialPrime%possibleFactor == 0 {
			if blocks == streams.NonBlocking {
				// we will only test one number per iteration for primeness.
				return nil, streams.ErrNoDataYet
			}

			return ps.Pull(blocks)
		}
	}

	ps.newPrimes = append(ps.newPrimes, potentialPrime)

	return &potentialPrime, nil
}
