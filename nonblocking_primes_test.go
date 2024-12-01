package stream_test

import (
	"errors"
	"fmt"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

func TestInternalSieveSize(t *testing.T) {
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

func TestInternalFindNonDivisibleNumbers(t *testing.T) {
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

func TestInternalBootstrapPrimeSourceAndPull(t *testing.T) {
	t.Parallel()

	primes := []int{2, 3, 5}
	source := bootstrapPrimeSource(primes)

	VerifyPrimeBootstrap(t, primes, source)
	VerifyPrimeSourcePull(t, source)
}

func TestInternalNewPrimeSource(t *testing.T) {
	t.Parallel()

	primes := []int{2, 3, 5}
	source := NewPrimeSource(len(primes))

	VerifyPrimeBootstrap(t, primes, source)
	VerifyPrimeSourcePull(t, source)
}

func VerifyPrimeBootstrap(t *testing.T, primes []int, primeStream *PrimeSource) {
	t.Helper()

	if primeStream == nil {
		t.Fatalf("bootstrapPrimeSource returned nil")

		return
	}

	if !slicesEqual(primes, primeStream.smallPrimes) {
		t.Errorf("primeStream.smallPrimes improperly populated: expected %v, got %v",
			primes, primeStream.smallPrimes)
	}

	if expectedSieveSize := calculateSieveSize(primes); expectedSieveSize != primeStream.sieveSize {
		t.Errorf("primeStream.sieveSize improperly populated: expected %v, got %v",
			expectedSieveSize, primeStream.sieveSize)
	}

	nonDivisible := findNonDivisibleNumbers(primes, primeStream.sieveSize)
	if !slicesEqual(nonDivisible, primeStream.sieve) {
		t.Errorf("primeStream.sieve improperly populated: expected %v, got %v",
			nonDivisible, primeStream.sieve)
	}

	if len(primeStream.newPrimes) != 0 {
		t.Errorf("slice primeStream.newPrimes not empty: expected [], got %v",
			primeStream.newPrimes)
	}

	if cap(primeStream.newPrimes) <= len(primes) {
		t.Errorf("slice primeStream.newPrimes not allocated enough size: expected > %d, got %d",
			len(primes), cap(primeStream.newPrimes))
	}
}

func VerifyPrimeSourcePull(t *testing.T, primeStream stream.Source[int]) {
	t.Helper()

	expectedPrimes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}
	for _, expected := range expectedPrimes {
		val, err := primeStream.Pull(stream.Blocking) // Blocking mode
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
	val, err := primeStream.Pull(stream.NonBlocking)
	if val != nil {
		t.Errorf("Expected (nil, data not ready) in non-blocking mode, got (%v, %v)", val, err)
	}

	if !errors.Is(err, stream.ErrNoDataYet) {
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
// how to use sources.
func calculateSieveSize(smallPrimes []int) int {
	source := stream.NewSliceSource(smallPrimes)

	consumer := stream.NewReducer(1, func(acc, next int) int { return acc * next })

	result, err := consumer.Reduce(source)
	if err != nil {
		panic(fmt.Sprintf("Got unexpected error: %v", err))
	}

	return result
}

// predictSieveCapacity calculates the number of members in the sieve.
func predictSieveCapacity(smallPrimes []int) int {
	source := stream.NewSliceSource(smallPrimes)

	consumer := stream.NewReducer(1, func(acc, next int) int { return acc * (next - 1) })

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
	sink := stream.NewSliceSink(&nonDivNums)
	counter := 0
	source := stream.SourceFunc[int](func(_ stream.BlockingType) (*int, error) {
		if counter < sieveSize {
			val := counter
			counter++

			return &val, nil
		}

		return nil, stream.ErrEndOfData
	}, func() {})
	filter := stream.NewFilter(source, func(val int) bool {
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

// PrimeSource emits a list of prime integers.
type PrimeSource struct {
	smallPrimes []int
	sieveSize   int
	sieve       []int
	newPrimes   []int
	counter     int // start at 0 - len(smallPrimes)
}

// NewPrimeSource creates a new PrimeSource to generate primes from a fixed sieve.
// While not the most efficient prime generator, it gives an opportunity to demo
// non-blocking source behavior.
func NewPrimeSource(sievePrimeCount int) *PrimeSource {
	if sievePrimeCount < 1 {
		panic(fmt.Sprintf("invalid sievePrimeCount: %d", sievePrimeCount))
	}

	initialPrimeSource := bootstrapPrimeSource([]int{2})

	if sievePrimeCount == 1 {
		return initialPrimeSource
	}

	smallPrimesSlice := make([]int, 0, sievePrimeCount)
	smallPrimeGenerator := stream.NewSliceSink(&smallPrimesSlice)

	primes, err := smallPrimeGenerator.Append(stream.NewTaker(initialPrimeSource, sievePrimeCount))
	if err != nil {
		panic(fmt.Sprintf("Got unexpected error: %v", err))
	}

	return bootstrapPrimeSource(*primes)
}

// bootstrapPrimeSource creates a new PrimeSource to generate primes from a fixed sieve.
func bootstrapPrimeSource(smallPrimes []int) *PrimeSource {
	sieveSize := calculateSieveSize(smallPrimes)
	nonDivisible := findNonDivisibleNumbers(smallPrimes, sieveSize)

	return &PrimeSource{
		smallPrimes: smallPrimes,
		sieveSize:   sieveSize,
		sieve:       nonDivisible,
		newPrimes:   make([]int, 0, 4*len(smallPrimes)),
		counter:     0 - len(smallPrimes),
	}
}

// Pull emits the next prime element.
func (ps *PrimeSource) Pull(blocks stream.BlockingType) (*int, error) {
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
			if blocks == stream.NonBlocking {
				// we will only test one number per iteration for primeness.
				return nil, stream.ErrNoDataYet
			}

			return ps.Pull(blocks)
		}
	}

	ps.newPrimes = append(ps.newPrimes, potentialPrime)

	return &potentialPrime, nil
}

// Close tells the source no more data will be Pull()ed.
func (ps *PrimeSource) Close() {
}
