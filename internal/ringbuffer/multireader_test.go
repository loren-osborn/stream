package ringbuffer_test

import (
	"reflect"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream/internal/ringbuffer"
)

func TestMultiReaderBuf_AppendAndRetrieve(t *testing.T) {
	t.Parallel()

	mrBuf := ringbuffer.NewMultiReaderBuf[int](3, 1)

	if idx1 := mrBuf.Append(100); idx1 != 0 {
		t.Errorf("Expected index 0, got %d", idx1)
	}

	if idx2 := mrBuf.Append(200); idx2 != 1 {
		t.Errorf("Expected index 1, got %d", idx2)
	}

	count := 0

	mrBuf.ReaderRange(0, func(index int, value int) bool {
		if (index == 0 && value != 100) || (index == 1 && value != 200) {
			t.Errorf("Unexpected value at index %d: %d", index, value)
		}

		count++

		return true
	})

	if count != 2 {
		t.Errorf("Expected 2 iterations, got %d", count)
	}
}

func TestMultiReaderBuf_Discard(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[int](0, 1)
	ringBuf.Append(10)
	ringBuf.Append(20)
	ringBuf.Append(30)

	ringBuf.ReaderDiscard(0, 2)

	if ringBuf.ReaderRangeFirst(0) != 2 {
		t.Errorf("Expected RangeFirst to be 2, got %d", ringBuf.ReaderRangeFirst(0))
	}

	expected := []int{30}

	var result []int

	ringBuf.ReaderRange(0, func(_ int, value int) bool {
		result = append(result, value)

		return true
	})

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// func TestMultiReaderBuf_Resize(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[int](3)

// 	if ringBuf.Cap() != 3 {
// 		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
// 	}

// 	if ringBuf.Len() != 0 {
// 		t.Errorf("Expected length 0, got %d", ringBuf.Len())
// 	}

// 	ringBuf.Append(1)
// 	ringBuf.Append(2)
// 	ringBuf.Append(3)

// 	if ringBuf.Cap() != 3 {
// 		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
// 	}

// 	if ringBuf.Len() != 3 {
// 		t.Errorf("Expected length 3, got %d", ringBuf.Len())
// 	}

// 	ringBuf.Resize(10)

// 	if ringBuf.Cap() != 10 {
// 		t.Errorf("Expected capacity 10, got %d", ringBuf.Cap())
// 	}

// 	if ringBuf.Len() != 3 {
// 		t.Errorf("Expected length 3, got %d", ringBuf.Len())
// 	}

// 	expected := []int{1, 2, 3}

// 	var result []int

// 	ringBuf.Range(func(_ int, value int) bool {
// 		result = append(result, value)

// 		return true
// 	})

// 	if !reflect.DeepEqual(result, expected) {
// 		t.Errorf("Expected %v, got %v", expected, result)
// 	}
// }

// func TestMultiReaderBuf_Empty(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[uint](5)

// 	ringBuf.Append(1)
// 	ringBuf.Append(2)
// 	ringBuf.Append(3)

// 	ringBuf.Empty()

// 	if len(ringBuf.ToSlice()) != 0 {
// 		t.Errorf("Expected empty buffer, got %v", ringBuf.ToSlice())
// 	}

// 	if ringBuf.Cap() != 5 {
// 		t.Errorf("Expected capacity 5, got %d", ringBuf.Cap())
// 	}
// }

// func TestMultiReaderBuf_Range(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[string](0)

// 	ringBuf.Append("ten")
// 	ringBuf.Append("twenty")
// 	ringBuf.Append("thirty")

// 	var result []string

// 	mapCopy := ringBuf.ToMap()

// 	itCount := 0

// 	ringBuf.Range(func(idx int, value string) bool {
// 		result = append(result, value)
// 		itCount++

// 		if mapVal, ok := mapCopy[idx]; !ok || mapVal != value {
// 			if !ok {
// 				t.Errorf("Expected map key %v, missing!", idx)
// 			} else {
// 				t.Errorf("Expected map[%v] == \"%v\", found \"%v\"", idx, value, mapVal)
// 			}
// 		}

// 		return true
// 	})

// 	if mapLen := len(mapCopy); mapLen != ringBuf.Len() {
// 		t.Errorf("Expected %d elements in mapCopy, found %d.", ringBuf.Len(), mapLen)
// 	}

// 	if itCount != ringBuf.Len() {
// 		t.Errorf("Expected %d iterations, saw %d.", ringBuf.Len(), itCount)
// 	}

// 	expected := []string{"ten", "twenty", "thirty"}
// 	if !reflect.DeepEqual(result, expected) {
// 		t.Errorf("Expected %v, got %v", expected, result)
// 	}
// }

func TestMultiReaderBuf_Range_EarlyExit(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[uint](0, 1)

	ringBuf.Append(1)
	ringBuf.Append(2)
	ringBuf.Append(3)
	ringBuf.Append(4)
	ringBuf.Append(5)

	var result []uint

	ringBuf.ReaderRange(0, func(_ int, value uint) bool {
		result = append(result, value)

		return value < 3 // Stop after value 3
	})

	expected := []uint{1, 2, 3}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// //nolint: funlen // *FIXME*
// func TestMultiReaderBuf_ConcurrentAccess(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[int](0)
// 	someFunc := func(in int) int { return in*3 + 7 }

// 	var waitGrp sync.WaitGroup

// 	// Writer goroutine
// 	waitGrp.Add(1)

// 	go func() {
// 		defer waitGrp.Done()

// 		dueToDiscard := 0
// 		nextDiscardGroup := 1

// 		for i := range 1000 {
// 			for j := range 2 {
// 				index := i*2 + j
// 				writeIndex := ringBuf.Append(someFunc(index))

// 				if writeIndex != index {
// 					t.Errorf("Unexpected write index %d when %d expected", writeIndex, index)
// 				}

// 				if val := ringBuf.At(writeIndex); val != someFunc(index) {
// 					t.Errorf("Unexpected value %d when %d expected", val, someFunc(index))
// 				}
// 			}

// 			dueToDiscard++

// 			if dueToDiscard >= nextDiscardGroup {
// 				ringBuf.Discard(nextDiscardGroup)

// 				dueToDiscard -= nextDiscardGroup

// 				nextDiscardGroup++
// 			}
// 		}
// 	}()

// 	// Reader goroutine
// 	waitGrp.Add(1)

// 	go func() {
// 		defer waitGrp.Done()

// 		for range 1000 {
// 			lastIndex := -1

// 			ringBuf.Range(func(index int, readVal int) bool {
// 				if index <= lastIndex {
// 					t.Errorf("Indices out of order: %d followed by %d", lastIndex, index)
// 				}

// 				if computed := someFunc(index); readVal != computed {
// 					t.Errorf("Unanticipated value: %d when %d expected", readVal, computed)
// 				}

// 				lastIndex = index

// 				return true
// 			})
// 		}
// 	}()

// 	waitGrp.Wait()
// }

// func TestMultiReaderBuf_DiscardInvalidCount(t *testing.T) {
// 	t.Parallel()

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, got none")
// 		} else if r != "Attempted to remove 2 elements when only 1 present" {
// 			t.Errorf("Got panic \"%v\" when \"Attempted to remove 2 elements when only 1 present\" expected", r)
// 		}
// 	}()

// 	rb := ringbuffer.New[int](0)
// 	rb.Append(1)
// 	rb.Discard(2) // Should panic because only one element exists
// }

// func TestMultiReaderBuf_ResizeInvalidCapacity(t *testing.T) {
// 	t.Parallel()

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, got none")
// 		} else if r != "Attempted to resize to 1 elements (not big enough to hold 2 elements)" {
// 			t.Errorf("Got panic \"%v\" when \"Attempted to resize to 1 elements (not big enough "+
// 				"to hold 2 elements)\" expected", r)
// 		}
// 	}()

// 	rb := ringbuffer.New[int](0)
// 	rb.Append(1)
// 	rb.Append(2)
// 	rb.Resize(1) // Should panic because size is 2
// }

// func TestMultiReaderBuf_Range_PanicHandling(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[int](1)

// 	// Simulate a panic during iteration
// 	ringBuf.Range(func(_ int, _ int) bool {
// 		panic("empty buffer... no iterations... no panic!")
// 	})

// 	ringBuf.Append(2)
// 	ringBuf.Append(3)

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, but no panic occurred")
// 		} else if r != "simulated panic" {
// 			t.Errorf("Got panic \"%v\" when \"simulated panic\" expected", r)
// 		}
// 	}()

// 	// Simulate a panic during iteration
// 	ringBuf.Range(func(_ int, _ int) bool {
// 		panic("simulated panic")
// 	})
// }

// func TestMultiReaderBuf_NegInitialSize_PanicHandling(t *testing.T) {
// 	t.Parallel()

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, but no panic occurred")
// 		} else if r != "capacity must be greater than zero" {
// 			t.Errorf("Got panic \"%v\" when \"capacity must be greater than zero\" expected", r)
// 		}
// 	}()

// 	ringbuffer.New[int](-1)
// }

// func TestMultiReaderBuf_IndexBefore_PanicHandling(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[int](0)

// 	ringBuf.Append(10)
// 	ringBuf.Append(20)
// 	ringBuf.Append(30)
// 	ringBuf.Append(40)
// 	ringBuf.Append(50)
// 	ringBuf.Discard(2)

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, but no panic occurred")
// 		} else if r != "Attempted to access index 1 before initial index 2" {
// 			t.Errorf("Got panic \"%v\" when \"Attempted to access index 1 before initial index 2\" expected", r)
// 		}
// 	}()

// 	// Panic attempting to set discarded element
// 	ringBuf.Set(1, 200)
// }

// func TestMultiReaderBuf_IndexAfter_PanicHandling(t *testing.T) {
// 	t.Parallel()

// 	ringBuf := ringbuffer.New[int](0)

// 	ringBuf.Append(10)
// 	ringBuf.Append(20)
// 	ringBuf.Append(30)
// 	ringBuf.Append(40)
// 	ringBuf.Append(50)
// 	ringBuf.Discard(2)

// 	rangeLen := ringBuf.RangeLen()
// 	if rangeLen != 5 {
// 		t.Errorf("Exprected RangeLen() == 5 got %d", rangeLen)
// 	}

// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, but no panic occurred")
// 		} else if r != "Attempted to access index 5 after final index 4" {
// 			t.Errorf("Got panic \"%v\" when \"Attempted to access index 5 after final index 4\" expected", r)
// 		}
// 	}()

// 	// Panic attempting to set discarded element
// 	ringBuf.Set(rangeLen, 60)
// }
