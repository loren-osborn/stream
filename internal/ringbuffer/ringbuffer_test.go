package ringbuffer_test

import (
	"reflect"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream/internal/ringbuffer"
)

func TestRingBuffer_AppendAndRetrieve(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[int](3)

	if idx1 := ringBuf.Append(100); idx1 != 0 {
		t.Errorf("Expected index 0, got %d", idx1)
	}

	if idx2 := ringBuf.Append(200); idx2 != 1 {
		t.Errorf("Expected index 1, got %d", idx2)
	}

	count := 0

	ringBuf.Range(func(index int, value int) bool {
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

func TestRingBuffer_Discard(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[int](0)
	ringBuf.Append(10)
	ringBuf.Append(20)
	ringBuf.Append(30)

	ringBuf.Discard(2)

	if ringBuf.RangeFirst() != 2 {
		t.Errorf("Expected RangeFirst to be 2, got %d", ringBuf.RangeFirst())
	}

	expected := []int{30}

	var result []int

	ringBuf.Range(func(_ int, value int) bool {
		result = append(result, value)

		return true
	})

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRingBuffer_Resize(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[int](3)

	if ringBuf.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
	}

	if ringBuf.Len() != 0 {
		t.Errorf("Expected length 0, got %d", ringBuf.Len())
	}

	ringBuf.Append(1)
	ringBuf.Append(2)
	ringBuf.Append(3)

	if ringBuf.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
	}

	if ringBuf.Len() != 3 {
		t.Errorf("Expected length 3, got %d", ringBuf.Len())
	}

	ringBuf.Resize(10)

	if ringBuf.Cap() != 10 {
		t.Errorf("Expected capacity 10, got %d", ringBuf.Cap())
	}

	if ringBuf.Len() != 3 {
		t.Errorf("Expected length 3, got %d", ringBuf.Len())
	}

	expected := []int{1, 2, 3}

	var result []int

	ringBuf.Range(func(_ int, value int) bool {
		result = append(result, value)

		return true
	})

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRingBuffer_Empty(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[uint](5)

	ringBuf.Append(1)
	ringBuf.Append(2)
	ringBuf.Append(3)

	ringBuf.Empty()

	if len(ringBuf.ToSlice()) != 0 {
		t.Errorf("Expected empty buffer, got %v", ringBuf.ToSlice())
	}

	if ringBuf.Cap() != 5 {
		t.Errorf("Expected capacity 5, got %d", ringBuf.Cap())
	}
}

func TestRingBuffer_Range(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[string](0)

	ringBuf.Append("ten")
	ringBuf.Append("twenty")
	ringBuf.Append("thirty")

	var result []string

	ringBuf.Range(func(_ int, value string) bool {
		result = append(result, value)

		return true
	})

	expected := []string{"ten", "twenty", "thirty"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRingBuffer_Range_EarlyExit(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewRingBuffer[uint](0)

	ringBuf.Append(1)
	ringBuf.Append(2)
	ringBuf.Append(3)
	ringBuf.Append(4)
	ringBuf.Append(5)

	var result []uint

	ringBuf.Range(func(_ int, value uint) bool {
		result = append(result, value)

		return value < 3 // Stop after value 3
	})

	expected := []uint{1, 2, 3}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// func TestRingBuffer_ConcurrentAccess(t *testing.T) {
// 	rb := ringbuffer.NewRingBuffer
// 	var wg sync.WaitGroup

// 	// Writer goroutine
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 1000; i++ {
// 			rb.Append(i)
// 		}
// 	}()

// 	// Reader goroutine
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		var lastIndex int = -1
// 		for i := 0; i < 1000; i++ {
// 			rb.Range(func(index int, value int) bool {
// 				if index <= lastIndex {
// 					t.Errorf("Indices out of order: %d followed by %d", lastIndex, index)
// 				}
// 				lastIndex = index
// 				return true
// 			})
// 		}
// 	}()

// 	wg.Wait()
// }

// func TestRingBuffer_DiscardInvalidCount(t *testing.T) {
// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, got none")
// 		}
// 	}()

// 	rb := ringbuffer.NewRingBuffer
// 	rb.Append(1)
// 	rb.Discard(2) // Should panic because only one element exists
// }

// func TestRingBuffer_ResizeInvalidCapacity(t *testing.T) {
// 	defer func() {
// 		if r := recover(); r == nil {
// 			t.Errorf("Expected panic, got none")
// 		}
// 	}()

// 	rb := ringbuffer.NewRingBuffer
// 	rb.Append(1)
// 	rb.Append(2)
// 	rb.Resize(1) // Should panic because size is 2
// }

// func TestRingBuffer_Range_PanicHandling(t *testing.T) {
//     rb := ringbuffer.NewRingBufferpend(1)
//     rb.Append(2)
//     rb.Append(3)

//     defer func() {
//         if r := recover(); r == nil {
//             t.Errorf("Expected panic, but no panic occurred")
//         }
//     }()

//     // Simulate a panic during iteration
//     rb.Range(func(index int, value int) bool {
//         panic("simulated panic")
//     })
// }
