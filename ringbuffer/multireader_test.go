package ringbuffer_test

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream/ringbuffer"
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

func TestMultiReaderBuf_Resize(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[int](3, 1)

	if ringBuf.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
	}

	ringBuf.Append(1)
	ringBuf.Append(2)
	ringBuf.Append(3)

	if ringBuf.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", ringBuf.Cap())
	}

	ringBuf.Resize(10)

	if ringBuf.Cap() != 10 {
		t.Errorf("Expected capacity 10, got %d", ringBuf.Cap())
	}

	expected := []int{1, 2, 3}

	var result []int

	ringBuf.ReaderRange(0, func(_ int, value int) bool {
		result = append(result, value)

		return true
	})

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestMultiReaderBuf_Range(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[string](0, 1)

	ringBuf.Append("ten")
	ringBuf.Append("twenty")
	ringBuf.Append("thirty")

	var result []string

	mapCopy := ringBuf.ReaderToMap(0)

	itCount := 0

	ringBuf.ReaderRange(0, func(idx int, value string) bool {
		result = append(result, value)
		itCount++

		if mapVal, ok := mapCopy[idx]; !ok || mapVal != value {
			if !ok {
				t.Errorf("Expected map key %v, missing!", idx)
			} else {
				t.Errorf("Expected map[%v] == \"%v\", found \"%v\"", idx, value, mapVal)
			}
		}

		return true
	})

	if mapLen := len(mapCopy); mapLen != ringBuf.ReaderLen(0) {
		t.Errorf("Expected %d elements in mapCopy, found %d.", ringBuf.ReaderLen(0), mapLen)
	}

	if itCount != ringBuf.ReaderLen(0) {
		t.Errorf("Expected %d iterations, saw %d.", ringBuf.ReaderLen(0), itCount)
	}

	expected := []string{"ten", "twenty", "thirty"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

//nolint: funlen // *FIXME*
func TestMultiReaderBuf_Range_FromReaderObj(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[string](0, 3)

	ringBuf.Append("ten")
	ringBuf.Append("twenty")
	ringBuf.Append("thirty")
	ringBuf.Append("forty")
	ringBuf.Append("fifty")
	ringBuf.Append("sixty")

	itCount := 0

	readers := make([]*ringbuffer.Reader[string], 0, 3)

	ringBuf.RangeReaders(func(readerID int, reader *ringbuffer.Reader[string]) bool {
		if reader == nil {
			t.Errorf(
				"RangeReaders called yield function with unexpected nil "+
					"reader on iteration %d",
				itCount,
			)
		} else if objReaderID := reader.ID(); readerID != objReaderID {
			t.Errorf(
				"RangeReaders called yield function with unexpected readerID "+
					"%d on not matching reader.ID() %d",
				readerID,
				objReaderID,
			)
		}

		if readerID != itCount {
			t.Errorf(
				"RangeReaders called yield function with unexpected readerID "+
					"%d on iteration %d",
				readerID,
				itCount,
			)
		}

		if readerFromFunc := ringBuf.GetReader(readerID); readerFromFunc != reader {
			t.Errorf(
				"ringBuf.GetReader() gave reader %v when yield function given "+
					"reader %v!",
				readerFromFunc,
				reader,
			)
		}

		reader.Discard(4 - itCount)

		readers = append(readers, reader)
		itCount++

		return true
	})

	expected := []map[int]string{
		{
			4: "fifty",
			5: "sixty",
		},
		{
			3: "forty",
			4: "fifty",
			5: "sixty",
		},
		{
			2: "thirty",
			3: "forty",
			4: "fifty",
			5: "sixty",
		},
	}

	for rID, reader := range readers {
		var result []string

		mapCopy := reader.ToMap()

		itCount = 0

		reader.Range(func(idx int, value string) bool {
			result = append(result, value)
			itCount++

			if mapVal, ok := mapCopy[idx]; !ok || mapVal != value {
				if !ok {
					t.Errorf("Expected map key %v, missing!", idx)
				} else {
					t.Errorf("Expected map[%v] == \"%v\", found \"%v\"", idx, value, mapVal)
				}
			}

			return true
		})

		if mapLen := len(mapCopy); mapLen != reader.Len() {
			t.Errorf("Expected %d elements in mapCopy, found %d.", reader.Len(), mapLen)
		}

		if itCount != reader.Len() {
			t.Errorf("Expected %d iterations, saw %d.", reader.Len(), itCount)
		}

		if !reflect.DeepEqual(expected[rID], mapCopy) {
			t.Errorf("Expected %v, got %v", expected[rID], mapCopy)
		}
	}
}

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

//nolint: funlen,gocognit // *FIXME*
func TestMultiReaderBuf_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[int](0, 5)
	someFunc := func(in int) int { return in*3 + 7 }

	var waitGrp sync.WaitGroup

	// Writer goroutine
	waitGrp.Add(1)

	go func() {
		defer waitGrp.Done()

		for i := range 1000 {
			for j := range 2 {
				index := i*2 + j
				writeIndex := ringBuf.Append(someFunc(index))

				if writeIndex != index {
					t.Errorf("Unexpected write index %d when %d expected", writeIndex, index)
				}
			}
		}
	}()

	for readerID := range 5 {
		// Reader goroutine
		waitGrp.Add(1)

		go func(rID int) {
			defer waitGrp.Done()

			firstIdx := 0

			for iteration := range 1000 {
				lastIndex := firstIdx - 1

				ringBuf.ReaderRange(rID, func(index int, readVal int) bool {
					if index != lastIndex+1 {
						t.Errorf("Indices out of order: %d followed by %d", lastIndex, index)
					}

					if computed := someFunc(index); readVal != computed {
						t.Errorf("Unanticipated value: %d when %d expected", readVal, computed)
					}

					// Just going modulo a bunch of primes to make this uncommon:
					if (iteration%7 == 0) && ((index+2*rID)%29 == 0) {
						maxToDiscard := 1 + ((index + 3*rID) % 5)
						if ringBuf.ReaderLen(rID) < maxToDiscard {
							maxToDiscard = ringBuf.ReaderLen(rID) / 2
						}

						firstIdx += maxToDiscard
						ringBuf.ReaderDiscard(rID, maxToDiscard)
					}

					lastIndex = index

					return true
				})
			}
		}(readerID)
	}

	waitGrp.Wait()
}

func TestMultiReaderBuf_DiscardInvalidCount(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, got none")
		} else if r != "Attempted to remove 2 elements when only 1 visible" {
			t.Errorf("Got panic \"%v\" when \"Attempted to remove 2 elements when only 1 visible\" expected", r)
		}
	}()

	rb := ringbuffer.NewMultiReaderBuf[int](0, 1)
	rb.Append(1)
	rb.ReaderDiscard(0, 2) // Should panic because only one element exists
}

func TestMultiReaderBuf_ResizeInvalidCapacity(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, got none")
		} else if r != "Attempted to resize to 1 elements (not big enough to hold 2 elements)" {
			t.Errorf("Got panic \"%v\" when \"Attempted to resize to 1 elements (not big enough "+
				"to hold 2 elements)\" expected", r)
		}
	}()

	rb := ringbuffer.NewMultiReaderBuf[int](0, 1)
	rb.Append(1)
	rb.Append(2)
	rb.Resize(1) // Should panic because size is 2
}

func TestMultiReaderBuf_Range_PanicHandling(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[int](1, 1)

	// Simulate a panic during iteration
	ringBuf.ReaderRange(0, func(_ int, _ int) bool {
		panic("empty buffer... no iterations... no panic!")
	})

	ringBuf.Append(2)
	ringBuf.Append(3)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but no panic occurred")
		} else if r != "simulated panic" {
			t.Errorf("Got panic \"%v\" when \"simulated panic\" expected", r)
		}
	}()

	// Simulate a panic during iteration
	ringBuf.ReaderRange(0, func(_ int, _ int) bool {
		panic("simulated panic")
	})
}

func TestMultiReaderBuf_NegInitialSize_PanicHandling(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but no panic occurred")
		} else if r != "capacity must be greater than zero" {
			t.Errorf("Got panic \"%v\" when \"capacity must be greater than zero\" expected", r)
		}
	}()

	ringbuffer.NewMultiReaderBuf[int](-1, 1)
}

func TestMultiReaderBuf_IndexAfter_PanicHandling(t *testing.T) {
	t.Parallel()

	ringBuf := ringbuffer.NewMultiReaderBuf[int](0, 1)

	ringBuf.Append(10)
	ringBuf.Append(20)
	ringBuf.Append(30)
	ringBuf.Append(40)
	ringBuf.Append(50)
	ringBuf.ReaderDiscard(0, 2)

	rangeLen := ringBuf.ReaderRangeLen(0)
	if rangeLen != 5 {
		t.Errorf("Exprected RangeLen() == 5 got %d", rangeLen)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but no panic occurred")
		} else if r != "Attempted to access index 5 after final index 4" {
			t.Errorf("Got panic \"%v\" when \"Attempted to access index 5 after final index 4\" expected", r)
		}
	}()

	// Panic attempting to set discarded element
	_ = ringBuf.ReaderAt(0, rangeLen)
}

//nolint: cyclop // *FIXME*
func TestMultiReaderBuf_ReaderPeekFirst(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)

	val, err := mrb.ReaderPeekFirst(0)
	if err == nil {
		t.Errorf("Expected error when peeking empty reader, got nil")
	} else if !errors.Is(err, io.EOF) {
		t.Errorf("expected error %v, got %v", io.EOF, err)
	}

	if val != nil {
		t.Errorf("Expected nil value with error condition, got %v", *val)
	}

	// Add some values
	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)

	if rLen := mrb.ReaderLen(0); rLen != 3 {
		t.Errorf("Expected Reader 0's length to be 3, got %d", rLen)
	}

	val, err = mrb.ReaderPeekFirst(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if val == nil || *val != 10 {
		t.Errorf("Expected first element 10, got %v", val)
	}

	if rLen := mrb.ReaderLen(0); rLen != 3 {
		t.Errorf("Expected Reader 0's length to be 3, got %d", rLen)
	}

	val, err = mrb.ReaderPeekFirst(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if val == nil || *val != 10 {
		t.Errorf("Expected first element 10, got %v", val)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid reader ID, got none")
		} else if r != "Attempting to use readerID 999 when only 2 readers allocated" {
			t.Errorf("Got panic \"%v\" when \"Attempting to use readerID 999 when only 2 readers allocated\" expected", r)
		}
	}()

	_, _ = mrb.ReaderPeekFirst(999) // Invalid ID
}

//nolint: cyclop // *FIXME*
func TestMultiReaderBuf_ReaderConsumeFirst(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)

	val, err := mrb.ReaderConsumeFirst(0)
	if err == nil {
		t.Errorf("Expected error when peeking empty reader, got nil")
	} else if !errors.Is(err, io.EOF) {
		t.Errorf("expected error %v, got %v", io.EOF, err)
	}

	if val != nil {
		t.Errorf("Expected nil value with error condition, got %v", *val)
	}

	// Add some values
	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)

	if rLen := mrb.ReaderLen(0); rLen != 3 {
		t.Errorf("Expected Reader 0's length to be 3, got %d", rLen)
	}

	val, err = mrb.ReaderConsumeFirst(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if val == nil || *val != 10 {
		t.Errorf("Expected first element 10, got %v", val)
	}

	if rLen := mrb.ReaderLen(0); rLen != 2 {
		t.Errorf("Expected Reader 0's length to be 2, got %d", rLen)
	}

	val, err = mrb.ReaderConsumeFirst(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if val == nil || *val != 20 {
		t.Errorf("Expected first element 20, got %v", val)
	}

	if rLen := mrb.ReaderLen(0); rLen != 1 {
		t.Errorf("Expected Reader 0's length to be 1, got %d", rLen)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid reader ID, got none")
		} else if r != "Attempting to use readerID 2 when only 2 readers allocated" {
			t.Errorf("Got panic \"%v\" when \"Attempting to use readerID 2 when only 2 readers allocated\" expected", r)
		}
	}()

	_, _ = mrb.ReaderConsumeFirst(2) // Invalid ID
}

func TestMultiReaderBuf_ReaderToSlice(t *testing.T) {
	t.Parallel()

	t.Run("EmptyReaderToSlice", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
		sliceEmpty := mrb.ReaderToSlice(0)

		if len(sliceEmpty) != 0 {
			t.Errorf("Expected empty slice for empty reader, got %v", sliceEmpty)
		}
	})

	t.Run("NonEmptyReaderToSlice", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)

		mrb.Append(10)
		mrb.Append(20)
		mrb.Append(30)

		slice := mrb.ReaderToSlice(0)
		if len(slice) != 3 || slice[0] != 10 || slice[1] != 20 || slice[2] != 30 {
			t.Errorf("Expected [10,20,30], got %v", slice)
		}

		// Consume one element
		_, _ = mrb.ReaderConsumeFirst(0)
		sliceAfter := mrb.ReaderToSlice(0)

		if len(sliceAfter) != 2 || sliceAfter[0] != 20 {
			t.Errorf("Expected [20,30] after consume, got %v", sliceAfter)
		}
	})

	t.Run("InvalidReaderIDShouldPanic", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for invalid reader ID, got none")
			} else if r != "Negative readerID (-1) not allowed" {
				t.Errorf("Got panic \"%v\" when \"Negative readerID (-1) not allowed\" expected", r)
			}
		}()
		mrb.ReaderToSlice(-1)
	})
}

func TestMultiReaderBuf_CloseReader(t *testing.T) {
	t.Parallel()

	t.Run("CloseValidReader", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)

		mrb.Append(100)
		mrb.Append(200)

		mrb.CloseReader(0)

		val, err := mrb.ReaderPeekFirst(0)

		if err == nil {
			t.Errorf("Expected error when accessing closed reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected error %v, got %v", io.EOF, err)
		}

		if val != nil {
			t.Errorf("Expected nil val pointer, got pointer to %v", *val)
		}

		val, err = mrb.ReaderPeekFirst(1)
		if err != nil || val == nil || *val != 100 {
			t.Errorf("Expected reader 1 first element 100, got %v (err: %v)", val, err)
		}
	})

	t.Run("CloseNonExistentReader", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for invalid reader ID, got none")
			} else if r != "Attempting to use readerID 999 when only 2 readers allocated" {
				t.Errorf("Got panic \"%v\" when \"Attempting to use readerID 999 when only 2 readers allocated\" expected", r)
			}
		}()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
		mrb.CloseReader(999)
	})

	t.Run("CloseReaderTwice", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](0, 1)
		mrb.CloseReader(0)
		mrb.CloseReader(0)
	})
}

func TestMultiReaderBuf_LenReaders(t *testing.T) {
	t.Parallel()

	// These steps depend on each other, so we won't use t.Run() here.
	mrb := ringbuffer.NewMultiReaderBuf[int](4, 3)
	initialCount := mrb.LenReaders()

	if initialCount != 3 {
		t.Errorf("Expected 3 readers, got %d", initialCount)
	}

	initialRangeCount := mrb.RangeLenReaders()
	if initialRangeCount != 3 {
		t.Errorf("Expected 3 reader slots, got %d", initialRangeCount)
	}

	mrb.CloseReader(1)

	afterCloseCount := mrb.LenReaders()
	if afterCloseCount != 2 {
		t.Errorf("Expected 2 readers after closing one, got %d", afterCloseCount)
	}

	afterRangeCount := mrb.RangeLenReaders()
	if afterRangeCount != 3 {
		t.Errorf("Expected 3 reader slots after closing one, got %d", afterRangeCount)
	}
}

func TestReader_PeekFirst(t *testing.T) {
	t.Parallel()

	t.Run("PeekEmptyReaderShouldError", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		r := mrb.GetReader(0)

		_, err := r.PeekFirst()

		if err == nil {
			t.Errorf("Expected error peeking empty reader, got nil")
		}
	})

	t.Run("PeekNonEmptyReader", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		r := mrb.GetReader(0)

		mrb.Append(10)

		val, err := r.PeekFirst()

		if err != nil || val == nil || *val != 10 {
			t.Errorf("Expected 10, got %v (err: %v)", val, err)
		}
	})
}

func TestReader_ConsumeFirst(t *testing.T) {
	t.Parallel()

	t.Run("ConsumeFromEmptyReaderShouldError", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		r := mrb.GetReader(0)
		_, err := r.ConsumeFirst()

		if err == nil {
			t.Errorf("Expected error consuming empty reader, got nil")
		}
	})

	t.Run("ConsumeSequentially", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		reader := mrb.GetReader(0)

		mrb.Append(5)
		mrb.Append(6)

		val, err := reader.ConsumeFirst()
		if err != nil || val == nil || *val != 5 {
			t.Errorf("Expected 5, got %v (err: %v)", val, err)
		}

		val, err = reader.ConsumeFirst()
		if err != nil || val == nil || *val != 6 {
			t.Errorf("Expected 6, got %v (err: %v)", val, err)
		}

		_, err = reader.ConsumeFirst()
		if err == nil {
			t.Errorf("Expected error consuming empty reader, got nil")
		}
	})
}

func TestReader_At(t *testing.T) {
	t.Parallel()

	// Dependencies and single scenario, no parallel sub-tests here.
	mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
	reader := mrb.GetReader(0)

	mrb.Append(10)
	mrb.Append(11)
	mrb.Append(12)

	if v := reader.At(reader.RangeFirst()); v != 10 {
		t.Errorf("Expected At(RangeFirst()) = 10, got %d", v)
	}

	if v := reader.At(reader.RangeFirst() + 2); v != 12 {
		t.Errorf("Expected At(RangeFirst()+2)=12, got %d", v)
	}

	defer func() {
		if rec := recover(); rec == nil {
			t.Errorf("Expected panic accessing out-of-range element")
		}
	}()
	reader.At(reader.RangeLen()) // out of range
}

func TestReader_ToSlice(t *testing.T) {
	t.Parallel()

	t.Run("EmptyReaderToSlice", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		r := mrb.GetReader(0)
		emptySlice := r.ToSlice()

		if len(emptySlice) != 0 {
			t.Errorf("Expected empty slice, got %v", emptySlice)
		}
	})

	t.Run("NonEmptyReaderToSlice", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)
		reader := mrb.GetReader(0)

		mrb.Append(100)
		mrb.Append(200)
		mrb.Append(300)

		s := reader.ToSlice()
		if len(s) != 3 || s[0] != 100 || s[1] != 200 || s[2] != 300 {
			t.Errorf("Expected [100,200,300], got %v", s)
		}

		_, _ = reader.ConsumeFirst()

		s2 := reader.ToSlice()
		if len(s2) != 2 || s2[0] != 200 {
			t.Errorf("After consume, expected [200,300], got %v", s2)
		}
	})
}

func TestMultiReaderBuf_ErrorConditions(t *testing.T) {
	t.Parallel()

	t.Run("PeekFirstOnClosedReader", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
		mrb.CloseReader(1)

		_, err := mrb.ReaderPeekFirst(1)
		if err == nil {
			t.Errorf("Expected error peeking closed reader")
		}
	})

	t.Run("ConsumeFirstOnClosedReader", func(t *testing.T) {
		t.Parallel()

		mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
		mrb.CloseReader(1)

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for invalid reader ID, got none")
			} else if r != "Can't read from a non-existent reader" {
				t.Errorf("Got panic \"%v\" when \"Can't read from a non-existent reader\" expected", r)
			}
		}()

		_, _ = mrb.ReaderConsumeFirst(1)
	})
}

// Test creating a MultiReaderBuf with zero readers.
// This should panic with a known message.
func TestMultiReaderBuf_NoReadersCreation(t *testing.T) {
	t.Parallel()

	defer func() {
		expected := "must have at least one reader"

		if r := recover(); r == nil {
			t.Errorf("Expected panic creating MultiReaderBuf with 0 readers, got none")
		} else if r != expected {
			t.Errorf("Got panic %q, expected %q", r, expected)
		}
	}()

	_ = ringbuffer.NewMultiReaderBuf[int](4, 0) // Invalid: no readers
}

// setupClosedMultiReaderBuf creates a MultiReaderBuf with a single reader,
// appends some values, and then closes the reader.
func setupClosedMultiReaderBuf(t *testing.T) *ringbuffer.MultiReaderBuf[int] {
	t.Helper() // Mark this function as a test helper

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 1)

	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)
	mrb.CloseReader(0)

	return mrb
}

// After closing the last reader, Cap should not panic or change state.
func TestMultiReaderBuf_CapAfterLastReaderClosed(t *testing.T) {
	t.Parallel()

	mrb := setupClosedMultiReaderBuf(t)

	c := mrb.Cap()
	if c != 0 {
		t.Errorf("Expected capacity to drop to 0, got %d", c)
	}
}

// After closing the last reader, Append should not panic or change state.
func TestMultiReaderBuf_AppendAfterLastReaderClosed(t *testing.T) {
	t.Parallel()

	mrb := setupClosedMultiReaderBuf(t)
	prevCap := mrb.Cap()
	prevReaderCount := mrb.LenReaders()

	idx := mrb.Append(40) // Should have no effect
	if idx >= 0 {
		t.Errorf("Expected illegal negative insertion index, got %d", idx)
	}

	if mrb.LenReaders() != prevReaderCount {
		t.Errorf("Expected no change in LenReaders after Append, got %d", mrb.LenReaders())
	}

	if c := mrb.Cap(); c != 0 {
		t.Errorf("Expected capacity to drop to 0, previously %d, got %d", prevCap, c)
	}
}

// After closing the last reader, Resize should not panic or change state.
func TestMultiReaderBuf_ResizeAfterLastReaderClosed(t *testing.T) {
	t.Parallel()

	mrb := setupClosedMultiReaderBuf(t)
	prevCap := mrb.Cap()
	prevReaderCount := mrb.LenReaders()

	mrb.Resize(8) // Should have no effect

	if mrb.LenReaders() != prevReaderCount {
		t.Errorf("Expected no change in LenReaders after Resize, got %d", mrb.LenReaders())
	}

	if c := mrb.Cap(); c != prevCap {
		t.Errorf("Expected capacity to remain %d, got %d", prevCap, c)
	}
}

func TestMultiReaderBuf_ReaderAt_ClosedReaderID(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
	mrb.Append(10)
	mrb.Append(20)

	// Close reader 0
	mrb.CloseReader(0)

	defer func() {
		reader := recover()
		if reader == nil {
			t.Errorf("Expected panic for accessing closed ReaderID, got none")

			return
		}

		expected := "Can't read from a non-existent reader"
		if reader != expected {
			t.Errorf("Got panic %q, expected %q", reader, expected)
		}
	}()

	_ = mrb.ReaderAt(0, mrb.ReaderRangeFirst(0)) // Access closed ReaderID
}

func TestMultiReaderBuf_ReaderAt_IndexBeforeRangeFirst(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](1, 2)
	mrb.Append(10)
	mrb.Append(20)

	readerID := 0

	mrb.ReaderDiscard(readerID, 1)

	rangeFirst := mrb.ReaderRangeFirst(readerID)

	defer func() {
		reader := recover()
		if reader == nil {
			t.Errorf("Expected panic for index before ReaderRangeFirst, got none")

			return
		}

		expected := "Attempted to access index 0 before initial index 1"
		if reader != expected {
			t.Errorf("Got panic %q, expected %q", reader, expected)
		}
	}()

	_ = mrb.ReaderAt(readerID, rangeFirst-1) // Access index before ReaderRangeFirst
}

func TestMultiReaderBuf_ReaderRange_ClosedReaderID(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)

	// Close ReaderID 0
	mrb.CloseReader(0)

	// Attempt to iterate over the range with the closed reader
	var iterations int

	mrb.ReaderRange(0, func(_ int, _ int) bool {
		iterations++

		return true
	})

	// Verify there were no iterations
	if iterations != 0 {
		t.Errorf("Expected 0 iterations for closed ReaderID, got %d", iterations)
	}
}

func TestMultiReaderBuf_CloseReaderWithHigherRangeFirst(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](4, 2)
	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)
	mrb.Append(40)

	// Reader 0 consumes the first two elements
	_, _ = mrb.ReaderConsumeFirst(0)
	_, _ = mrb.ReaderConsumeFirst(0)

	// Verify RangeFirst for both readers
	rangeFirst0 := mrb.ReaderRangeFirst(0) // Should point to the third element
	rangeFirst1 := mrb.ReaderRangeFirst(1) // Should point to the first element

	if rangeFirst0 <= rangeFirst1 {
		t.Fatalf("Expected RangeFirst for reader 0 (%d) to be greater than reader 1 (%d)", rangeFirst0, rangeFirst1)
	}

	// Close reader 0
	mrb.CloseReader(0)

	// Verify that reader 1 is unaffected
	var reader1Values []int

	mrb.ReaderRange(1, func(_ int, value int) bool {
		reader1Values = append(reader1Values, value)

		return true
	})

	expectedValues := []int{10, 20, 30, 40}
	if len(reader1Values) != len(expectedValues) {
		t.Errorf("Expected reader 1 to see %d elements, got %d", len(expectedValues), len(reader1Values))
	}

	for i, v := range expectedValues {
		if reader1Values[i] != v {
			t.Errorf("Expected value %d at index %d for reader 1, got %d", v, i, reader1Values[i])
		}
	}

	// Verify that the buffer did not discard elements since reader 1 still references them
	if mrb.ReaderRangeFirst(1) != rangeFirst1 {
		t.Errorf("Expected RangeFirst for reader 1 to remain %d, got %d", rangeFirst1, mrb.ReaderRangeFirst(1))
	}
}

//nolint: funlen // *FIXME*
func TestMultiReaderBuf_CloseReaderWithLowerRangeFirst(t *testing.T) {
	t.Parallel()

	mrb := ringbuffer.NewMultiReaderBuf[int](6, 3)
	mrb.Append(10)
	mrb.Append(20)
	mrb.Append(30)
	mrb.Append(40)
	mrb.Append(50)
	mrb.Append(60)

	// Advance readers 1 and 2 beyond the range of reader 0
	for range 4 {
		_, _ = mrb.ReaderConsumeFirst(1)
		_, _ = mrb.ReaderConsumeFirst(2)
	}

	// Verify preconditions
	rangeFirst0 := mrb.ReaderRangeFirst(0) // Should still point to the first element
	rangeFirst1 := mrb.ReaderRangeFirst(1) // Should point to the fifth element
	rangeFirst2 := mrb.ReaderRangeFirst(2) // Should point to the fifth element

	if !(rangeFirst1 > rangeFirst0 && rangeFirst2 > rangeFirst0) {
		t.Fatalf(
			"Expected RangeFirst of readers 1 (%d) and 2 (%d) to be greater than reader 0 (%d)",
			rangeFirst1,
			rangeFirst2,
			rangeFirst0,
		)
	}

	// Close reader 0
	mrb.CloseReader(0)

	// Verify the buffer has discarded elements up to the minimum RangeFirst of the remaining readers
	if discardedStart := mrb.ReaderRangeFirst(1); discardedStart != rangeFirst1 {
		t.Errorf(
			"Expected buffer to discard elements up to RangeFirst of remaining readers (%d), got %d",
			rangeFirst1,
			discardedStart,
		)
	}

	// Verify the remaining readers are unaffected
	var reader1Values, reader2Values []int

	mrb.ReaderRange(1, func(_ int, value int) bool {
		reader1Values = append(reader1Values, value)

		return true
	})
	mrb.ReaderRange(2, func(_ int, value int) bool {
		reader2Values = append(reader2Values, value)

		return true
	})

	expectedValues := []int{50, 60}
	if len(reader1Values) != len(expectedValues) {
		t.Errorf("Expected reader 1 to see %d elements, got %d", len(expectedValues), len(reader1Values))
	}

	for i, v := range expectedValues {
		if reader1Values[i] != v {
			t.Errorf("Expected value %d at index %d for reader 1, got %d", v, i, reader1Values[i])
		}
	}

	if len(reader2Values) != len(expectedValues) {
		t.Errorf("Expected reader 2 to see %d elements, got %d", len(expectedValues), len(reader2Values))
	}

	for i, v := range expectedValues {
		if reader2Values[i] != v {
			t.Errorf("Expected value %d at index %d for reader 2, got %d", v, i, reader2Values[i])
		}
	}
}
