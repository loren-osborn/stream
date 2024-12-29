package stream_test

import (
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

func TestAssertf(t *testing.T) {
	t.Parallel()

	stream.Assertf(2+2 == 4, "This will not panic")

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, got none")
		} else if r != "Math error: 2 + 2 != 5" {
			t.Errorf("Got panic \"%v\" when \"Math error: 2 + 2 != 5\" expected", r)
		}
	}()

	stream.Assertf(2+2 == 5, "Math error: %d + %d != %d", 2, 2, 5) // This will panic
}
