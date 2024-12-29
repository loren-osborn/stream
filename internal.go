package stream

import (
	"fmt"
)

// assertf panics if the condition is false. It takes a format string and arguments
// to construct the panic message using fmt.Sprintf.
func assertf(condition bool, format string, args ...any) {
	if !condition {
		panic(fmt.Sprintf(format, args...))
	}
}
