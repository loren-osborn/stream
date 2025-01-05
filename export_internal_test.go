package stream

import "fmt"

func Assertf(condition bool, format string, args ...any) {
	assertf(condition, format, args...)
}

func (mohs MultiOutputHelperState) String() string {
	switch mohs {
	case MOHelperUninitialized:
		return "MOHelperUninitialized"
	case MOHelperWaiting:
		return "MOHelperWaiting"
	case MOHelperClosed:
		return "MOHelperClosed"
	default:
		return fmt.Sprintf("Undefined state %d", int(mohs))
	}
}
