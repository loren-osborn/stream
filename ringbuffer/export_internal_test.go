package ringbuffer

func Assertf(condition bool, format string, args ...any) {
	assertf(condition, format, args...)
}
