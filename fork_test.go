package stream_test

import (
	"context"
	"io"
	"testing"

	//nolint:depguard // package under test.
	"github.com/loren-osborn/stream"
)

func ptr[T any](v T) *T {
	return &v
}

func assertPointedToVal[T comparable](t *testing.T, want *T, got *T) {
	t.Helper() // Mark as helper to improve error trace readability

	switch {
	case want == nil && got == nil:
		// Both are nil, valid case
	case want == nil:
		t.Errorf("expected nil, but got %v", got)
	case got == nil:
		t.Errorf("expected %v, but got nil", *want)
	default:
		if *want != *got {
			t.Errorf("expected %v, but got %v", *want, *got)
		}
	}
}

// MockSourceCall defines the expected behavior for a single call to the mock source.
type MockSourceCall[T any] struct {
	ctxGen func() context.Context // Expected Context for this call
	RetVal *T                     // Value to return (nil if no value)
	RetErr error                  // Error to return
}

// mockSource creates a mock Source that validates calls against a sequence of expected behaviors.
func mockSource[T any](t *testing.T, expectedCall **MockSourceCall[T]) stream.Source[T] {
	t.Helper()

	return stream.SourceFunc[T](func(ctx context.Context) (*T, error) {
		if *expectedCall == nil {
			t.Errorf("unexpected call to mockSource when expectedCall nil")

			return nil, io.EOF
		}

		expected := **expectedCall
		*expectedCall = nil

		expectedCtx := expected.ctxGen()

		// Validate context
		if ctx != expectedCtx {
			t.Errorf("expected context %v, got %v", expectedCtx, ctx)
		}

		// Return the configured value and error
		return expected.RetVal, expected.RetErr
	}, nil)
}

type PullCallType[T any] struct {
	PreStuffCalls  [][]T                  // Values to Stuff() before this Pull()
	ctxGen         func() context.Context // Context passed to Pull()
	ExpectedCalls  *MockSourceCall[T]     // Expected behavior for proxied Pull() call
	ExpectedRetVal *T                     // Expected return value of Pull() call
	ExpectedRetErr error                  // Expected returned error of Pull() call
}

type SpoolerTestCase[T any] struct {
	Name      string            // Descriptive name of the test case
	PullCalls []PullCallType[T] // Each call to Spooler.Pull()
}

//nolint: funlen // dataset only
func getTestCasesSpoolerNormalOps() []SpoolerTestCase[int] {
	return []SpoolerTestCase[int]{
		{
			Name: "Basic Pulling",
			PullCalls: []PullCallType[int]{
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  &MockSourceCall[int]{ctxGen: context.Background, RetVal: ptr(1), RetErr: nil},
					ExpectedRetVal: ptr(1),
					ExpectedRetErr: nil,
				},
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  &MockSourceCall[int]{ctxGen: context.Background, RetVal: ptr(2), RetErr: nil},
					ExpectedRetVal: ptr(2),
					ExpectedRetErr: nil,
				},
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  &MockSourceCall[int]{ctxGen: context.Background, RetVal: nil, RetErr: io.EOF},
					ExpectedRetVal: nil,
					ExpectedRetErr: io.EOF,
				},
			},
		},
		{
			Name: "Basic Stuffing",
			PullCalls: []PullCallType[int]{
				{
					PreStuffCalls:  [][]int{{1}, {2}},
					ctxGen:         context.Background,
					ExpectedCalls:  nil,
					ExpectedRetVal: ptr(1),
					ExpectedRetErr: nil,
				},
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  nil,
					ExpectedRetVal: ptr(2),
					ExpectedRetErr: nil,
				},
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  &MockSourceCall[int]{ctxGen: context.Background, RetVal: nil, RetErr: io.EOF},
					ExpectedRetVal: nil,
					ExpectedRetErr: io.EOF,
				},
			},
		},
		{
			Name: "Read closed",
			PullCalls: []PullCallType[int]{
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  &MockSourceCall[int]{ctxGen: context.Background, RetVal: nil, RetErr: io.EOF},
					ExpectedRetVal: nil,
					ExpectedRetErr: io.EOF,
				},
				{
					PreStuffCalls:  [][]int{},
					ctxGen:         context.Background,
					ExpectedCalls:  nil,
					ExpectedRetVal: nil,
					ExpectedRetErr: io.EOF,
				},
			},
		},
	}
}

func TestSpoolerNormalOps(t *testing.T) {
	t.Parallel()

	testCases := getTestCasesSpoolerNormalOps()

	runSpoolerTests(t, testCases)
}

func runSpoolerTests[T comparable](t *testing.T, testCases []SpoolerTestCase[T]) {
	t.Helper()

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			// Set up the mock source
			var currentCall *MockSourceCall[T]
			source := mockSource(t, &currentCall)
			spooler := stream.NewSpooler(source)

			// Iterate over PullCalls
			for index, pullCall := range testCase.PullCalls {
				// Perform pre-`Stuff()` calls
				for _, stuffValues := range pullCall.PreStuffCalls {
					spooler.Stuff(stuffValues)
				}

				ctx := pullCall.ctxGen()

				// Set the expected proxied Pull() call
				currentCall = pullCall.ExpectedCalls

				// Perform Pull()
				value, err := spooler.Pull(ctx)

				// Validate proxied Pull() behavior
				if currentCall != nil {
					t.Errorf(
						"iteration %d: expected Pull() call to be proxied to source call, but wasn't: Expected %+v",
						index,
						currentCall,
					)
				}

				currentCall = nil

				// Validate returned value
				assertPointedToVal(t, pullCall.ExpectedRetVal, value)

				// Validate returned error
				assertError(t, pullCall.ExpectedRetErr, err)
			}
		})
	}
}
