# stream

**Work in Progress**

The `stream` package provides tools for lazy evaluation and data transformation pipelines in Go, aiming to offer a more idiomatic solution to operations commonly associated with functional programming—such as `map`, `filter`, and `reduce`—without modifying Go's core language.

This project leverages generics and interfaces to model flexible, type-safe streams and transformations. It’s designed to handle large or potentially unbounded data sets efficiently, while enabling clear, composable code.

## Features

- **Sources:** Define data sources that emit elements one at a time. For example, a `SliceSource` wraps a slice to produce items on demand.
- **Transformations:** Apply functions to each element of a stream, such as:
  - `Map`: Transform elements from one type into another.
  - `Filter`: Exclude elements that don’t match a predicate.
  - `Reduce`: Condense an entire source into a single accumulated value.
  - `ReduceTransformer`: Incrementally reduces a source while emitting intermediate results.
- **Multi-Reader Concurrency:** A key piece of the design is a multi-reader ring buffer intended to support a future “forker” concept, allowing multiple readers to pull data from the same source independently. Unlike a naive approach that copies data for each reader, the ring buffer enables sharing a single copy of data efficiently among multiple readers.

## Intended Usage

The `stream` library is meant to offer a Go-native “streaming solution”:
- Where developers previously requested higher-order functions like `map`, `filter`, and `reduce` directly, this library provides a more idiomatic alternative.
- With streaming abstractions, you can build pipelines that transform and consume data incrementally, rather than loading everything into memory at once.
- It’s designed to integrate naturally with Go’s concurrency patterns and potentially feels at home as an addition to the Go standard library (e.g., `stream` and `container/ringbuffer`), providing a typesafe and more feature-rich alternative to `container/ring`.

## Examples

**Mapping and Filtering:**

```go
var result []string

data := []int{1, 2, 3, 4, 5}
source := NewSliceSource(data)

squareMapper := NewMapper(source, func(n int) int { return n * n })     // Square each number
filter := NewFilter(squareMapper, func(n int) bool { return n%2 == 0 }) // Keep even numbers
toStrMapper := NewMapper(filter, strconv.Itoa)                          // Convert to string
sink := NewSliceSink(&result)

outResult, err := sink.Append(context.Background(), toStrMapper)

 if err != nil {
     return // Fail
 }

fmt.Println(strings.Join(*outResult, ", ")) // Output: 4, 16
```

**Reducing:**

```go
data := []int{1, 2, 3, 4, 5}
source := NewSliceSource(data)
reducer := NewReducer(0, func(acc, next int) int { return acc + next })
result, _ := reducer.Reduce(source)
fmt.Println(result) // Output: 15
```

## Status and Goals

This project is still under active development:
- The ring buffer and related abstractions are mostly complete, but the forker concept (multi-reader sharing from the same source) is not implemented yet.
- Further testing, refinement, and documentation are ongoing.
- Feedback is welcome! The ultimate vision is a stable, idiomatic streaming library that could theoretically fit into the Go standard library ecosystem.

## Contributing

Contributions, suggestions, and feedback are warmly encouraged. If you have thoughts on design clarity, naming, or general direction, feel free to open an issue.

