# Copilot Instructions for Go Libraries

Welcome, Copilot! These guidelines ensure we write clean, idiomatic, and maintainable Go code that follows best practices for library development.

## 🧠 Code Style

* Write **idiomatic Go** code: Use `gofmt` for formatting, follow standard naming conventions, and avoid unnecessary abstractions.
* Favor small, focused functions with minimal dependencies.
* Use `context.Context` in all public APIs for proper cancellation and timeout handling.
* Prefer `errors.Is` and `errors.As` for robust error checking and handling.
* Return early to reduce nesting and improve readability.
* Design clean interfaces: Use interfaces for abstraction when they add value, but prefer concrete types when possible to avoid over-engineering.

## 🧪 Testing Guidelines

* Ensure comprehensive test coverage with meaningful, behavioral tests.
* Each test should verify a single behavior:
  - Split tests with multiple "Act" operations into separate tests.
  - Use table-driven tests for multiple inputs with the same behavior.
* Use Go's standard `testing` package supplemented by `testify/assert` for assertions.
* Write tests in behavioral style with descriptive names:

  ```go
  func TestShouldReturnErrorWhenUserIsInvalid(t *testing.T)
  func TestShouldStoreResultGivenValidInput(t *testing.T)
  ```

* Structure tests clearly with comments:

  ```go
  // Arrange
  require.NoError(t, err)
  // ... setup code ...

  // Act
  result, err := someFunction(input)

  // Assert
  assert.NoError(t, err)
  assert.Equal(t, expected, result)
  ```

* Include benchmarks for performance-critical code to measure and optimize.
* Test edge cases, error conditions, and concurrent scenarios where applicable.

## 📚 Documentation

* Provide GoDoc-style comments for all exported functions, methods, and types.
* Start each comment with the name of the item being documented.
* Maintain up-to-date documentation in the `/docs` directory.
* Ensure the main `README.md` offers clear, concise guidance for new developers, including installation, usage examples, and troubleshooting.

## 🩵 Logging

* Use `slog` for structured, context-aware logging.
* Always pass `context.Context` to logging calls for proper tracing and correlation.
* Log at appropriate levels: debug for development, info for normal operations, warn for recoverable issues, and error for failures.

## ⚡ Performance & Concurrency

* Write efficient code: Minimize allocations, use appropriate data structures, and profile performance bottlenecks.
* Handle concurrency safely: Use channels, sync primitives, or atomic operations appropriately. Avoid race conditions.
* Consider streaming and iterators for large datasets to reduce memory usage.

## 🔒 Security & Dependencies

* Keep dependencies minimal and up-to-date; audit for vulnerabilities.
* Avoid exposing sensitive information in logs or errors.
* Follow secure coding practices: Validate inputs, handle errors gracefully, and avoid common pitfalls like SQL injection or path traversal.

Thanks for helping us build high-quality Go libraries!