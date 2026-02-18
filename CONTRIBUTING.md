# Contributing

Contributions are welcome. This document covers workflow, standards, and how to run tests.

## Development Workflow

1. Fork and clone the repository.
2. Create a feature branch: `git checkout -b feature/your-feature`.
3. Make your changes and add or update tests as needed.
4. Run the test suite and linter (see below).
5. Commit with a clear message, push your branch, and open a Pull Request.

## Code Standards

- Follow standard Go conventions: `go fmt`, `go vet`.
- Add tests for new functionality and update documentation for API changes.
- For features that touch the KV interface or storage, ensure all supported backends are considered and maintain backward compatibility.

For detailed code style, patterns, and testing guidelines (e.g. Arrange-Act-Assert, use of `lexkey`, enumerators), see [.github/copilot-instructions.md](.github/copilot-instructions.md). Those guidelines reflect how the maintainers expect the codebase to be written.

## Running Tests

**Prerequisites:** Go 1.25+ and Docker (for backend services).

**Pebble-only (no Docker):**

```bash
go test ./pkg/storage/pebble -v
```

**Full test suite (fazure + Redis):**

```bash
docker compose up -d
go test ./... -v
docker compose down
```

**Coverage:**

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

**Specific packages:**

```bash
go test ./pkg/storage/pebble -v
go test ./pkg/storage/redis -v
go test ./pkg/storage/azure -v
go test ./pkg/graph/... -v
```

**Benchmarks:**

```bash
go test -bench=. ./pkg/graph
go test -bench=. ./pkg/merkle
```

## Testing Requirements

- All tests must pass. For changes that affect multiple backends, run the full suite with `docker compose` (see [README](README.md#development-setup)).
- Add integration tests for new features where appropriate.
- Include benchmarks for performance-critical changes.

## Reporting Issues and Ideas

- **Bugs:** [GitHub Issues](https://github.com/fgrzl/kv/issues).
- **Feature requests or questions:** [GitHub Discussions](https://github.com/fgrzl/kv/discussions).

Please search existing issues and discussions before opening a new one.
