# Copilot Instructions for KV Library

Welcome, Copilot! The KV library is a **pluggable key-value store abstraction** supporting multiple backends (Pebble, Redis, Azure Tables, Merkle Trees) with overlay abstractions for Graph, Timeseries, and Search operations. These guidelines ensure clean, performant, and maintainable code.

## 🏗️ Architecture Overview

**Core KV Interface** (`kv.go`): Defines CRUD operations (`Get`, `Put`, `Insert`, `Remove`, `Query`, `Batch`) and uses `lexkey.LexKey` for hierarchical partitioning. All operations are context-aware for cancellation/timeouts.

**Pluggable Backends** (`pkg/storage/`):
- **Pebble** (`pebble/store.go`): Embedded RocksDB-like store; default for local development
- **Redis** (`redis/store.go`): In-memory with TTL support
- **Azure Tables** (`azure/store.go`): Cloud-native NoSQL

**Overlay Abstractions** (`pkg/`):
- **Graph** (`graph/`): Directed graph with BFS/DFS traversal; stores nodes and edges separately with forward/inverse indices for bidirectional queries
- **Merkle** (`merkle/`): Persistent Merkle trees for entity snapshots; stores tree nodes in KV with hash verification
- **Timeseries** (`timeseries/`): Time-bucketed data for range queries
- **Index** (`index/`): Inverted indices for full-text search
- **Search** (`search/`): Combined index + query logic

**Instrumentation** (`instrumentation.go`): Every operation is wrapped with OpenTelemetry tracing and metrics (operation count, duration histograms) using `go.opentelemetry.io/otel`.

## 🧠 Code Style

* Write **idiomatic Go**; use `gofmt` for formatting
* Use `context.Context` in all public APIs—pass it through to backend calls
* Prefer `errors.Is` and `errors.As` for error checking
* Return early to reduce nesting
* Favor small functions; avoid over-engineering with interfaces

## 🔑 Key Patterns & Conventions

**LexKey Hierarchical Partitioning**: Use `lexkey.Encode()` for multi-level key organization. Example from `pkg/graph/graph.go`:
```go
lexkey.Encode("graph", g.name, "node")        // Graph nodes partition
lexkey.Encode("graph", g.name, "edge", from)  // Forward edges keyed by source
lexkey.Encode("graph", g.name, "inedge", to)  // Inverse edges for bidirectional queries
```
This creates sortable, queryable namespaces within the KV store.

**Backend-Agnostic Abstractions**: Overlay implementations (Graph, Merkle, Timeseries) work with any backend via the KV interface. Test with Pebble locally; implementations should not assume backend specifics.

**Batch Deduplication**: Use `kv.BatchItem` with `kv.Put` and `kv.Delete` operations. Overlays deduplicate batch items to avoid redundant writes.

**Enumerators for Large Result Sets**: Use `github.com/fgrzl/enumerators` (lazy iteration) instead of loading all results into memory. Example: Merkle tree leaf enumeration iterates over KV results without full materialization.

**Instrumentation Wrapping**: Overlay operations are instrumented via `InstrumentedKV` wrapper. Document which operations are traced (e.g., Graph BFS is traced with node count attributes).

## 🧪 Testing Guidelines

* **Arrange-Act-Assert structure** with clear comments:
  ```go
  func TestShouldAddNodeSuccessfully(t *testing.T) {
      // Arrange
      g := setupGraph(t)
      ctx := context.Background()
      // Act
      err := g.AddNode(ctx, "a", []byte("A-meta"))
      // Assert
      assert.NoError(t, err)
  }
  ```
* **Single behavior per test**: If testing multiple branches, create separate tests (e.g., `TestShouldReturnErrorWhenAddingDuplicateNode` vs. `TestShouldAddNodeSuccessfully`)
* **Setup helpers**: Use `setup*()` functions to initialize backends (e.g., `setupGraph()` creates temp Pebble DB and cleans up)
* **Use `testify/assert` and `testify/require`**: `require.*` for setup failures, `assert.*` for behavioral assertions
* **Include benchmarks** for performance-sensitive operations (e.g., `graph_bench_test.go`, `merkle_bench_test.go`)
* **Test enumerator results**: Verify enumeration logic thoroughly since lazy evaluation can hide bugs

## 📚 Documentation

* Provide GoDoc-style comments for all exported functions, methods, and types.
* Start each comment with the name of the item being documented.
* Document overlay abstractions thoroughly: explain partitioning strategy and data layout (see `Graph` implementation for reference)
* Link between related abstractions (e.g., Graph uses KV, which may be Pebble/Redis/Azure)

## 🩵 Logging

* Use `slog` for structured, context-aware logging.
* Always pass `context.Context` to logging calls for proper tracing and correlation.
* Log at appropriate levels: debug for development, info for normal operations, warn for recoverable issues, and error for failures.

## ⚡ Performance & Concurrency

* Write efficient code: Minimize allocations, use appropriate data structures, and profile performance bottlenecks.
* **Pre-allocation for known sizes**: Overlay abstractions pre-allocate slices/maps (Graph BFS, Merkle tree building) to reduce GC pressure.
* Handle concurrency safely: Use channels, sync primitives, or atomic operations appropriately. Avoid race conditions.
* **Streaming over materialization**: Use enumerators for large result sets; don't load everything into memory (especially Merkle leaves, graph neighbors).
* Backends use write options for batch efficiency (see Pebble `NewBatch`, Redis pipelines).

## 🔒 Security & Dependencies

* Keep dependencies minimal and up-to-date; audit for vulnerabilities.
* Avoid exposing sensitive information in logs or errors.
* Follow secure coding practices: Validate inputs, handle errors gracefully, and avoid common pitfalls.
* External packages: `lexkey` (key encoding), `enumerators` (lazy iteration), OpenTelemetry (tracing/metrics), `pebble`/`redis`/`azure-sdk` (backends)

## 🛠️ Developer Workflows

**Running Tests**:
```sh
go test ./...                    # All tests
go test ./pkg/graph/... -v       # Specific package
go test -bench=. ./pkg/graph     # Benchmarks
```

**Local Development with Pebble** (default):
- Tests use temp directories (`./test-pebble-db`), cleaned up automatically
- No external services required; suitable for CI and local iteration

**Docker Setup** (see `compose.yml` for Redis/Azure):
```sh
docker-compose up -d             # Start dependencies
go test ./...                    # Run full test suite
docker-compose down
```

**Code Generation**: No code generation required; interfaces and overlays are hand-written.

Thanks for helping us build high-quality Go libraries!