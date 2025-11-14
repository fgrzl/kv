[![ci](https://github.com/fgrzl/kv/actions/workflows/ci.yaml/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/ci.yml)
[![Dependabot Updates](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates)

# KV

A simple and flexible **key-value store abstraction** for Go that provides a unified interface for multiple backend storage systems. This library supports CRUD operations, batch writes, range queries, and efficient enumeration across different storage backends.

The KV interface allows you to seamlessly switch between storage backends including **Azure Tables**, **Pebble DB**, **Redis**, and **Merkle Trees** without changing your application code.

---

## 🏗️ **Architecture**

The library follows an interface-based design pattern with a core `KV` interface that abstracts common key-value operations. Each backend implementation provides specific optimizations while maintaining the same consistent API.

**Supported Backends:**
- **Azure Tables** - Cloud-native NoSQL storage
- **Pebble** - High-performance embedded key-value store (RocksDB successor)
- **Redis** - In-memory data structure store
- **Merkle Trees** - Cryptographically verifiable data structures

---

## 🚀 **Features**

- 🔑 **CRUD Operations** - `Get`, `Put`, `Insert`, `Remove` with full type safety
- ⚡ **Batch Operations** - Efficient bulk writes with deduplication support
- 🔍 **Advanced Queries** - Range queries, prefix searches, and custom operators
- 📊 **Query Operators** - `Equal`, `GreaterThan`, `Between`, `StartsWith`, and more
- 🔄 **Enumeration** - Memory-efficient iteration over large datasets
- 🎯 **Pluggable Backends** - Easy switching between storage systems
- 🧪 **Test-Friendly** - Built-in test utilities and Docker setup
- 📈 **Performance** - Optimized for high-throughput scenarios
- 📊 **Observability** - Built-in OpenTelemetry tracing and metrics for monitoring

---

## ⚡ **Performance**

The library is designed with performance in mind, ensuring that overlay abstractions (Graph, Merkle, Timeseries) add minimal overhead compared to direct KV operations.

**Benchmark Results (on Intel i9-12900HK):**

| Operation | Time | Notes |
|-----------|------|-------|
| KV Put | ~323µs | Base operation |
| KV Get | ~494ns | Fast retrieval |
| KV Batch | ~329µs | Efficient bulk writes |
| Graph AddNode | ~328µs | Optimized with pre-allocation |
| Graph BFS | ~19.8µs | Optimized traversal with pre-allocated data structures |
| Merkle Build (100 leaves) | ~663µs | Optimized with pre-allocation and efficient batching |
| Merkle Build (1000 leaves) | ~773µs | Scales well for larger trees |
| Timeseries Append | ~339µs | Slight overhead acceptable |
| Timeseries QueryRange | ~102µs | Fast range queries |

Overlays maintain high performance while providing rich functionality, with no unnecessary abstraction penalties.

**Graph Optimizations:**
- Pre-allocated slices and maps for BFS traversal
- Optimized batch operations with capacity hints
- Memory-efficient data structures for large graphs

**Merkle Tree Optimizations:**
- Pre-allocated slices for reduced memory allocations
- Efficient batching for storage operations
- Optimized hash computation with SHA256 reuse
- Memory-efficient processing for large trees

---

## 📊 **Observability**

The KV library includes comprehensive **OpenTelemetry instrumentation** for tracing and metrics collection. All core operations and overlay abstractions are automatically instrumented.

### Tracing

- **Core KV Operations**: All `Get`, `Put`, `Insert`, `Remove`, `Query`, and `Batch` operations are traced
- **Overlay Operations**: Graph BFS traversals, Merkle tree builds, and Timeseries queries are traced
- **Storage Backends**: Each backend (Pebble, Redis, Azure) includes operation-specific spans

### Metrics

- **Operation Counters**: Total operations by type and result (success/error)
- **Operation Duration**: Histograms measuring operation latency
- **Backend-specific Metrics**: Store type and operation attributes

### Usage

```go
import (
    "github.com/fgrzl/kv"
    "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/exporters/jaeger"
)

// Initialize OpenTelemetry (example with Jaeger)
exp, _ := jaeger.New(jaeger.WithCollectorEndpoint())
tp := trace.NewTracerProvider(trace.WithBatcher(exp))
otel.SetTracerProvider(tp)

// Use instrumented KV store
store := kv.NewInstrumentedKV(pebbleStore, "pebble")
```

### Span Attributes

- `store`: Backend type (pebble, redis, azure)
- `operation`: Operation type (get, put, insert, etc.)
- `partition_key`: Hex-encoded partition key
- `row_key`: Hex-encoded row key (when applicable)
- `batch_size`: Number of items in batch operations
- `result`: Operation result (success, error, hit, miss)

---

## 📦 **Installation**

```bash
go get github.com/fgrzl/kv
```

**Requirements:**
- Go 1.24.0 or later
- Docker (for running tests with backend services)

---

## ⚡ **Dead Simple Setup**

For the quickest possible setup with sensible defaults, use the `quickstart` package:

```go
package main

import (
    "context"
    "fmt"

    "github.com/fgrzl/kv/pkg/quickstart"
    kvstore "github.com/fgrzl/kv"
    "github.com/fgrzl/lexkey"
)

func main() {
    // Create a KV store with Pebble backend (temporary database)
    kv, err := quickstart.NewPebbleKV("")
    if err != nil {
        panic(err)
    }
    defer kv.Close()

    // Create a graph database
    graph, err := quickstart.NewGraph("myapp", "")
    if err != nil {
        panic(err)
    }

    // Create a time series database
    ts, err := quickstart.NewTimeSeries("metrics", "")
    if err != nil {
        panic(err)
    }

    // Create a Merkle tree
    tree, err := quickstart.NewMerkleTree("")
    if err != nil {
        panic(err)
    }

    // Ready to use! All backends are pre-configured with optimized settings.

    // Example: Use the KV store
    pk := lexkey.NewPrimaryKey(lexkey.Encode("users"), lexkey.Encode("john"))
    err = kv.Put(context.Background(), &kvstore.Item{PK: pk, Value: []byte("John Doe")})
    // ... use kv, graph, ts, tree as needed
}
```

**Available Quick Start Functions:**
- `NewPebbleKV(path)` - Embedded key-value store (empty path = temp DB)
- `NewRedisKV(addr)` - Redis-backed store (empty addr = localhost:6379)  
- `NewAzureKV(table)` - Azure Tables store (uses env vars for auth)
- `NewGraph(name, pebblePath)` - Complete graph setup
- `NewTimeSeries(name, pebblePath)` - Complete time series setup
- `NewMerkleTree(pebblePath)` - Complete Merkle tree setup

All functions include OpenTelemetry instrumentation by default.

---

## 🚀 **Quick Start**

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/fgrzl/kv"
    "github.com/fgrzl/kv/pebble"
    "github.com/fgrzl/lexkey"
)

func main() {
    // Create a Pebble store
    store, err := pebble.NewPebbleStore("./data", pebble.WithTableCacheShards(1))
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Create a primary key
    pk := lexkey.NewPrimaryKey(
        lexkey.Encode("users"),    // partition key
        lexkey.Encode("john123"),  // row key
    )

    // Put an item
    item := &kv.Item{
        PK:    pk,
        Value: []byte(`{"name": "John Doe", "email": "john@example.com"}`),
    }
    
    err = store.Put(context.Background(), item)
    if err != nil {
        log.Fatal(err)
    }

    // Get the item back
    retrieved, err := store.Get(context.Background(), pk)
    if err != nil {
        log.Fatal(err)
    }
    
    if retrieved != nil {
        fmt.Printf("Retrieved: %s\n", string(retrieved.Value))
    }
}
```

### Backend-Specific Examples

#### Azure Tables

```go
import (
    "github.com/fgrzl/kv/azure"
)

credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
if err != nil {
    log.Fatal(err)
}

store, err := azure.NewAzureStore(
    azure.WithTable("mytable"),
    azure.WithEndpoint("https://myaccount.table.core.windows.net/"),
    azure.WithSharedKey(credential),
)
```

#### Redis

```go
import (
    "github.com/fgrzl/kv/redis"
)

store, err := redis.NewRedisStore(
    redis.WithAddress("localhost:6379"),
    redis.WithDatabase(0),
    redis.WithPrefix("myapp:"),
)
```

#### Pebble (Embedded)

```go
import (
    "github.com/fgrzl/kv/pebble"
)

store, err := pebble.NewPebbleStore(
    "./mydb.pebble",
    pebble.WithTableCacheShards(4),
)
```

### Advanced Operations

#### Batch Operations

```go
batch := []*kv.BatchItem{
    {Op: kv.Put, PK: pk1, Value: []byte("value1")},
    {Op: kv.Put, PK: pk2, Value: []byte("value2")},
    {Op: kv.Delete, PK: pk3},
}

err := store.Batch(context.Background(), batch)
```

#### Range Queries

```go
queryArgs := kv.QueryArgs{
    PartitionKey: lexkey.Encode("users"),
    StartRowKey:  lexkey.Encode("a"),
    EndRowKey:    lexkey.Encode("m"),
    Operator:     kv.Between,
    Limit:        100,
}

items, err := store.Query(context.Background(), queryArgs, kv.Ascending)
```

#### Enumeration

```go
enumerator := store.Enumerate(context.Background(), queryArgs)
defer enumerator.Close()

for enumerator.Next() {
    item := enumerator.Current()
    // Process item
    fmt.Printf("Key: %s, Value: %s\n", 
        string(item.PK.RowKey), string(item.Value))
}

if enumerator.Error() != nil {
    log.Fatal(enumerator.Error())
}
```

---

## 🛠️ **Development Setup**

### Prerequisites

1. **Install Go 1.24.0+**
   ```bash
   go version  # Should show 1.24.0 or later
   ```

2. **Install Docker** (for running test infrastructure)
   ```bash
   docker --version
   docker compose --version
   ```

### Clone and Setup

```bash
# Clone the repository
git clone https://github.com/fgrzl/kv.git
cd kv

# Download dependencies
go mod download

# Build the project
go build ./...
```

### Running Tests

The test suite requires backend services to be running. We provide a Docker Compose setup for this:

```bash
# Start test infrastructure (Azure Storage Emulator + Redis)
docker compose -f test/compose.yml up -d

# Run all tests
go test ./... -v

# Run tests with coverage
go test ./... -v -coverprofile=coverage.out

# View coverage report
go tool cover -html=coverage.out

# Stop test infrastructure
docker compose -f test/compose.yml down
```

### Test Infrastructure

The `test/compose.yml` file sets up:
- **Azurite** (Azure Storage Emulator) on port 10002
- **Redis** on port 6379

### Running Specific Backend Tests

```bash
# Test only Pebble backend
go test ./pebble -v

# Test only Redis backend  
go test ./redis -v

# Test only Azure backend
go test ./azure -v
```

---

## 📖 **API Reference**

### Core Interface

The `kv.KV` interface provides the following methods:

```go
type KV interface {
    Get(ctx context.Context, pk lexkey.PrimaryKey) (*Item, error)
    GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]*Item, error)
    Insert(ctx context.Context, item *Item) error
    Put(ctx context.Context, item *Item) error
    Remove(ctx context.Context, pk lexkey.PrimaryKey) error
    RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error
    RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error
    Query(ctx context.Context, queryArgs QueryArgs, sort SortDirection) ([]*Item, error)
    Enumerate(ctx context.Context, queryArgs QueryArgs) enumerators.Enumerator[*Item]
    Batch(ctx context.Context, items []*BatchItem) error
    BatchChunks(ctx context.Context, items enumerators.Enumerator[*BatchItem], chunkSize int) error
    Close() error
}
```

### Query Operators

- `Scan` - Retrieve all items in range
- `Equal` - Exact match
- `GreaterThan` / `GreaterThanOrEqual` - Range queries
- `LessThan` / `LessThanOrEqual` - Range queries  
- `Between` - Range between two keys
- `StartsWith` - Prefix matching

### Data Types

```go
type Item struct {
    PK    lexkey.PrimaryKey  // Composite key (partition + row)
    Value []byte             // Stored value
}

type BatchItem struct {
    Op    BatchOp            // Put or Delete
    PK    lexkey.PrimaryKey  
    Value []byte
}
```

---

## 🤝 **Contributing**

We welcome contributions! Please follow these guidelines:

### Development Workflow

1. **Fork and clone** the repository
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** with appropriate tests
4. **Run the test suite**: `go test ./...`
5. **Run the linter**: `go vet ./...`
6. **Commit changes**: `git commit -m 'Add amazing feature'`
7. **Push to branch**: `git push origin feature/amazing-feature`
8. **Open a Pull Request**

### Code Standards

- Follow standard Go conventions (`go fmt`, `go vet`)
- Add tests for new functionality
- Update documentation for API changes
- Ensure all backends are supported for new features
- Maintain backward compatibility

### Testing Requirements

- All tests must pass across all supported backends
- Add integration tests for new features
- Include benchmarks for performance-critical changes
- Test with the provided Docker infrastructure

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🔗 **Related Projects**

- [lexkey](https://github.com/fgrzl/lexkey) - Lexicographic key encoding library
- [enumerators](https://github.com/fgrzl/enumerators) - Generic enumeration utilities

---

## 📞 **Support**

- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/fgrzl/kv/issues)
- 💡 **Feature Requests**: [GitHub Discussions](https://github.com/fgrzl/kv/discussions)
- 📖 **Documentation**: This README and inline code documentation
