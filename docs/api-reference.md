# API reference

The `kv.KV` interface is the portable contract. Backends may optimize internally; callers must not rely on behavior beyond what the interface guarantees.

## Core interface

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

## Query operators

| Operator | Description |
|----------|-------------|
| `Scan` | All items in range |
| `Equal` | Exact match |
| `GreaterThan` / `GreaterThanOrEqual` | Lower bound |
| `LessThan` / `LessThanOrEqual` | Upper bound |
| `Between` | Inclusive range between two keys |
| `StartsWith` | Prefix match |

## Data types

```go
type Item struct {
    PK    lexkey.PrimaryKey
    Value []byte
}

type BatchItem struct {
    Op    BatchOp
    PK    lexkey.PrimaryKey
    Value []byte
}
```

## Instrumentation

Wrap any implementation with `NewInstrumentedKV` for OpenTelemetry. See [Observability](observability.md).

## Package layout

| Import | Role |
|--------|------|
| `github.com/fgrzl/kv` | Interface, query types, instrumentation |
| `github.com/fgrzl/kv/pkg/storage/pebble` | Embedded Pebble backend |
| `github.com/fgrzl/kv/pkg/storage/redis` | Redis backend |
| `github.com/fgrzl/kv/pkg/storage/azure` | Azure Tables backend |
| `github.com/fgrzl/kv/pkg/graph` | Graph overlay |
| `github.com/fgrzl/kv/pkg/merkle` | Merkle overlay |
| `github.com/fgrzl/kv/pkg/timeseries` | Timeseries overlay |
| `github.com/fgrzl/kv/pkg/search` | Search helpers |

For benchmarks, development setup, and value compression, see the root [README](../README.md).
