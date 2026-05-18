# Overview

KV exposes a single **`KV` interface** that abstracts partition/row key storage, batch writes, range queries, and enumeration. Application code targets the interface; backends swap without API changes.

## Architecture

```text
 Application
     │
     ▼
  kv.KV interface  ◄── overlays (graph, merkle, timeseries, search)
     │
     ├── pebble   (embedded)
     ├── redis    (remote)
     └── azure    (Table Storage via azkit)
```

The interface is the **lowest common denominator** of all backends. Portable code must not rely on backend-specific atomicity or ordering beyond what the interface documents.

## Backends

| Backend | Package | Use case |
|---------|---------|----------|
| Pebble | `pkg/storage/pebble` | Local embedded, high throughput |
| Redis | `pkg/storage/redis` | Shared remote cache / KV |
| Azure Tables | `pkg/storage/azure` | Cloud-native, uses `github.com/fgrzl/azkit` |

## Features

- CRUD: `Get`, `Put`, `Insert`, `Remove`
- `Batch` and `BatchChunks` for bulk writes
- Range and prefix queries via `lexkey` primary keys
- Optional value compression (framed, integrity-checked)
- `NewInstrumentedKV` for OpenTelemetry

## Keys

Keys use **`github.com/fgrzl/lexkey`** for lexicographic partition and row encoding. See [lexkey documentation](https://github.com/fgrzl/lexkey/tree/main/docs).

## Stability

Semantic versioning applies to the `KV` interface and public types. Review backend release notes when upgrading storage implementations.
