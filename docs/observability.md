# Observability

KV ships with OpenTelemetry **tracing** and **metrics** for core operations and overlays.

## Enable instrumentation

```go
store := kv.NewInstrumentedKV(pebbleStore, "pebble")
```

Configure exporters per [OpenTelemetry Go documentation](https://opentelemetry.io/docs/languages/go/).

## Tracing

Spans cover (via `NewInstrumentedKV` and overlay packages):

- Core: `Get`, `Put`, `Insert`, `Remove`, `Query`, `Batch`, `GetBatch`, etc.
- Overlays: graph BFS, Merkle builds, timeseries range queries

Raw Pebble/Redis/Azure store implementations do not emit their own spans unless you wrap them with `NewInstrumentedKV`.

Common attributes: `store`, `operation`, `partition_key`, `row_key`, `batch_size`, `result`.

## Metrics

- Operation counters by type and result
- Latency histograms per operation
- Backend-labeled series

## Logging

Routine operations rely on traces/metrics. Logs use `slog` at `debug` for success paths, `warn` for recoverable skips, `error` for failures.
