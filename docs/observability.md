# Observability

KV ships with OpenTelemetry **tracing** and **metrics** for core operations and overlays.

## Enable instrumentation

```go
store := kv.NewInstrumentedKV(pebbleStore, "pebble")
```

Configure exporters per [OpenTelemetry Go documentation](https://opentelemetry.io/docs/languages/go/).

## Tracing

Spans cover:

- Core: `Get`, `Put`, `Insert`, `Remove`, `Query`, `Batch`
- Overlays: graph BFS, Merkle builds, timeseries range queries
- Backends: backend-specific child spans

Common attributes: `store`, `operation`, `partition_key`, `row_key`, `batch_size`, `result`.

## Metrics

- Operation counters by type and result
- Latency histograms per operation
- Backend-labeled series

## Logging

Routine operations rely on traces/metrics. Logs use `slog` at `debug` for success paths, `warn` for recoverable skips, `error` for failures.
