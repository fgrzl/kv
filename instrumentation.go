package kv

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
)

var (
	tracer = otel.Tracer("github.com/fgrzl/kv")
	meter  = otel.Meter("github.com/fgrzl/kv")

	// Metrics
	operationCount    metric.Int64Counter
	operationDuration metric.Float64Histogram
)

func init() {
	var err error

	operationCount, err = meter.Int64Counter(
		"kv_operations_total",
		metric.WithDescription("Total number of KV operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		slog.Error("failed to create operation counter", "err", err)
	}

	operationDuration, err = meter.Float64Histogram(
		"kv_operation_duration_seconds",
		metric.WithDescription("Duration of KV operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		slog.Error("failed to create operation duration histogram", "err", err)
	}
}

// InstrumentedKV wraps a KV store with OpenTelemetry instrumentation.
type InstrumentedKV struct {
	kv    KV
	store string // e.g., "pebble", "redis", "azure"
}

// NewInstrumentedKV creates a new instrumented KV store.
func NewInstrumentedKV(kv KV, store string) KV {
	return &InstrumentedKV{kv: kv, store: store}
}

func (i *InstrumentedKV) Get(ctx context.Context, pk lexkey.PrimaryKey) (*Item, error) {
	ctx, span := tracer.Start(ctx, "kv.Get",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "get"),
			attribute.String("partition_key", pk.PartitionKey.ToHexString()),
			attribute.String("row_key", pk.RowKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "get"),
			))
	}()

	item, err := i.kv.Get(ctx, pk)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "get"),
				attribute.String("result", "error"),
			))
		return nil, err
	}

	found := "miss"
	if item != nil {
		found = "hit"
	}
	span.SetAttributes(attribute.String("result", found))
	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "get"),
			attribute.String("result", found),
		))

	return item, nil
}

func (i *InstrumentedKV) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]BatchGetResult, error) {
	ctx, span := tracer.Start(ctx, "kv.GetBatch",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "get_batch"),
			attribute.Int("batch_size", len(keys)),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "get_batch"),
			))
	}()

	results, err := i.kv.GetBatch(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "get_batch"),
				attribute.String("result", "error"),
			))
		return nil, err
	}

	span.SetAttributes(attribute.Int("items_returned", len(results)))
	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "get_batch"),
			attribute.String("result", "success"),
		))

	return results, nil
}

func (i *InstrumentedKV) Insert(ctx context.Context, item *Item) error {
	ctx, span := tracer.Start(ctx, "kv.Insert",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "insert"),
			attribute.String("partition_key", item.PK.PartitionKey.ToHexString()),
			attribute.String("row_key", item.PK.RowKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "insert"),
			))
	}()

	err := i.kv.Insert(ctx, item)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "insert"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "insert"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) Put(ctx context.Context, item *Item) error {
	ctx, span := tracer.Start(ctx, "kv.Put",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "put"),
			attribute.String("partition_key", item.PK.PartitionKey.ToHexString()),
			attribute.String("row_key", item.PK.RowKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "put"),
			))
	}()

	err := i.kv.Put(ctx, item)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "put"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "put"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	ctx, span := tracer.Start(ctx, "kv.Remove",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove"),
			attribute.String("partition_key", pk.PartitionKey.ToHexString()),
			attribute.String("row_key", pk.RowKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove"),
			))
	}()

	err := i.kv.Remove(ctx, pk)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	ctx, span := tracer.Start(ctx, "kv.RemoveBatch",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove_batch"),
			attribute.Int("batch_size", len(keys)),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove_batch"),
			))
	}()

	err := i.kv.RemoveBatch(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove_batch"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove_batch"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {
	ctx, span := tracer.Start(ctx, "kv.RemoveRange",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove_range"),
			attribute.String("partition_key", rangeKey.PartitionKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove_range"),
			))
	}()

	err := i.kv.RemoveRange(ctx, rangeKey)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "remove_range"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "remove_range"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) Query(ctx context.Context, queryArgs QueryArgs, sort SortDirection) ([]*Item, error) {
	ctx, span := tracer.Start(ctx, "kv.Query",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "query"),
			attribute.String("partition_key", queryArgs.PartitionKey.ToHexString()),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "query"),
			))
	}()

	items, err := i.kv.Query(ctx, queryArgs, sort)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "query"),
				attribute.String("result", "error"),
			))
		return nil, err
	}

	span.SetAttributes(attribute.Int("items_returned", len(items)))
	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "query"),
			attribute.String("result", "success"),
		))

	return items, nil
}

func (i *InstrumentedKV) Enumerate(ctx context.Context, queryArgs QueryArgs) enumerators.Enumerator[*Item] {
	ctx, span := tracer.Start(ctx, "kv.Enumerate",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "enumerate"),
			attribute.String("partition_key", queryArgs.PartitionKey.ToHexString()),
		))
	defer span.End()

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "enumerate"),
			attribute.String("result", "started"),
		))

	return i.kv.Enumerate(ctx, queryArgs)
}

func (i *InstrumentedKV) Batch(ctx context.Context, items []*BatchItem) error {
	ctx, span := tracer.Start(ctx, "kv.Batch",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "batch"),
			attribute.Int("batch_size", len(items)),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "batch"),
			))
	}()

	err := i.kv.Batch(ctx, items)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "batch"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "batch"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) BatchChunks(ctx context.Context, items enumerators.Enumerator[*BatchItem], chunkSize int) error {
	ctx, span := tracer.Start(ctx, "kv.BatchChunks",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "batch_chunks"),
			attribute.Int("chunk_size", chunkSize),
		))
	defer span.End()

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		operationDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "batch_chunks"),
			))
	}()

	err := i.kv.BatchChunks(ctx, items, chunkSize)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		operationCount.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("store", i.store),
				attribute.String("operation", "batch_chunks"),
				attribute.String("result", "error"),
			))
		return err
	}

	operationCount.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "batch_chunks"),
			attribute.String("result", "success"),
		))

	return nil
}

func (i *InstrumentedKV) Close() error {
	_, span := tracer.Start(context.Background(), "kv.Close",
		trace.WithAttributes(
			attribute.String("store", i.store),
			attribute.String("operation", "close"),
		))
	defer span.End()

	return i.kv.Close()
}
