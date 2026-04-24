package timeseries

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Append inserts a sample for the given series. Timestamp should be a unix
// epoch in the unit of your choosing (seconds, millis, nanos); lexicographic
// ordering is preserved by the lexkey encoding of the timestamp value. The
// id is appended to the row key so multiple events sharing the same timestamp
// can coexist. Pass an empty id to preserve overwrite-on-duplicate-timestamp
// behavior.
//
// The value bytes are stored verbatim in the underlying kv row, so there is
// zero per-row metadata overhead beyond the keys themselves.
func (ts *TimeSeries) Append(ctx context.Context, series string, timestamp int64, id string, value []byte) error {
	ctx, span := tracer.Start(ctx, "timeseries.Append")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("timeseries", ts.name),
			attribute.String("series", series),
			attribute.Int64("timestamp", timestamp),
			attribute.String("id", id),
			attribute.Int("value_size", len(value)),
		)
	}

	pk := lexkey.NewPrimaryKey(ts.partition(series), ts.encodeRowKey(timestamp, id))
	if err := ts.store.Put(ctx, &kv.Item{PK: pk, Value: value}); err != nil {
		slog.ErrorContext(ctx, "failed to store timeseries sample", "series", series, "timestamp", timestamp, "id", id, "err", err)
		if span.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
	return nil
}

// AppendTime inserts a sample using a time.Time value (converted to Unix
// nanoseconds). See Append for id semantics.
func (ts *TimeSeries) AppendTime(ctx context.Context, series string, timestamp time.Time, id string, value []byte) error {
	return ts.Append(ctx, series, timestamp.UnixNano(), id, value)
}

// AppendBatch writes a slice of entries in a single grouped batch call to
// the underlying store. Entries may target different series; the kv store
// groups by partition (one group == one series) before issuing any round
// trip, so Azure Table Storage translates this into the minimum number of
// batch-transaction HTTP calls (one per series, chunked at 100 ops each).
// Entries with the same (series, timestamp, id) triple within the batch
// collapse to a single final value (last write wins per row key).
func (ts *TimeSeries) AppendBatch(ctx context.Context, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ctx, span := tracer.Start(ctx, "timeseries.AppendBatch")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("timeseries", ts.name),
			attribute.Int("entry_count", len(entries)),
		)
	}

	items := make([]*kv.BatchItem, len(entries))
	for i, e := range entries {
		pk := lexkey.NewPrimaryKey(ts.partition(e.Series), ts.encodeRowKey(e.Timestamp, e.ID))
		items[i] = &kv.BatchItem{Op: kv.Put, PK: pk, Value: e.Value}
	}
	if err := ts.store.Batch(ctx, items); err != nil {
		if span.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
	return nil
}

// AppendBatchChunks streams entries through the store's chunked batch path,
// submitting chunkSize operations at a time without ever materializing the
// entire enumerator in memory. Use this for very large ingests (millions of
// samples) where holding the whole slice isn't viable.
//
// chunkSize must be >= 1 and should be <= 100 for Azure Table Storage (the
// service's per-batch op limit). For other backends chunkSize tunes the
// memory/round-trip tradeoff.
func (ts *TimeSeries) AppendBatchChunks(ctx context.Context, entries enumerators.Enumerator[Entry], chunkSize int) error {
	if chunkSize <= 0 {
		return errors.New("timeseries: chunkSize must be >= 1")
	}
	ctx, span := tracer.Start(ctx, "timeseries.AppendBatchChunks")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("timeseries", ts.name),
			attribute.Int("chunk_size", chunkSize),
		)
	}

	mapped := enumerators.Map(entries, func(e Entry) (*kv.BatchItem, error) {
		pk := lexkey.NewPrimaryKey(ts.partition(e.Series), ts.encodeRowKey(e.Timestamp, e.ID))
		return &kv.BatchItem{Op: kv.Put, PK: pk, Value: e.Value}, nil
	})
	if err := ts.store.BatchChunks(ctx, mapped, chunkSize); err != nil {
		if span.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
	return nil
}
