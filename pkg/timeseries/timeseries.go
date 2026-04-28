// Package timeseries provides a thin, storage-efficient overlay on top of a
// kv.KV store for append-only, time-ordered samples per series.
//
// Storage layout (wire-efficient for all backends, but especially important
// for Azure Table Storage where every byte stored and returned costs money
// and bandwidth):
//
//	PartitionKey = lexkey.Encode("timeseries", <namespace>, <series>)
//	RowKey       = lexkey.Encode(<orderedTs>[, <id>])
//	Value        = raw caller bytes (no JSON wrapper)
//
// The timestamp (and optional per-event id) live in the row key itself, so
// they don't need to be duplicated in the stored value. Samples are
// reconstructed on read by decoding the row key — no JSON marshal/unmarshal
// per row, and no wasted bytes on the wire.
//
// File layout in this package:
//
//	timeseries.go  core types, options, constructor, DeleteSeries
//	keys.go        row-key and range-key encoding/decoding
//	write.go       Append / AppendTime / AppendBatch / AppendBatchChunks
//	read.go        QueryRange / EnumerateRange / Latest and time variants
package timeseries

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// tracer is the package-level OpenTelemetry tracer. Span attributes are
// populated lazily (only when a span is recording) so tracing carries no
// allocation cost on the hot write path when disabled.
var tracer = otel.Tracer("github.com/fgrzl/kv/timeseries")

// -----------------------------------------------------------------------------
// Public types
// -----------------------------------------------------------------------------

// Sample represents a single time series datapoint. Series, Timestamp, and
// ID are populated from the row key on read; only Value is stored in the
// underlying kv row.
type Sample struct {
	Series    string
	Timestamp int64
	// ID is an optional per-event suffix used to make the row key unique
	// when multiple events share the same timestamp. An empty ID preserves
	// overwrite-on-duplicate behavior.
	ID    string
	Value []byte
}

// Entry is a single sample to append as part of a batch. Series identifies
// the target series (entries in a batch may target different series — the
// batch is grouped per partition before being handed to the store).
type Entry struct {
	Series    string
	Timestamp int64
	ID        string
	Value     []byte
}

// TimeSeries provides a small overlay backed by a kv.KV store to append and
// query time-ordered samples per series.
type TimeSeries struct {
	store kv.KV
	name  string
	// descending indicates the natural order for this series. When true,
	// row keys are encoded so that lexicographic ascending scans yield
	// newest->oldest items.
	descending bool
	// partitionCache memoizes lexkey.Encode("timeseries", ts.name, series)
	// so hot-path writes don't re-encode the partition key per call. Values
	// are lexkey.LexKey (immutable once stored).
	partitionCache sync.Map
}

// -----------------------------------------------------------------------------
// Options & constructor
// -----------------------------------------------------------------------------

// Option configures a TimeSeries instance.
type Option func(*TimeSeries)

// WithDescending configures the TimeSeries to store row-keys such that
// scans return newest->oldest (descending) order.
func WithDescending(d bool) Option {
	return func(ts *TimeSeries) { ts.descending = d }
}

// New returns a new TimeSeries overlay using the provided kv store and
// namespace name. The namespace is used as a prefix in partition keys so
// multiple overlays can share a single kv store without key collisions.
func New(store kv.KV, name string, opts ...Option) *TimeSeries {
	ts := &TimeSeries{store: store, name: name}
	for _, o := range opts {
		o(ts)
	}
	return ts
}

// -----------------------------------------------------------------------------
// Partition key cache
// -----------------------------------------------------------------------------

// partition returns the encoded partition key for the given series, using a
// small per-instance cache so hot paths don't re-encode on every call.
func (ts *TimeSeries) partition(series string) lexkey.LexKey {
	if v, ok := ts.partitionCache.Load(series); ok {
		return v.(lexkey.LexKey)
	}
	encoded := lexkey.Encode("timeseries", ts.name, series)
	actual, _ := ts.partitionCache.LoadOrStore(series, encoded)
	return actual.(lexkey.LexKey)
}

// -----------------------------------------------------------------------------
// Admin
// -----------------------------------------------------------------------------

// DeleteSeries removes all samples for a series.
func (ts *TimeSeries) DeleteSeries(ctx context.Context, series string) error {
	rangeKey := lexkey.NewRangeKeyFull(ts.partition(series))
	if err := ts.store.RemoveRange(ctx, rangeKey); err != nil {
		slog.ErrorContext(ctx, "failed to delete timeseries series", "series", series, "err", err)
		return err
	}
	return nil
}

// PruneRange removes samples for series within [from, to) using the same
// timestamp semantics as QueryRange. This is the building block for TTL-style
// retention jobs: call it with your retention cutoff to delete expired data
// without scanning or materializing the whole series in memory.
func (ts *TimeSeries) PruneRange(ctx context.Context, series string, from, to int64) error {
	startRK, endRK, ok := ts.encodeRangeForBounds(from, to)
	if !ok {
		return nil
	}
	rangeKey := lexkey.NewRangeKey(ts.partition(series), startRK, endRK)
	if err := ts.store.RemoveRange(ctx, rangeKey); err != nil {
		slog.ErrorContext(ctx, "failed to prune timeseries range", "series", series, "from", from, "to", to, "err", err)
		return err
	}
	return nil
}

// PruneRangeTime is the time.Time variant of PruneRange (UnixNano).
func (ts *TimeSeries) PruneRangeTime(ctx context.Context, series string, from, to time.Time) error {
	return ts.PruneRange(ctx, series, from.UnixNano(), to.UnixNano())
}
