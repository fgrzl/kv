package timeseries

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/timeseries")

// Sample represents a single time series datapoint.
type Sample struct {
	Series    string `json:"series"`
	Timestamp int64  `json:"ts"`
	Value     []byte `json:"value"`
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
}

// New returns a new TimeSeries overlay using the provided kv store and
// optional namespace name to partition keys.
// Option configures a TimeSeries instance.
type Option func(*TimeSeries)

// WithDescending configures the TimeSeries to store row-keys such that
// scans return newest->oldest (descending) order.
func WithDescending(d bool) Option {
	return func(ts *TimeSeries) { ts.descending = d }
}

func New(store kv.KV, name string, opts ...Option) *TimeSeries {
	ts := &TimeSeries{store: store, name: name}
	for _, o := range opts {
		o(ts)
	}
	return ts
}

func (ts *TimeSeries) partition(series string) lexkey.LexKey {
	return lexkey.Encode("timeseries", ts.name, series)
}

func (ts *TimeSeries) encodeRowKeyForTimestamp(tsVal int64) lexkey.LexKey {
	if !ts.descending {
		return lexkey.Encode(tsVal)
	}
	// Use bitwise complement to invert ordering in uint64 space. This maps
	// larger timestamps to smaller encoded values so that lexicographic
	// ascending scans return newest->oldest.
	return lexkey.Encode(^uint64(uint64(tsVal)))
}

func (ts *TimeSeries) encodeRangeForBounds(from, to int64) (lexkey.LexKey, lexkey.LexKey, bool) {
	// returns (startRowKey, endRowKey, ok)
	if from >= to {
		return nil, nil, false
	}
	if !ts.descending {
		return lexkey.Encode(from), lexkey.Encode(to), true
	}
	// descending: map [from, to) timestamps into encoded space.
	// encodedStart = enc(to-1) (smallest encoded value in range)
	// encodedEnd = enc(from) (largest encoded value in range)
	// handle edge when to==math.MinInt64 or to==0
	if to <= math.MinInt64+1 {
		return nil, nil, false
	}
	encStart := ts.encodeRowKeyForTimestamp(to - 1)
	encEnd := ts.encodeRowKeyForTimestamp(from)
	return encStart, encEnd, true
}

// Append inserts a sample for the given series. Timestamp should be an unix
// epoch in seconds or milliseconds as desired by the caller; lexicographic
// ordering is preserved by the lexkey encoding of the timestamp value.
func (ts *TimeSeries) Append(ctx context.Context, series string, timestamp int64, value []byte) error {
	ctx, span := tracer.Start(ctx, "timeseries.Append",
		trace.WithAttributes(
			attribute.String("timeseries", ts.name),
			attribute.String("series", series),
			attribute.Int64("timestamp", timestamp),
			attribute.Int("value_size", len(value)),
		))
	defer span.End()

	s := Sample{Series: series, Timestamp: timestamp, Value: value}
	b, err := json.Marshal(s)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal timeseries sample", "series", series, "timestamp", timestamp, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	pk := lexkey.NewPrimaryKey(ts.partition(series), ts.encodeRowKeyForTimestamp(timestamp))
	err = ts.store.Put(ctx, &kv.Item{PK: pk, Value: b})
	if err != nil {
		slog.ErrorContext(ctx, "failed to store timeseries sample", "series", series, "timestamp", timestamp, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	slog.DebugContext(ctx, "timeseries sample appended", "series", series, "timestamp", timestamp)
	return nil
}

// QueryRange returns samples for series within [from, to) ordered by timestamp ascending.
func (ts *TimeSeries) QueryRange(ctx context.Context, series string, from, to int64) ([]Sample, error) {
	slog.DebugContext(ctx, "querying timeseries range", "series", series, "from", from, "to", to)
	part := ts.partition(series)
	startRK, endRK, ok := ts.encodeRangeForBounds(from, to)
	if !ok {
		slog.DebugContext(ctx, "invalid timeseries range", "series", series, "from", from, "to", to)
		return nil, nil
	}
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: startRK, EndRowKey: endRK, Operator: kv.Scan}
	items, err := ts.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		slog.ErrorContext(ctx, "failed to query timeseries", "series", series, "from", from, "to", to, "err", err)
		return nil, err
	}
	var out []Sample
	for _, it := range items {
		var s Sample
		if err := json.Unmarshal(it.Value, &s); err != nil {
			slog.WarnContext(ctx, "skipping malformed timeseries sample", "series", series, "err", err)
			continue
		}
		// Ensure returned samples honor the requested bounds since underlying
		// storage query may include padding or use lexicographic row keys.
		if s.Timestamp >= from && s.Timestamp < to {
			out = append(out, s)
		}
	}
	slog.DebugContext(ctx, "timeseries query completed", "series", series, "samples", len(out))
	return out, nil
}

// DeleteSeries removes all samples for a series.
func (ts *TimeSeries) DeleteSeries(ctx context.Context, series string) error {
	slog.InfoContext(ctx, "deleting timeseries series", "series", series)
	rangeKey := lexkey.NewRangeKeyFull(ts.partition(series))
	err := ts.store.RemoveRange(ctx, rangeKey)
	if err != nil {
		slog.ErrorContext(ctx, "failed to delete timeseries series", "series", series, "err", err)
		return err
	}
	slog.InfoContext(ctx, "timeseries series deleted", "series", series)
	return nil
}

// EnumerateRange streams samples for `series` within [from, to) ordered by timestamp ascending.
func (ts *TimeSeries) EnumerateRange(ctx context.Context, series string, from, to int64) enumerators.Enumerator[Sample] {
	part := ts.partition(series)
	startRK, endRK, ok := ts.encodeRangeForBounds(from, to)
	if !ok {
		return enumerators.Empty[Sample]()
	}
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: startRK, EndRowKey: endRK, Operator: kv.Scan}
	inner := ts.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (Sample, bool, error) {
		var s Sample
		if err := json.Unmarshal(it.Value, &s); err != nil {
			return Sample{}, false, nil
		}
		if s.Timestamp >= from && s.Timestamp < to {
			return s, true, nil
		}
		return Sample{}, false, nil
	})
}
