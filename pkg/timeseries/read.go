package timeseries

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// ErrLatestRequiresDescending is returned by Latest when called on a series
// that was not created with WithDescending(true). An efficient "latest N"
// query requires the underlying row keys to be ordered newest-first.
var ErrLatestRequiresDescending = errors.New("timeseries: Latest requires WithDescending(true)")

// -----------------------------------------------------------------------------
// Query options
// -----------------------------------------------------------------------------

// QueryOption configures a range query or enumeration.
type QueryOption func(*queryOptions)

type queryOptions struct {
	limit int
}

// WithLimit caps the total number of samples returned by a range query or
// enumeration. A value of 0 (default) means no cap. Passing a limit is the
// preferred way to keep Azure Table Storage scans from fetching unbounded
// pages when you only need the first N results.
func WithLimit(n int) QueryOption {
	return func(o *queryOptions) { o.limit = n }
}

func buildQueryOptions(opts []QueryOption) queryOptions {
	var o queryOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// -----------------------------------------------------------------------------
// Range queries
// -----------------------------------------------------------------------------

// QueryRange returns samples for series within [from, to) in the series'
// natural order. The id suffix on the row key (if any) is recovered into
// Sample.ID. Optional WithLimit caps the total number of samples returned,
// which is particularly important on Azure Table Storage where unbounded
// scans can page through large amounts of data.
func (ts *TimeSeries) QueryRange(ctx context.Context, series string, from, to int64, opts ...QueryOption) ([]Sample, error) {
	args, part, ok := ts.buildQueryArgs(series, from, to, buildQueryOptions(opts))
	if !ok {
		return nil, nil
	}
	items, err := ts.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		slog.ErrorContext(ctx, "failed to query timeseries", "series", series, "from", from, "to", to, "err", err)
		return nil, err
	}
	out := make([]Sample, 0, len(items))
	for _, it := range items {
		s, ok := ts.itemToSample(series, part, it)
		if !ok {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// EnumerateRange streams samples for series within [from, to) in the series'
// natural order without materializing the full result set.
func (ts *TimeSeries) EnumerateRange(ctx context.Context, series string, from, to int64, opts ...QueryOption) enumerators.Enumerator[Sample] {
	args, part, ok := ts.buildQueryArgs(series, from, to, buildQueryOptions(opts))
	if !ok {
		return enumerators.Empty[Sample]()
	}
	inner := ts.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (Sample, bool, error) {
		s, ok := ts.itemToSample(series, part, it)
		return s, ok, nil
	})
}

// QueryRangeTime is the time.Time variant of QueryRange (UnixNano).
func (ts *TimeSeries) QueryRangeTime(ctx context.Context, series string, from, to time.Time, opts ...QueryOption) ([]Sample, error) {
	return ts.QueryRange(ctx, series, from.UnixNano(), to.UnixNano(), opts...)
}

// EnumerateRangeTime is the time.Time variant of EnumerateRange (UnixNano).
func (ts *TimeSeries) EnumerateRangeTime(ctx context.Context, series string, from, to time.Time, opts ...QueryOption) enumerators.Enumerator[Sample] {
	return ts.EnumerateRange(ctx, series, from.UnixNano(), to.UnixNano(), opts...)
}

// -----------------------------------------------------------------------------
// Latest
// -----------------------------------------------------------------------------

// Latest returns up to n most-recent samples for the series. This only makes
// sense on a TimeSeries created with WithDescending(true): the row keys are
// already ordered newest-first, so the query is a plain bounded partition
// scan with Limit=n — a single Azure Table `PartitionKey eq '...'` filter
// with `$top=n`, no client-side reversal, no pulling the whole series.
//
// Returns ErrLatestRequiresDescending if the series is ascending-ordered.
func (ts *TimeSeries) Latest(ctx context.Context, series string, n int) ([]Sample, error) {
	if !ts.descending {
		return nil, ErrLatestRequiresDescending
	}
	if n <= 0 {
		return nil, nil
	}
	part := ts.partition(series)
	args := kv.QueryArgs{
		PartitionKey: part,
		Operator:     kv.Scan,
		Limit:        n,
	}
	// Enumerate + early-stop rather than Query+materialize: some backends
	// only apply Limit as a filter (not a terminating cursor), so driving
	// iteration ourselves guarantees we stop after n items on every store.
	enum := ts.store.Enumerate(ctx, args)
	defer enum.Dispose()
	out := make([]Sample, 0, n)
	for enum.MoveNext() && len(out) < n {
		it, err := enum.Current()
		if err != nil {
			slog.ErrorContext(ctx, "failed to query timeseries latest", "series", series, "n", n, "err", err)
			return nil, err
		}
		s, ok := ts.itemToSample(series, part, it)
		if !ok {
			continue
		}
		out = append(out, s)
	}
	if err := enum.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Internals shared by the read paths
// -----------------------------------------------------------------------------

// buildQueryArgs assembles a kv.QueryArgs for a [from, to) scan. Returns the
// partition key used (so readers can strip it off decoded row keys) along
// with ok=false when the range is empty or invalid.
func (ts *TimeSeries) buildQueryArgs(series string, from, to int64, opts queryOptions) (kv.QueryArgs, lexkey.LexKey, bool) {
	startRK, endRK, ok := ts.encodeRangeForBounds(from, to)
	if !ok {
		return kv.QueryArgs{}, nil, false
	}
	part := ts.partition(series)
	return kv.QueryArgs{
		PartitionKey: part,
		StartRowKey:  startRK,
		EndRowKey:    endRK,
		Operator:     kv.Between,
		Limit:        opts.limit,
	}, part, true
}

// itemToSample reconstructs a Sample from a kv.Item by decoding the row key.
// The known partition key is used to recover the true row-key bytes across
// backends that may split partition/row at the first separator. Malformed
// rows are logged and skipped so one bad row can't derail a whole scan.
func (ts *TimeSeries) itemToSample(series string, part lexkey.LexKey, it *kv.Item) (Sample, bool) {
	rk, ok := extractRowKey(part, it.PK)
	if !ok {
		slog.Warn("skipping timeseries row with unexpected partition split", "series", series)
		return Sample{}, false
	}
	tsVal, id, err := ts.decodeRowKey(rk)
	if err != nil {
		slog.Warn("skipping timeseries row with malformed row key", "series", series, "err", err)
		return Sample{}, false
	}
	return Sample{
		Series:    series,
		Timestamp: tsVal,
		ID:        id,
		Value:     it.Value,
	}, true
}
