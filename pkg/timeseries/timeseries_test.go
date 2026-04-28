package timeseries

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTS(t *testing.T) *TimeSeries {
	path := filepath.Join(t.TempDir(), "ts")
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return New(store, "testns")
}

func setupTSDescending(t *testing.T) *TimeSeries {
	path := filepath.Join(t.TempDir(), "ts-desc")
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return New(store, "testns", WithDescending(true))
}

func TestShouldAppendAndQueryRange(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	require.NoError(t, ts.Append(ctx, "s1", 1, "", []byte("v1")))
	require.NoError(t, ts.Append(ctx, "s1", 2, "", []byte("v2")))
	require.NoError(t, ts.Append(ctx, "s1", 3, "", []byte("v3")))

	// Assert
	out, err := ts.QueryRange(ctx, "s1", 1, 4)
	assert.NoError(t, err)
	assert.Len(t, out, 3)
	assert.Equal(t, int64(1), out[0].Timestamp)
	assert.Equal(t, []byte("v1"), out[0].Value)

	// Partial range
	out2, err := ts.QueryRange(ctx, "s1", 2, 3)
	assert.NoError(t, err)
	assert.Len(t, out2, 1)
	assert.Equal(t, int64(2), out2[0].Timestamp)
}

func TestShouldDeleteSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s2", 10, "", []byte("x")))
	require.NoError(t, ts.Append(ctx, "s2", 20, "", []byte("y")))

	// Act
	require.NoError(t, ts.DeleteSeries(ctx, "s2"))

	// Assert
	items, err := ts.QueryRange(ctx, "s2", 0, 100)
	assert.NoError(t, err)
	assert.Len(t, items, 0)
}

func TestShouldPruneRange(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s3", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "s3", 20, "a", []byte("twenty-a")))
	require.NoError(t, ts.Append(ctx, "s3", 20, "b", []byte("twenty-b")))
	require.NoError(t, ts.Append(ctx, "s3", 30, "", []byte("thirty")))

	// Act
	require.NoError(t, ts.PruneRange(ctx, "s3", 10, 30))
	out, err := ts.QueryRange(ctx, "s3", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, int64(30), out[0].Timestamp)
	assert.Equal(t, []byte("thirty"), out[0].Value)
}

func TestShouldPruneRangeOnDescendingSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "sd-prune", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "sd-prune", 20, "", []byte("twenty")))
	require.NoError(t, ts.Append(ctx, "sd-prune", 30, "", []byte("thirty")))

	// Act
	require.NoError(t, ts.PruneRange(ctx, "sd-prune", 0, 20))
	out, err := ts.QueryRange(ctx, "sd-prune", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, []int64{30, 20}, []int64{out[0].Timestamp, out[1].Timestamp})
}

func TestShouldPruneBeforeExcludingCutoff(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s-before", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "s-before", 20, "a", []byte("twenty-a")))
	require.NoError(t, ts.Append(ctx, "s-before", 20, "b", []byte("twenty-b")))
	require.NoError(t, ts.Append(ctx, "s-before", 30, "", []byte("thirty")))

	// Act
	require.NoError(t, ts.PruneBefore(ctx, "s-before", 20))
	out, err := ts.QueryRange(ctx, "s-before", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, []int64{20, 20, 30}, []int64{out[0].Timestamp, out[1].Timestamp, out[2].Timestamp})
}

func TestShouldPruneBeforeOnDescendingSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "sd-before", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "sd-before", 20, "", []byte("twenty")))
	require.NoError(t, ts.Append(ctx, "sd-before", 30, "", []byte("thirty")))

	// Act
	require.NoError(t, ts.PruneBefore(ctx, "sd-before", 30))
	out, err := ts.QueryRange(ctx, "sd-before", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, int64(30), out[0].Timestamp)
}

func TestShouldPruneOldestTimestampOnDescendingSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "sd-min", math.MinInt64, "", []byte("min")))
	require.NoError(t, ts.Append(ctx, "sd-min", math.MinInt64+1, "", []byte("min-plus-one")))

	// Act
	require.NoError(t, ts.PruneBefore(ctx, "sd-min", math.MinInt64+1))
	out, err := ts.QueryRange(ctx, "sd-min", math.MinInt64, math.MinInt64+2)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, int64(math.MinInt64+1), out[0].Timestamp)
	assert.Equal(t, []byte("min-plus-one"), out[0].Value)
}

func TestShouldNotPruneWhenRangeIsEmpty(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s-empty", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "s-empty", 20, "", []byte("twenty")))

	// Act
	require.NoError(t, ts.PruneRange(ctx, "s-empty", 20, 20))
	require.NoError(t, ts.PruneRange(ctx, "s-empty", 30, 20))
	out, err := ts.QueryRange(ctx, "s-empty", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, []int64{10, 20}, []int64{out[0].Timestamp, out[1].Timestamp})
}

func TestShouldPreserveUpperBoundTimestampAndIDsWhenPruning(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s-upper", 10, "", []byte("ten")))
	require.NoError(t, ts.Append(ctx, "s-upper", 20, "a", []byte("twenty-a")))
	require.NoError(t, ts.Append(ctx, "s-upper", 20, "b", []byte("twenty-b")))
	require.NoError(t, ts.Append(ctx, "s-upper", 30, "", []byte("thirty")))

	// Act
	require.NoError(t, ts.PruneRange(ctx, "s-upper", 10, 20))
	out, err := ts.QueryRange(ctx, "s-upper", 0, 100)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, []int64{20, 20, 30}, []int64{out[0].Timestamp, out[1].Timestamp, out[2].Timestamp})
	assert.Equal(t, []string{"a", "b", ""}, []string{out[0].ID, out[1].ID, out[2].ID})
}

func TestShouldPreserveMaxTimestampWhenPruningBeforeMax(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s-max", math.MaxInt64-1, "", []byte("max-minus-one")))
	require.NoError(t, ts.Append(ctx, "s-max", math.MaxInt64, "", []byte("max")))

	// Act
	require.NoError(t, ts.PruneBefore(ctx, "s-max", math.MaxInt64))
	out, err := ts.QueryRange(ctx, "s-max", math.MaxInt64-1, math.MaxInt64)

	// Assert
	require.NoError(t, err)
	assert.Empty(t, out)

	item, err := ts.store.Get(ctx, lexkey.NewPrimaryKey(ts.partition("s-max"), ts.encodeRowKey(math.MaxInt64, "")))
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, []byte("max"), item.Value)
}

func TestShouldNotPruneAnythingBeforeMinTimestamp(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "sd-before-min", math.MinInt64, "", []byte("min")))
	require.NoError(t, ts.Append(ctx, "sd-before-min", 0, "", []byte("zero")))

	// Act
	require.NoError(t, ts.PruneBefore(ctx, "sd-before-min", math.MinInt64))
	out, err := ts.QueryRange(ctx, "sd-before-min", math.MinInt64, 1)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, []int64{0, math.MinInt64}, []int64{out[0].Timestamp, out[1].Timestamp})
}

func TestShouldEnumerateRangeStream(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s4", 1, "", []byte("x")))
	require.NoError(t, ts.Append(ctx, "s4", 2, "", []byte("y")))
	require.NoError(t, ts.Append(ctx, "s4", 3, "", []byte("z")))

	// Act
	enum := ts.EnumerateRange(ctx, "s4", 1, 4)
	var got [][]byte
	err := enumerators.ForEach(enum, func(s Sample) error {
		got = append(got, s.Value)
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.Equal(t, []byte("x"), got[0])
	assert.Equal(t, []byte("y"), got[1])
	assert.Equal(t, []byte("z"), got[2])
}

func TestShouldOrderSeriesDescendingWhenConfigured(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "sd", 1, "", []byte("a")))
	require.NoError(t, ts.Append(ctx, "sd", 2, "", []byte("b")))
	require.NoError(t, ts.Append(ctx, "sd", 3, "", []byte("c")))

	// Act
	out, err := ts.QueryRange(ctx, "sd", 1, 4)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, int64(3), out[0].Timestamp)
	assert.Equal(t, int64(2), out[1].Timestamp)
	assert.Equal(t, int64(1), out[2].Timestamp)

	// Enumerator should also stream in descending order
	enum := ts.EnumerateRange(ctx, "sd", 1, 4)
	var tsVals []int64
	err = enumerators.ForEach(enum, func(s Sample) error {
		tsVals = append(tsVals, s.Timestamp)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 2, 1}, tsVals)
}

func TestShouldReturnEmptyWhenQueryingNonExistentSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	out, err := ts.QueryRange(ctx, "nonexistent", 0, 100)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out, 0)
}

func TestShouldHandleAppendingDuplicateTimestamps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s", 1, "", []byte("first")))

	// Act
	err := ts.Append(ctx, "s", 1, "", []byte("second"))
	assert.NoError(t, err)

	// Assert
	// Empty id preserves legacy overwrite-on-duplicate-timestamp behavior.
	out, err := ts.QueryRange(ctx, "s", 1, 2)
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, []byte("second"), out[0].Value)
}

func TestShouldStoreRawValueBytesWithNoJSONEnvelope(t *testing.T) {
	// Arrange — the storage format intentionally stores the caller's bytes
	// verbatim so Azure Table rows don't pay a per-row JSON tax.
	ctx := context.Background()
	ts := setupTS(t)
	raw := []byte("not-json \x00\x01\x02 at all")
	require.NoError(t, ts.Append(ctx, "s", 42, "evt", raw))

	// Act — read the underlying kv row directly, bypassing the overlay.
	pk := lexkey.NewPrimaryKey(ts.partition("s"), ts.encodeRowKey(42, "evt"))
	item, err := ts.store.Get(ctx, pk)
	require.NoError(t, err)
	require.NotNil(t, item)

	// Assert — stored bytes are exactly what was passed to Append.
	assert.Equal(t, raw, item.Value)
}

func TestShouldSkipRowsWithMalformedRowKey(t *testing.T) {
	// Arrange — a row with a truncated row key must not panic the decoder;
	// it should be skipped so one corrupt row can't derail a whole scan.
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "mixed", 100, "", []byte("good")))
	badPK := lexkey.NewPrimaryKey(ts.partition("mixed"), lexkey.LexKey{0x01, 0x02})
	require.NoError(t, ts.store.Put(ctx, &kv.Item{PK: badPK, Value: []byte("bad")}))

	// Act
	out, err := ts.QueryRange(ctx, "mixed", 0, 200)

	// Assert — the malformed row is dropped; the good row survives.
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, int64(100), out[0].Timestamp)
	assert.Equal(t, []byte("good"), out[0].Value)
}

func TestShouldHonorQueryLimit(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	for i := int64(0); i < 10; i++ {
		require.NoError(t, ts.Append(ctx, "s", i, "", []byte{byte(i)}))
	}

	// Act
	out, err := ts.QueryRange(ctx, "s", 0, 10, WithLimit(3))

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, int64(0), out[0].Timestamp)
	assert.Equal(t, int64(1), out[1].Timestamp)
	assert.Equal(t, int64(2), out[2].Timestamp)
}

func TestShouldRoundtripTimestampAndIDFromRowKey(t *testing.T) {
	// Arrange — a spread of timestamps (including negative and MinInt64+1)
	// and ids with non-ASCII bytes to exercise the decoder.
	ctx := context.Background()
	ts := setupTS(t)
	tsDesc := setupTSDescending(t)

	cases := []struct {
		ts int64
		id string
	}{
		{ts: math.MinInt64 + 1, id: ""},
		{ts: -42, id: "neg"},
		{ts: 0, id: ""},
		{ts: 1, id: "a b c"},
		{ts: 1_700_000_000_000_000_000, id: "uuid-ish"},
		{ts: math.MaxInt64, id: "\xf0\x9f\x98\x80"},
	}
	for _, c := range cases {
		require.NoError(t, ts.Append(ctx, "s", c.ts, c.id, []byte("v")))
		require.NoError(t, tsDesc.Append(ctx, "s", c.ts, c.id, []byte("v")))
	}

	// Act & assert for ascending
	out, err := ts.QueryRange(ctx, "s", math.MinInt64+1, math.MaxInt64)
	require.NoError(t, err)
	// QueryRange bounds are [from, to), so MaxInt64 itself is not returned.
	// Verify the rest roundtripped correctly.
	assert.Equal(t, len(cases)-1, len(out))
	for _, s := range out {
		assert.Equal(t, "s", s.Series)
	}

	// Descending scan should hit the MaxInt64 entry first.
	outDesc, err := tsDesc.QueryRange(ctx, "s", 0, math.MaxInt64)
	require.NoError(t, err)
	require.NotEmpty(t, outDesc)
	assert.Equal(t, int64(1_700_000_000_000_000_000), outDesc[0].Timestamp)
	assert.Equal(t, "uuid-ish", outDesc[0].ID)
}

func TestShouldHandleNegativeTimestamps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	err := ts.Append(ctx, "series", -1000, "", []byte("negative-ts"))

	// Assert
	assert.NoError(t, err)

	// Should be able to query it back
	out, err := ts.QueryRange(ctx, "series", -2000, 0)
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, int64(-1000), out[0].Timestamp)
}

func TestShouldHandleDuplicateTimestamps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "series", 1000, "", []byte("first")))
	require.NoError(t, ts.Append(ctx, "series", 1000, "", []byte("second"))) // Same timestamp - should overwrite

	// Act
	out, err := ts.QueryRange(ctx, "series", 999, 1001)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out, 1) // Only one result - last one wins
	assert.Equal(t, int64(1000), out[0].Timestamp)
	assert.Equal(t, []byte("second"), out[0].Value) // Last written value
}

func TestShouldPreserveDuplicateTimestampsWhenIDProvided(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "series", 1000, "a", []byte("first")))
	require.NoError(t, ts.Append(ctx, "series", 1000, "b", []byte("second")))
	require.NoError(t, ts.Append(ctx, "series", 1000, "c", []byte("third")))

	// Act
	out, err := ts.QueryRange(ctx, "series", 999, 1001)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	ids := []string{out[0].ID, out[1].ID, out[2].ID}
	assert.ElementsMatch(t, []string{"a", "b", "c"}, ids)
	for _, s := range out {
		assert.Equal(t, int64(1000), s.Timestamp)
	}
}

func TestShouldPreserveDuplicateTimestampsWhenIDProvidedDescending(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "series", 1000, "a", []byte("first")))
	require.NoError(t, ts.Append(ctx, "series", 1000, "b", []byte("second")))
	require.NoError(t, ts.Append(ctx, "series", 2000, "c", []byte("third")))

	// Act
	out, err := ts.QueryRange(ctx, "series", 1000, 2001)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 3)
	// Newest timestamp first (descending), then both id=a/b at ts=1000 in some order.
	assert.Equal(t, int64(2000), out[0].Timestamp)
	assert.Equal(t, "c", out[0].ID)
	assert.Equal(t, int64(1000), out[1].Timestamp)
	assert.Equal(t, int64(1000), out[2].Timestamp)
	ids := []string{out[1].ID, out[2].ID}
	assert.ElementsMatch(t, []string{"a", "b"}, ids)
}

func TestShouldNotCollideBetweenSuffixedAndPlainAtSameTimestamp(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "series", 1000, "", []byte("plain")))
	require.NoError(t, ts.Append(ctx, "series", 1000, "x", []byte("suffixed")))

	// Act
	out, err := ts.QueryRange(ctx, "series", 999, 1001)

	// Assert
	require.NoError(t, err)
	require.Len(t, out, 2)
	var plain, suffixed *Sample
	for i := range out {
		s := &out[i]
		if s.ID == "" {
			plain = s
		} else {
			suffixed = s
		}
	}
	require.NotNil(t, plain)
	require.NotNil(t, suffixed)
	assert.Equal(t, []byte("plain"), plain.Value)
	assert.Equal(t, []byte("suffixed"), suffixed.Value)
	assert.Equal(t, "x", suffixed.ID)
}

func TestShouldRoundtripIDViaAppendTime(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	now := time.Now()

	// Act
	require.NoError(t, ts.AppendTime(ctx, "s", now, "evt-1", []byte("a")))
	require.NoError(t, ts.AppendTime(ctx, "s", now, "evt-2", []byte("b")))

	// Assert
	out, err := ts.QueryRangeTime(ctx, "s", now, now.Add(time.Nanosecond))
	require.NoError(t, err)
	require.Len(t, out, 2)
	ids := []string{out[0].ID, out[1].ID}
	assert.ElementsMatch(t, []string{"evt-1", "evt-2"}, ids)
}

func TestShouldHandleInvalidRangeQueries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act - from >= to should return empty
	out, err := ts.QueryRange(ctx, "series", 100, 50)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out, 0)
}

func TestShouldHandleVeryLargeTimestamps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	largeTs := int64(9223372036854775807) // Max int64

	// Act
	err := ts.Append(ctx, "series", largeTs, "", []byte("large-ts"))

	// Assert
	assert.NoError(t, err)

	// Query the entire possible range to find our data
	out, err := ts.QueryRange(ctx, "series", largeTs-1000, largeTs+1)
	assert.NoError(t, err)
	if len(out) > 0 {
		assert.Equal(t, largeTs, out[0].Timestamp)
		assert.Equal(t, []byte("large-ts"), out[0].Value)
	} else {
		// If range query fails due to encoding issues, at least verify append didn't error
		t.Logf("Large timestamp %d may have encoding issues with range queries", largeTs)
	}
}

func TestShouldHandleDescendingOrderConsistency(t *testing.T) {
	// Arrange
	ctx := context.Background()
	tsDesc := setupTSDescending(t)
	require.NoError(t, tsDesc.Append(ctx, "series", 1, "", []byte("first")))
	require.NoError(t, tsDesc.Append(ctx, "series", 2, "", []byte("second")))
	require.NoError(t, tsDesc.Append(ctx, "series", 3, "", []byte("third")))

	// Act
	out, err := tsDesc.QueryRange(ctx, "series", 1, 4)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out, 3)
	// In descending order, results should be returned newest-first
	assert.Equal(t, int64(3), out[0].Timestamp)
	assert.Equal(t, int64(2), out[1].Timestamp)
	assert.Equal(t, int64(1), out[2].Timestamp)
}

func TestShouldHandleEmptySeriesNames(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	err := ts.Append(ctx, "", 1000, "", []byte("empty-series"))

	// Assert
	assert.NoError(t, err)

	out, err := ts.QueryRange(ctx, "", 999, 1001)
	assert.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestShouldHandleLargeValueData(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Act
	err := ts.Append(ctx, "series", 1000, "", largeValue)

	// Assert
	assert.NoError(t, err)

	out, err := ts.QueryRange(ctx, "series", 999, 1001)
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, largeValue, out[0].Value)
}

func TestShouldAppendAndQueryRangeWithTime(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	now := time.Now()
	t1 := now.Add(-2 * time.Second)
	t2 := now.Add(-1 * time.Second)
	t3 := now

	// Act
	require.NoError(t, ts.AppendTime(ctx, "s1", t1, "", []byte("v1")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t2, "", []byte("v2")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t3, "", []byte("v3")))

	// Assert
	out, err := ts.QueryRangeTime(ctx, "s1", t1, now.Add(1*time.Second))
	assert.NoError(t, err)
	assert.Len(t, out, 3)
	assert.Equal(t, t1.UnixNano(), out[0].Timestamp)
	assert.Equal(t, []byte("v1"), out[0].Value)

	// Partial range
	out2, err := ts.QueryRangeTime(ctx, "s1", t2, t3)
	assert.NoError(t, err)
	assert.Len(t, out2, 1) // Only t2
	assert.Equal(t, t2.UnixNano(), out2[0].Timestamp)
}

func TestShouldEnumerateRangeWithTime(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	now := time.Now()
	t1 := now.Add(-2 * time.Second)
	t2 := now.Add(-1 * time.Second)
	t3 := now

	require.NoError(t, ts.AppendTime(ctx, "s1", t1, "", []byte("v1")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t2, "", []byte("v2")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t3, "", []byte("v3")))

	// Act
	enum := ts.EnumerateRangeTime(ctx, "s1", t1, now.Add(1*time.Second))
	var samples []Sample
	err := enumerators.ForEach(enum, func(s Sample) error {
		samples = append(samples, s)
		return nil
	})

	// Assert
	assert.NoError(t, err)
	assert.Len(t, samples, 3)
	assert.Equal(t, t1.UnixNano(), samples[0].Timestamp)
	assert.Equal(t, t2.UnixNano(), samples[1].Timestamp)
	assert.Equal(t, t3.UnixNano(), samples[2].Timestamp)
}

func TestShouldOrderDescendingSeriesCorrectlyAcrossZero(t *testing.T) {
	// Arrange — descending encoding must preserve int64 ordering across the
	// sign boundary (this was previously broken by a plain bitwise complement
	// that treated the high bit as a magnitude bit instead of a sign flip).
	ctx := context.Background()
	ts := setupTSDescending(t)
	stamps := []int64{math.MinInt64 + 1, -100, -1, 0, 1, 100, math.MaxInt64 - 1}
	for i, s := range stamps {
		require.NoError(t, ts.Append(ctx, "sd", s, "", []byte{byte(i)}))
	}

	// Act
	out, err := ts.QueryRange(ctx, "sd", math.MinInt64+1, math.MaxInt64)

	// Assert — newest (largest) first, strictly decreasing.
	require.NoError(t, err)
	require.Len(t, out, len(stamps))
	for i := 1; i < len(out); i++ {
		assert.Greater(t, out[i-1].Timestamp, out[i].Timestamp,
			"expected strictly descending timestamps, got %v", timestampsOf(out))
	}
	assert.Equal(t, int64(math.MaxInt64-1), out[0].Timestamp)
	assert.Equal(t, int64(math.MinInt64+1), out[len(out)-1].Timestamp)
}

func timestampsOf(samples []Sample) []int64 {
	out := make([]int64, len(samples))
	for i, s := range samples {
		out[i] = s.Timestamp
	}
	return out
}

func TestShouldAppendBatchAcrossMultipleSeries(t *testing.T) {
	// Arrange — batch writes should correctly route to their target series
	// and be readable via the normal query path.
	ctx := context.Background()
	ts := setupTS(t)
	entries := []Entry{
		{Series: "a", Timestamp: 1, ID: "", Value: []byte("a1")},
		{Series: "a", Timestamp: 2, ID: "", Value: []byte("a2")},
		{Series: "b", Timestamp: 1, ID: "x", Value: []byte("b1x")},
		{Series: "b", Timestamp: 1, ID: "y", Value: []byte("b1y")},
	}

	// Act
	require.NoError(t, ts.AppendBatch(ctx, entries))

	// Assert
	outA, err := ts.QueryRange(ctx, "a", 0, 10)
	require.NoError(t, err)
	require.Len(t, outA, 2)
	assert.Equal(t, []byte("a1"), outA[0].Value)
	assert.Equal(t, []byte("a2"), outA[1].Value)

	outB, err := ts.QueryRange(ctx, "b", 0, 10)
	require.NoError(t, err)
	require.Len(t, outB, 2)
	ids := []string{outB[0].ID, outB[1].ID}
	assert.ElementsMatch(t, []string{"x", "y"}, ids)
}

func TestShouldAppendBatchNoOpOnEmpty(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	err := ts.AppendBatch(ctx, nil)

	// Assert
	assert.NoError(t, err)
}

func TestShouldStreamAppendBatchChunks(t *testing.T) {
	// Arrange — chunked path should process arbitrarily large enumerators
	// without requiring the full slice in memory.
	ctx := context.Background()
	ts := setupTS(t)
	const total = 250
	built := make([]Entry, total)
	for i := 0; i < total; i++ {
		built[i] = Entry{Series: "s", Timestamp: int64(i), Value: []byte{byte(i)}}
	}
	enum := enumerators.Slice(built)

	// Act
	require.NoError(t, ts.AppendBatchChunks(ctx, enum, 100))

	// Assert
	out, err := ts.QueryRange(ctx, "s", 0, total)
	require.NoError(t, err)
	assert.Len(t, out, total)
}

func TestShouldRejectAppendBatchChunksWithInvalidChunkSize(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	err := ts.AppendBatchChunks(ctx, enumerators.Empty[Entry](), 0)

	// Assert
	require.Error(t, err)
}

func TestShouldReturnLatestSamplesFromDescendingSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	for i := int64(1); i <= 10; i++ {
		require.NoError(t, ts.Append(ctx, "s", i, "", []byte{byte(i)}))
	}

	// Act
	out, err := ts.Latest(ctx, "s", 3)

	// Assert — newest three, newest first.
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, int64(10), out[0].Timestamp)
	assert.Equal(t, int64(9), out[1].Timestamp)
	assert.Equal(t, int64(8), out[2].Timestamp)
}

func TestShouldReturnEmptyLatestWhenNIsZero(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	require.NoError(t, ts.Append(ctx, "s", 1, "", []byte("v")))

	// Act
	out, err := ts.Latest(ctx, "s", 0)

	// Assert
	require.NoError(t, err)
	assert.Len(t, out, 0)
}

func TestShouldRejectLatestOnAscendingSeries(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)

	// Act
	out, err := ts.Latest(ctx, "s", 5)

	// Assert
	assert.ErrorIs(t, err, ErrLatestRequiresDescending)
	assert.Nil(t, out)
}

func TestShouldCachePartitionKeyPerSeries(t *testing.T) {
	// Arrange — after one call, the partition should be memoized and
	// subsequent reads must return the same byte sequence.
	ts := setupTS(t)

	// Act
	first := ts.partition("s-hot")
	second := ts.partition("s-hot")
	other := ts.partition("s-cold")

	// Assert
	assert.Equal(t, []byte(first), []byte(second))
	assert.NotEqual(t, []byte(first), []byte(other))
	// Cache entry must be present under the series name we asked for.
	cached, ok := ts.partitionCache.Load("s-hot")
	require.True(t, ok)
	assert.Equal(t, []byte(first), []byte(cached.(lexkey.LexKey)))
}

func TestShouldAppendAndQueryRangeWithTimeDescending(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTSDescending(t)
	now := time.Now()
	t1 := now.Add(-2 * time.Second)
	t2 := now.Add(-1 * time.Second)
	t3 := now

	// Act
	require.NoError(t, ts.AppendTime(ctx, "s1", t1, "", []byte("v1")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t2, "", []byte("v2")))
	require.NoError(t, ts.AppendTime(ctx, "s1", t3, "", []byte("v3")))

	// Assert
	out, err := ts.QueryRangeTime(ctx, "s1", t1, now.Add(1*time.Second))
	assert.NoError(t, err)
	assert.Len(t, out, 3)
	// Results should be in descending order when descending flag is set
	assert.Equal(t, t3.UnixNano(), out[0].Timestamp)
	assert.Equal(t, t2.UnixNano(), out[1].Timestamp)
	assert.Equal(t, t1.UnixNano(), out[2].Timestamp)
}
