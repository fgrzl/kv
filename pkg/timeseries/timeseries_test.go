package timeseries

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
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
	require.NoError(t, ts.Append(ctx, "s1", 1, []byte("v1")))
	require.NoError(t, ts.Append(ctx, "s1", 2, []byte("v2")))
	require.NoError(t, ts.Append(ctx, "s1", 3, []byte("v3")))

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
	require.NoError(t, ts.Append(ctx, "s2", 10, []byte("x")))
	require.NoError(t, ts.Append(ctx, "s2", 20, []byte("y")))

	// Act
	require.NoError(t, ts.DeleteSeries(ctx, "s2"))

	// Assert
	items, err := ts.QueryRange(ctx, "s2", 0, 100)
	assert.NoError(t, err)
	assert.Len(t, items, 0)
}

func TestShouldEnumerateRangeStream(t *testing.T) {
	// Arrange
	ctx := context.Background()
	ts := setupTS(t)
	require.NoError(t, ts.Append(ctx, "s4", 1, []byte("x")))
	require.NoError(t, ts.Append(ctx, "s4", 2, []byte("y")))
	require.NoError(t, ts.Append(ctx, "s4", 3, []byte("z")))

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
	require.NoError(t, ts.Append(ctx, "sd", 1, []byte("a")))
	require.NoError(t, ts.Append(ctx, "sd", 2, []byte("b")))
	require.NoError(t, ts.Append(ctx, "sd", 3, []byte("c")))

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
	require.NoError(t, ts.Append(ctx, "s", 1, []byte("first")))

	// Act
	err := ts.Append(ctx, "s", 1, []byte("second"))
	assert.NoError(t, err)

	// Assert
	// Assuming it overwrites, check the value
	out, err := ts.QueryRange(ctx, "s", 1, 2)
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, []byte("second"), out[0].Value)
}
