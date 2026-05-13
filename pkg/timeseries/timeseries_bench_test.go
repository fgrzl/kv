package timeseries

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchTS creates a timeseries for benchmarks.
func setupBenchTS(b *testing.B) *TimeSeries {
	path := filepath.Join(b.TempDir(), "bench-ts")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close store: %v", err)
		}
	})
	return New(store, "bench")
}

// BenchmarkAppend measures append performance.
func BenchmarkAppend(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()
	value := []byte("value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ts.Append(ctx, "series1", int64(i), "", value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendWithID measures append performance when events carry a
// per-event id suffix.
func BenchmarkAppendWithID(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()
	value := []byte("value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ts.Append(ctx, "series1", int64(i), "evt", value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendBatch measures throughput of batch writes grouped by
// series. This is the Azure-optimal write path: one round trip per
// partition per 100-op chunk.
func BenchmarkAppendBatch(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()
	value := []byte("value")
	const batchSize = 100

	batch := make([]Entry, batchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := int64(i) * batchSize
		for j := 0; j < batchSize; j++ {
			batch[j] = Entry{Series: "series1", Timestamp: base + int64(j), Value: value}
		}
		if err := ts.AppendBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLatest measures the hot "most recent N" path on a descending
// series. On Azure this is a single `PartitionKey eq` + `$top=N` query.
func BenchmarkLatest(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-latest")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close store: %v", err)
		}
	})
	ts := New(store, "bench", WithDescending(true))
	ctx := context.Background()
	value := []byte("value")

	for i := 0; i < 10_000; i++ {
		if err := ts.Append(ctx, "series1", int64(i), "", value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.Latest(ctx, "series1", 10); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryRange measures range query performance, exercising the hot
// read path (row-key decode for every returned sample, no JSON).
func BenchmarkQueryRange(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()
	value := []byte("value")

	for i := 0; i < 1000; i++ {
		if err := ts.Append(ctx, "series1", int64(i), "", value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ts.QueryRange(ctx, "series1", 100, 200)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnumerateRange measures streaming enumeration (no intermediate
// slice allocation beyond the per-sample id string).
func BenchmarkEnumerateRange(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()
	value := []byte("value")

	for i := 0; i < 1000; i++ {
		if err := ts.Append(ctx, "series1", int64(i), "", value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		err := enumerators.ForEach(ts.EnumerateRange(ctx, "series1", 100, 200), func(Sample) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
