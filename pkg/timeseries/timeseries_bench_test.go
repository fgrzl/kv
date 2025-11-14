package timeseries

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchTS creates a timeseries for benchmarks.
func setupBenchTS(b *testing.B) *TimeSeries {
	path := filepath.Join(b.TempDir(), "bench-ts")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	return New(store, "bench")
}

// BenchmarkAppend measures append performance.
func BenchmarkAppend(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ts.Append(ctx, "series1", int64(i), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryRange measures range query performance.
func BenchmarkQueryRange(b *testing.B) {
	ts := setupBenchTS(b)
	ctx := context.Background()

	// Pre-populate with 1000 samples
	for i := 0; i < 1000; i++ {
		if err := ts.Append(ctx, "series1", int64(i), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ts.QueryRange(ctx, "series1", 100, 200)
		if err != nil {
			b.Fatal(err)
		}
	}
}