package merkle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchMerkle creates a merkle tree for benchmarks.
func setupBenchMerkle(b *testing.B) *Tree {
	// Suppress all logging during benchmarks to avoid skewing performance measurements
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	path := filepath.Join(b.TempDir(), "bench-merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	return NewTree(store)
}

func benchLeaves(n int) enumerators.Enumerator[Leaf] {
	ls := make([]Leaf, n)
	for i := 0; i < n; i++ {
		ls[i] = leaf(fmt.Sprintf("leaf-%d", i))
	}
	return enumerators.Slice(ls)
}

// BenchmarkBuildMerkle measures merkle tree building performance.
// Build streams all puts through a single kv.BatchChunks call (chunked by WithBatchSize / default).
func BenchmarkBuildMerkle(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	leaves := benchLeaves(100) // 100 leaves

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stage := fmt.Sprintf("stage-%d", i)
		if err := m.Build(ctx, stage, "bench", leaves); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildMerkle_Scale measures merkle tree building performance at different scales.
// This replaces ad-hoc size choices with a proper scaling matrix.
func BenchmarkBuildMerkle_Scale(b *testing.B) {
	sizes := []int{100, 1_000, 10_000, 100_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			leaves := benchLeaves(n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("stage-%d", i)
				if err := m.Build(ctx, stage, "bench", leaves); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetRootHash measures root hash retrieval performance.
func BenchmarkGetRootHash(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	// Pre-build tree
	if err := m.Build(ctx, "bench-stage", "bench", benchLeaves(100)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := m.GetRootHash(ctx, "bench-stage", "bench")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdateLeaf measures incremental update performance (hot path).
// CRITICAL: This benchmarks the O(log N) guarantee for surgical updates.
// Any regression here indicates accidental full recomputation.
func BenchmarkUpdateLeaf(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 10_000
	if err := m.Build(ctx, "bench-stage", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}

	newLeaf := leaf("updated-leaf")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % leafCount
		if err := m.UpdateLeaf(ctx, "bench-stage", "bench", idx, newLeaf); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddLeaf measures append performance and tree growth behavior.
// Tests padding logic correctness and structural growth costs.
func BenchmarkAddLeaf(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	if err := m.Build(ctx, "bench-stage", "bench", benchLeaves(1)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.AddLeaf(ctx, "bench-stage", "bench", leaf(fmt.Sprintf("new-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddLeafStableHeight measures the common append path where tree height does not change.
// This isolates the hot O(log N) recomputation path from the rare full-rebuild case.
func BenchmarkAddLeafStableHeight(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 1_000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stage := fmt.Sprintf("stable-stage-%d", i)

		b.StopTimer()
		if err := m.Build(ctx, stage, "bench", benchLeaves(leafCount)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, err := m.AddLeaf(ctx, stage, "bench", leaf(fmt.Sprintf("stable-new-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddLeafHeightGrowth measures the rare append path where adding a leaf grows tree height.
// For branching=2, appending to 1024 leaves forces the next level to be created.
func BenchmarkAddLeafHeightGrowth(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 1_024

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stage := fmt.Sprintf("growth-stage-%d", i)

		b.StopTimer()
		if err := m.Build(ctx, stage, "bench", benchLeaves(leafCount)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, err := m.AddLeaf(ctx, stage, "bench", leaf(fmt.Sprintf("growth-new-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRemoveLeaf measures soft delete performance.
// Verifies that delete marker behavior has no reindexing cost.
func BenchmarkRemoveLeaf(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 10_000
	if err := m.Build(ctx, "bench-stage", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % leafCount
		if err := m.RemoveLeaf(ctx, "bench-stage", "bench", idx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffIdentical measures diff performance when trees are identical (fast path).
// Verifies root-hash short-circuiting and early-exit correctness.
func BenchmarkDiffIdentical(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	leaves := benchLeaves(5_000)
	if err := m.Build(ctx, "prev", "bench", leaves); err != nil {
		b.Fatal(err)
	}
	if err := m.Build(ctx, "curr", "bench", leaves); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := m.Diff(ctx, "prev", "curr", "bench")
		if err := enumerators.ForEach(it, func(Leaf) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffSparseChange measures diff performance with minimal changes (real ingestion case).
// CRITICAL: Verifies logarithmic diff descent - no accidental full scans.
func BenchmarkDiffSparseChange(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 10_000
	if err := m.Build(ctx, "prev", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}
	if err := m.Build(ctx, "curr", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}

	// Mutate a single leaf to create sparse diff
	if err := m.UpdateLeaf(ctx, "curr", "bench", 1234, leaf("changed")); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := m.Diff(ctx, "prev", "curr", "bench")
		if err := enumerators.ForEach(it, func(Leaf) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffSparseChange_100k measures diff performance at scale (production ingestion case).
// CRITICAL: This is the number you quote when justifying the system.
// Validates O(log N) diff descent at 100k leaves with 1 change.
func BenchmarkDiffSparseChange_100k(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	const leafCount = 100_000
	if err := m.Build(ctx, "prev", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}
	if err := m.Build(ctx, "curr", "bench", benchLeaves(leafCount)); err != nil {
		b.Fatal(err)
	}

	// Mutate a single leaf to create sparse diff
	if err := m.UpdateLeaf(ctx, "curr", "bench", 12345, leaf("changed")); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := m.Diff(ctx, "prev", "curr", "bench")
		if err := enumerators.ForEach(it, func(Leaf) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
