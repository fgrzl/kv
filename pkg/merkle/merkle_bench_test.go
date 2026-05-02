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
	return enumerators.Slice(benchLeafSlice(n))
}

func benchLeafSlice(n int) []Leaf {
	ls := make([]Leaf, n)
	for i := 0; i < n; i++ {
		ls[i] = leaf(fmt.Sprintf("leaf-%d", i))
	}
	return ls
}

func appendOnlyMutations(count int, prefix string) []LeafMutation {
	mutations := make([]LeafMutation, count)
	for i := range mutations {
		mutations[i] = LeafMutation{Append: true, Leaf: leaf(fmt.Sprintf("%s-%d", prefix, i))}
	}
	return mutations
}

func mixedMutations(existingCount, updateCount, removeCount, appendCount int, prefix string) []LeafMutation {
	mutations := make([]LeafMutation, 0, updateCount+removeCount+appendCount)
	stride := max(1, existingCount/max(1, updateCount+removeCount))
	index := 0

	for i := 0; i < updateCount; i++ {
		mutations = append(mutations, LeafMutation{
			Index: index,
			Leaf:  leaf(fmt.Sprintf("%s-update-%d", prefix, i)),
		})
		index += stride
	}
	for i := 0; i < removeCount; i++ {
		mutations = append(mutations, LeafMutation{
			Index:  index,
			Remove: true,
		})
		index += stride
	}
	for i := 0; i < appendCount; i++ {
		mutations = append(mutations, LeafMutation{
			Append: true,
			Leaf:   leaf(fmt.Sprintf("%s-append-%d", prefix, i)),
		})
	}

	return mutations
}

// BenchmarkBuildMerkle measures merkle tree building performance.
// Build streams all puts through a single kv.BatchChunks call (chunked by WithBatchSize / default).
func BenchmarkBuildMerkle(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	leaves := benchLeafSlice(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stage := fmt.Sprintf("stage-%d", i)
		if err := m.Build(ctx, stage, "bench", enumerators.Slice(leaves)); err != nil {
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
			leaves := benchLeafSlice(n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("stage-%d", i)
				if err := m.Build(ctx, stage, "bench", enumerators.Slice(leaves)); err != nil {
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

// BenchmarkUpdateLeaf_PebbleDepthScale keeps Pebble in the loop while showing how
// surgical update cost grows with tree depth.
func BenchmarkUpdateLeaf_PebbleDepthScale(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", size), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			stage := fmt.Sprintf("update-depth-%d", size)
			if err := m.Build(ctx, stage, "bench", benchLeaves(size)); err != nil {
				b.Fatal(err)
			}
			updatedLeaf := leaf(fmt.Sprintf("updated-%d", size))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % size
				if err := m.UpdateLeaf(ctx, stage, "bench", idx, updatedLeaf); err != nil {
					b.Fatal(err)
				}
			}
		})
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

// BenchmarkRemoveLeaf_PebbleDepthScale keeps Pebble in the loop while showing how
// soft-delete cost grows with tree depth.
func BenchmarkRemoveLeaf_PebbleDepthScale(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", size), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			stage := fmt.Sprintf("remove-depth-%d", size)
			if err := m.Build(ctx, stage, "bench", benchLeaves(size)); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % size
				if err := m.RemoveLeaf(ctx, stage, "bench", idx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDiffIdentical measures diff performance when trees are identical (fast path).
// Verifies root-hash short-circuiting and early-exit correctness.
func BenchmarkDiffIdentical(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	leaves := benchLeafSlice(5_000)
	if err := m.Build(ctx, "prev", "bench", enumerators.Slice(leaves)); err != nil {
		b.Fatal(err)
	}
	if err := m.Build(ctx, "curr", "bench", enumerators.Slice(leaves)); err != nil {
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

// BenchmarkApplyLeafMutations_Scale measures batched mutation performance at different tree sizes.
func BenchmarkApplyLeafMutations_Scale(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			mutations := []LeafMutation{
				{Index: n / 3, Leaf: leaf("updated")},
				{Index: n / 2, Remove: true},
				{Append: true, Leaf: leaf("append-a")},
				{Append: true, Leaf: leaf("append-b")},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("mut-stage-%d", i)

				b.StopTimer()
				if err := m.Build(ctx, stage, "bench", benchLeaves(n)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if _, err := m.ApplyLeafMutations(ctx, stage, "bench", mutations); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkApplyLeafMutations_AppendOnlyScale measures the append-only hot path at different tree sizes.
// This models projector batches that append a fixed number of leaves into a growing tree.
func BenchmarkApplyLeafMutations_AppendOnlyScale(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}
	const appendCount = 90

	for _, n := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			mutations := make([]LeafMutation, appendCount)
			for i := range mutations {
				mutations[i] = LeafMutation{Append: true, Leaf: leaf(fmt.Sprintf("append-%d", i))}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("append-stage-%d", i)

				b.StopTimer()
				if err := m.Build(ctx, stage, "bench", benchLeaves(n)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if _, err := m.ApplyLeafMutations(ctx, stage, "bench", mutations); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkApplyLeafMutations_AppendBatchTargets measures 1k append batches at the tree sizes
// used by the performance acceptance targets.
func BenchmarkApplyLeafMutations_AppendBatchTargets(b *testing.B) {
	sizes := []int{5_000, 50_000, 500_000}
	const appendCount = 1_000

	for _, n := range sizes {
		b.Run(fmt.Sprintf("existing=%d/appends=%d", n, appendCount), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			mutations := appendOnlyMutations(appendCount, "append-target")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("append-target-stage-%d", i)

				b.StopTimer()
				if err := m.Build(ctx, stage, "bench", benchLeaves(n)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if _, err := m.ApplyLeafMutations(ctx, stage, "bench", mutations); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkApplyLeafMutations_InitialLoad50k measures a 50k initial load applied as repeated
// 1k append-only batches against an initially empty tree.
func BenchmarkApplyLeafMutations_InitialLoad50k(b *testing.B) {
	const totalLeaves = 50_000
	const batchSize = 1_000
	const batchCount = totalLeaves / batchSize

	b.Run(fmt.Sprintf("total=%d/batch=%d", totalLeaves, batchSize), func(b *testing.B) {
		m := setupBenchMerkle(b)
		ctx := context.Background()
		batchMutations := make([][]LeafMutation, batchCount)
		for i := range batchMutations {
			batchMutations[i] = appendOnlyMutations(batchSize, fmt.Sprintf("initial-load-%d", i))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stage := fmt.Sprintf("initial-load-stage-%d", i)
			for _, mutations := range batchMutations {
				if _, err := m.ApplyLeafMutations(ctx, stage, "bench", mutations); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// BenchmarkMutationSession_InitialLoad50k measures a 50k initial load buffered through a single
// append-only mutation session and flushed once at the end.
func BenchmarkMutationSession_InitialLoad50k(b *testing.B) {
	const totalLeaves = 50_000
	const batchSize = 1_000
	const batchCount = totalLeaves / batchSize

	b.Run(fmt.Sprintf("total=%d/batch=%d", totalLeaves, batchSize), func(b *testing.B) {
		m := setupBenchMerkle(b)
		ctx := context.Background()
		batchMutations := make([][]LeafMutation, batchCount)
		for i := range batchMutations {
			batchMutations[i] = appendOnlyMutations(batchSize, fmt.Sprintf("session-load-%d", i))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stage := fmt.Sprintf("session-load-stage-%d", i)
			session, err := m.BeginMutationSession(ctx, stage, "bench")
			if err != nil {
				b.Fatal(err)
			}
			for _, mutations := range batchMutations {
				if _, err := session.QueueLeafMutations(mutations); err != nil {
					b.Fatal(err)
				}
			}
			if err := session.Flush(ctx); err != nil {
				b.Fatal(err)
			}
			session.Close()
		}
	})
}

// BenchmarkApplyLeafMutations_MixedBatchTargets measures 1k mixed mutation batches at the tree sizes
// used by the performance acceptance targets.
func BenchmarkApplyLeafMutations_MixedBatchTargets(b *testing.B) {
	sizes := []int{5_000, 50_000, 500_000}
	const updateCount = 400
	const removeCount = 300
	const appendCount = 300

	for _, n := range sizes {
		b.Run(fmt.Sprintf("existing=%d/mixed=%d", n, updateCount+removeCount+appendCount), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			mutations := mixedMutations(n, updateCount, removeCount, appendCount, "mixed-target")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stage := fmt.Sprintf("mixed-target-stage-%d", i)

				b.StopTimer()
				if err := m.Build(ctx, stage, "bench", benchLeaves(n)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if _, err := m.ApplyLeafMutations(ctx, stage, "bench", mutations); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetLeavesByIndex_Scale measures ordered batch leaf reads at different tree sizes.
func BenchmarkGetLeavesByIndex_Scale(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			m := setupBenchMerkle(b)
			ctx := context.Background()
			stage := fmt.Sprintf("read-stage-%d", n)

			if err := m.Build(ctx, stage, "bench", benchLeaves(n)); err != nil {
				b.Fatal(err)
			}

			indexes := []int{n - 1, 1, n / 2, 3, n / 3}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leaves, err := m.GetLeavesByIndex(ctx, stage, "bench", indexes)
				if err != nil {
					b.Fatal(err)
				}
				if len(leaves) != len(indexes) {
					b.Fatalf("expected %d leaves, got %d", len(indexes), len(leaves))
				}
			}
		})
	}
}
