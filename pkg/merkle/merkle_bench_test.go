package merkle

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchMerkle creates a merkle tree for benchmarks.
func setupBenchMerkle(b *testing.B) *Tree {
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

// BenchmarkBuildMerkleLarge measures merkle tree building performance for larger trees.
func BenchmarkBuildMerkleLarge(b *testing.B) {
	m := setupBenchMerkle(b)
	ctx := context.Background()

	leaves := benchLeaves(1000) // 1000 leaves

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stage := fmt.Sprintf("stage-%d", i)
		if err := m.Build(ctx, stage, "bench", leaves); err != nil {
			b.Fatal(err)
		}
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
