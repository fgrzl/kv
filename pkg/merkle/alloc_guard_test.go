package merkle

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// Allocation regression guards: these tests fail if allocations increase significantly.
// Thresholds are intentionally loose (~20% margin) to avoid flakiness while catching regressions.

// TestShouldNotExceedUpdateLeafAllocationBudget guards against allocation regressions in UpdateLeaf.
// Current baseline: ~403 allocs/op (after Phase 1 optimizations)
// Threshold: 480 allocs/op (20% margin)
func TestShouldNotExceedUpdateLeafAllocationBudget(t *testing.T) {
	const maxAllocs = 480 // 403 baseline + 20% margin

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	// Build 10K tree (same as benchmark)
	leaves := generateTestLeaves(10_000)
	if err := tree.Build(ctx, "blue", "testspace", leaves); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Measure UpdateLeaf allocations
	newLeaf := Leaf{
		Ref:  "updated-ref",
		Hash: ComputeHash([]byte("updated-data")),
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := tree.UpdateLeaf(ctx, "blue", "testspace", 5000, newLeaf); err != nil {
				b.Fatalf("UpdateLeaf failed: %v", err)
			}
		}
	})

	allocsPerOp := result.AllocsPerOp()
	if allocsPerOp > maxAllocs {
		t.Errorf("UpdateLeaf allocation regression: got %d allocs/op, expected ≤%d (baseline: 403)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("UpdateLeaf allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedAddLeafAllocationBudget guards against allocation regressions in AddLeaf.
// Current baseline: ~950 allocs/op (includes tree growth/recomputation when adding to existing tree)
// Threshold: 1150 allocs/op (20% margin)
// NOTE: AddLeaf can trigger full recomputation when tree height increases, causing higher allocations.
func TestShouldNotExceedAddLeafAllocationBudget(t *testing.T) {
	const maxAllocs = 1150 // 950 baseline + 20% margin

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	// Build initial tree (smaller for add operations to be stable)
	leaves := generateTestLeaves(1000)
	if err := tree.Build(ctx, "blue", "testspace", leaves); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Measure AddLeaf allocations
	newLeaf := Leaf{
		Ref:  "new-ref",
		Hash: ComputeHash([]byte("new-data")),
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		leafIndex := 1000
		for i := 0; i < b.N; i++ {
			_, err := tree.AddLeaf(ctx, "blue", "testspace", newLeaf)
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
			leafIndex++
		}
	})

	allocsPerOp := result.AllocsPerOp()
	if allocsPerOp > maxAllocs {
		t.Errorf("AddLeaf allocation regression: got %d allocs/op, expected ≤%d (baseline: 950)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("AddLeaf allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedDiffSparseChangeAllocationBudget guards against allocation regressions in Diff.
// Measures sparse diff performance (single leaf change in 100K tree).
// Current baseline: ~1500 allocs for single-leaf diff (empirical)
// Threshold: 1800 allocs (20% margin)
func TestShouldNotExceedDiffSparseChangeAllocationBudget(t *testing.T) {
	const maxAllocs = 1800 // 1500 baseline + 20% margin

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	// Build 100K tree in "prev" stage
	leaves := generateTestLeaves(100_000)
	if err := tree.Build(ctx, "prev", "diffspace", leaves); err != nil {
		t.Fatalf("Build prev failed: %v", err)
	}

	// Build identical tree in "curr" stage then update one leaf
	leavesForCurr := generateTestLeaves(100_000)
	if err := tree.Build(ctx, "curr", "diffspace", leavesForCurr); err != nil {
		t.Fatalf("Build curr failed: %v", err)
	}
	updatedLeaf := Leaf{
		Ref:  "updated-50000",
		Hash: ComputeHash([]byte("updated-data")),
	}
	if err := tree.UpdateLeaf(ctx, "curr", "diffspace", 50000, updatedLeaf); err != nil {
		t.Fatalf("UpdateLeaf failed: %v", err)
	}

	// Measure sparse diff allocations
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			diff := tree.Diff(ctx, "prev", "curr", "diffspace")
			count := 0
			for diff.MoveNext() {
				_, err := diff.Current()
				if err != nil {
					b.Fatalf("Diff enumeration failed: %v", err)
				}
				count++
			}
			if diff.Err() != nil {
				b.Fatalf("Diff error: %v", diff.Err())
			}
			if count != 1 {
				b.Fatalf("Expected 1 diff, got %d", count)
			}
			diff.Dispose()
		}
	})

	allocsPerOp := result.AllocsPerOp()
	if allocsPerOp > maxAllocs {
		t.Errorf("Diff allocation regression: got %d allocs/op, expected ≤%d (baseline: 1500)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("Diff sparse change allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// generateTestLeaves creates an enumerator of test leaves for benchmarking.
func generateTestLeaves(count int) *sliceLeafEnumerator {
	leaves := make([]Leaf, count)
	for i := 0; i < count; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		leaves[i] = Leaf{
			Ref:  fmt.Sprintf("ref-%d", i),
			Hash: ComputeHash(data),
		}
	}
	return &sliceLeafEnumerator{leaves: leaves, index: -1}
}

// sliceLeafEnumerator implements enumerators.Enumerator[Leaf] for testing.
type sliceLeafEnumerator struct {
	leaves  []Leaf
	index   int
	current Leaf
}

func (e *sliceLeafEnumerator) MoveNext() bool {
	e.index++
	if e.index >= len(e.leaves) {
		return false
	}
	e.current = e.leaves[e.index]
	return true
}

func (e *sliceLeafEnumerator) Current() (Leaf, error) {
	return e.current, nil
}

func (e *sliceLeafEnumerator) Err() error {
	return nil
}

func (e *sliceLeafEnumerator) Dispose() {
	// No cleanup needed
}
