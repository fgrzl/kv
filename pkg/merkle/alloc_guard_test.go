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
// Current baseline: ~324 allocs/op (single-partition row keys with partition-aware helpers)
// Threshold: 390 allocs/op (~20% margin over baseline)
func TestShouldNotExceedUpdateLeafAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 390

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
		t.Errorf("UpdateLeaf allocation regression: got %d allocs/op, expected ≤%d (baseline: 324)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("UpdateLeaf allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedAddLeafAllocationBudget guards against allocation regressions in AddLeaf.
// Current baseline: ~261 allocs/op (single-partition keys with partition-aware helpers)
// Threshold: 340 allocs/op
// NOTE: AddLeaf can trigger full recomputation when tree height increases, causing higher allocations.
func TestShouldNotExceedAddLeafAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 340

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
		t.Errorf("AddLeaf allocation regression: got %d allocs/op, expected ≤%d (baseline: 261)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("AddLeaf allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedDiffSparseChangeAllocationBudget guards against allocation regressions in Diff.
// Measures sparse diff performance (single leaf change in 100K tree).
// Current baseline: ~1780 allocs/op (single-partition row keys)
// Threshold: 2200 allocs/op
func TestShouldNotExceedDiffSparseChangeAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 2200

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
		t.Errorf("Diff allocation regression: got %d allocs/op, expected ≤%d (baseline: 1780)",
			allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("Diff sparse change allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedApplyLeafMutationsAllocationBudget guards against allocation regressions in ApplyLeafMutations.
// Measures a stable mixed batch on a 10K tree without height growth.
func TestShouldNotExceedApplyLeafMutationsAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 2000

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	leaves := generateTestLeaves(10_000)
	if err := tree.Build(ctx, "blue", "testspace", leaves); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	mutations := []LeafMutation{
		{Index: 1234, Leaf: leaf("updated-1234")},
		{Index: 2345, Remove: true},
		{Index: 3456, Leaf: leaf("updated-3456")},
		{Index: 4567, Remove: true},
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := tree.ApplyLeafMutations(ctx, "blue", "testspace", mutations); err != nil {
				b.Fatalf("ApplyLeafMutations failed: %v", err)
			}
		}
	})

	allocsPerOp := result.AllocsPerOp()
	if allocsPerOp > maxAllocs {
		t.Errorf("ApplyLeafMutations allocation regression: got %d allocs/op, expected ≤%d", allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("ApplyLeafMutations allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

// TestShouldNotExceedAppendOnlyApplyLeafMutationsAllocationBudget guards against allocation regressions
// in the append-only ApplyLeafMutations path that models bulk projector appends.
func TestShouldNotExceedAppendOnlyApplyLeafMutationsAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 2200
	const appendCount = 90
	const measuredRuns = 25

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	leaves := generateTestLeaves(10_000)
	if err := tree.Build(ctx, "blue", "testspace", leaves); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	mutations := make([]LeafMutation, appendCount)
	for i := range mutations {
		mutations[i] = LeafMutation{Append: true, Leaf: leaf(fmt.Sprintf("append-%d", i))}
	}

	stages := make([]string, measuredRuns)
	for i := range stages {
		stage := fmt.Sprintf("blue-%d", i)
		stages[i] = stage
		if err := tree.Build(ctx, stage, "testspace", generateTestLeaves(10_000)); err != nil {
			t.Fatalf("Build failed: %v", err)
		}
	}

	runIndex := 0
	allocsPerOp := testing.AllocsPerRun(measuredRuns, func() {
		stage := stages[runIndex%len(stages)]
		runIndex++
		if _, err := tree.ApplyLeafMutations(ctx, stage, "testspace", mutations); err != nil {
			t.Fatalf("ApplyLeafMutations failed: %v", err)
		}
	})
	allocsPerOpInt := int64(allocsPerOp)
	if allocsPerOpInt > maxAllocs {
		t.Errorf("append-only ApplyLeafMutations allocation regression: got %d allocs/op, expected ≤%d", allocsPerOpInt, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("Append-only ApplyLeafMutations allocations: %d allocs/op (threshold: %d)", allocsPerOpInt, maxAllocs)
}

// TestShouldNotExceedMixedApplyLeafMutationsAllocationBudget guards the mixed in-memory recompute path.
// Measures a 1k mutation batch on a 50k tree, excluding setup/build work from the measurement.
func TestShouldNotExceedMixedApplyLeafMutationsAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 100_000
	const measuredRuns = 12
	const existingCount = 50_000
	const updateCount = 400
	const removeCount = 300
	const appendCount = 300

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	stages := make([]string, measuredRuns)
	for i := range stages {
		stage := fmt.Sprintf("mixed-%d", i)
		stages[i] = stage
		if err := tree.Build(ctx, stage, "testspace", generateTestLeaves(existingCount)); err != nil {
			t.Fatalf("Build failed: %v", err)
		}
	}

	mutations := make([]LeafMutation, 0, updateCount+removeCount+appendCount)
	stride := existingCount / (updateCount + removeCount)
	index := 0
	for i := 0; i < updateCount; i++ {
		mutations = append(mutations, LeafMutation{Index: index, Leaf: leaf(fmt.Sprintf("mixed-update-%d", i))})
		index += stride
	}
	for i := 0; i < removeCount; i++ {
		mutations = append(mutations, LeafMutation{Index: index, Remove: true})
		index += stride
	}
	for i := 0; i < appendCount; i++ {
		mutations = append(mutations, LeafMutation{Append: true, Leaf: leaf(fmt.Sprintf("mixed-append-%d", i))})
	}

	runIndex := 0
	allocsPerOp := testing.AllocsPerRun(measuredRuns, func() {
		stage := stages[runIndex%len(stages)]
		runIndex++
		if _, err := tree.ApplyLeafMutations(ctx, stage, "testspace", mutations); err != nil {
			t.Fatalf("ApplyLeafMutations failed: %v", err)
		}
	})
	allocsPerOpInt := int64(allocsPerOp)
	if allocsPerOpInt > maxAllocs {
		t.Errorf("mixed ApplyLeafMutations allocation regression: got %d allocs/op, expected ≤%d", allocsPerOpInt, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("Mixed ApplyLeafMutations allocations: %d allocs/op (threshold: %d)", allocsPerOpInt, maxAllocs)
}

// TestShouldNotExceedGetLeavesByIndexAllocationBudget guards against allocation regressions in GetLeavesByIndex.
// Measures ordered batch reads on a 100K tree.
func TestShouldNotExceedGetLeavesByIndexAllocationBudget(t *testing.T) {
	skipAllocationGuardUnderRace(t)

	const maxAllocs = 220

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	tree := NewTree(store)

	leaves := generateTestLeaves(100_000)
	if err := tree.Build(ctx, "blue", "testspace", leaves); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	indexes := []int{99_999, 1, 50_000, 3, 33_333}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := tree.GetLeavesByIndex(ctx, "blue", "testspace", indexes)
			if err != nil {
				b.Fatalf("GetLeavesByIndex failed: %v", err)
			}
			if len(result) != len(indexes) {
				b.Fatalf("expected %d leaves, got %d", len(indexes), len(result))
			}
		}
	})

	allocsPerOp := result.AllocsPerOp()
	if allocsPerOp > maxAllocs {
		t.Errorf("GetLeavesByIndex allocation regression: got %d allocs/op, expected ≤%d", allocsPerOp, maxAllocs)
		t.Errorf("This indicates a performance regression. Review recent changes or update threshold if intentional.")
	}

	t.Logf("GetLeavesByIndex allocations: %d allocs/op (threshold: %d)", allocsPerOp, maxAllocs)
}

func skipAllocationGuardUnderRace(t *testing.T) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("allocation budgets are not meaningful under -race instrumentation")
	}
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
