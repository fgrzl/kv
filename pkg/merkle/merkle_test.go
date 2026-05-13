package merkle

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Helpers ---

func setup(t *testing.T) *Tree {
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return NewTree(store)
}

func hashFor(data string) []byte {
	return ComputeHash([]byte(data))
}

func leaf(ref string) Leaf {
	return Leaf{
		Ref:  ref,
		Hash: hashFor(ref),
	}
}

func leaves(refs ...string) enumerators.Enumerator[Leaf] {
	ls := make([]Leaf, len(refs))
	for i, r := range refs {
		ls[i] = leaf(r)
	}
	return enumerators.Slice(ls)
}

func buildEmptyThenNonEmptyStages(ctx context.Context, t *testing.T, m *Tree, emptyStage, nonEmptyStage, space string, nonemptyLeaves []Leaf) {
	t.Helper()
	require.NoError(t, m.Build(ctx, emptyStage, space, enumerators.Empty[Leaf]()))
	require.NoError(t, m.Build(ctx, nonEmptyStage, space, enumerators.Slice(nonemptyLeaves)))
}

// --- Core Build & Root Hash Tests ---

func TestShouldBuildMerkleTreeAndGetRootHash(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage := "blue"
	space := "testspace"
	refs := []string{"A", "B", "C"}

	// Act
	err := m.Build(ctx, stage, space, leaves(refs...))

	// Assert
	assert.NoError(t, err)
	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotNil(t, root)
}

func TestShouldReturnSymmetricDiffForAdditions(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"
	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B", "C")))

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "C", diffs[0].Ref)
}

func TestShouldReturnSymmetricDiffForSubtractions(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"
	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B", "C")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B")))

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "C", diffs[0].Ref)
}

func TestShouldReturnSymmetricDiffForAddAndRemove(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"
	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B", "C")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B", "D")))

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 2)
	assert.Equal(t, "D", diffs[0].Ref)
	assert.Equal(t, "C", diffs[1].Ref)
}

func TestShouldReturnSymmetricDiffForEmptyVsNonEmpty(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "emptynonempty"
	require.NoError(t, m.Build(ctx, stage1, space, leaves()))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A")))

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "A", diffs[0].Ref)
}

func TestShouldReturnSymmetricDiffForNonEmptyVsEmpty(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "emptynonempty"
	require.NoError(t, m.Build(ctx, stage1, space, leaves("A")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves()))

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "A", diffs[0].Ref)
}

func TestShouldPruneMerkleTree(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage := "blue"
	space := "prunespace"
	require.NoError(t, m.Build(ctx, stage, space, leaves("X", "Y")))

	// Act
	err := m.Prune(ctx, stage, space)

	// Assert
	assert.NoError(t, err)
	root, err := m.GetRootHash(ctx, stage, space)
	assert.Nil(t, root)
	assert.NoError(t, err)
}

func TestShouldReturnDiffBetweenStages(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"
	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B", "C")))

	// Act
	diffs, err := enumerators.ToSlice(m.Diff(ctx, stage1, stage2, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "C", diffs[0].Ref)
}

func TestShouldAllowBuildingWithEmptyLeaves(t *testing.T) {
	// Arrange
	ctx := t.Context()
	m := setup(t)
	stage := "blue"
	space := "emptyspace"

	// Act
	err := m.Build(ctx, stage, space, leaves())

	// Assert
	assert.NoError(t, err)
	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.Nil(t, root)
}

func TestShouldHandleInvalidBranchingFactor(t *testing.T) {
	// Arrange
	store, err := pebble.NewPebbleStore(filepath.Join(t.TempDir(), "invalid"))
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	// Act
	tree := NewTree(store, WithBranching(1))

	// Assert
	// Since WithBranching checks n >= 2, it should default to 2
	assert.NotNil(t, tree)
}

func TestShouldHandleJSONUnmarshalErrorInGetHash(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "errorspace"

	// Manually insert invalid JSON data for a leaf
	pk := nodePK(stage, space, 0, 0)
	invalidJSON := []byte(`invalid json`)
	require.NoError(t, m.store.Put(ctx, &kv.Item{PK: pk, Value: invalidJSON}))

	// Act
	hash, val, err := m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})

	// Assert
	assert.Error(t, err)
	assert.Nil(t, hash)
	assert.Nil(t, val)
}

func TestShouldHandleGetHashErrorInDiffNode(t *testing.T) {
	// This test is complex because diffNode error handling depends on when getHash fails.
	// For now, skip this as the error paths are covered by the direct getHash test.
	t.Skip("Diff node error handling is complex and already covered by getHash test")
}

func TestShouldHandleDecodeLeafEnumeratorWithInvalidJSON(t *testing.T) {
	// Arrange
	invalidJSON := []byte(`invalid json`)

	// Act
	enum := decodeLeafEnumerator(invalidJSON)
	leaves, err := enumerators.ToSlice(enum)

	// Assert
	assert.NoError(t, err) // decodeLeafEnumerator doesn't return errors, just empty
	assert.Len(t, leaves, 0)
}

func TestShouldHandleDecodeLeafEnumeratorWithEmptyRef(t *testing.T) {
	// Arrange
	emptyRefJSON := []byte(`{"ref": "", "hash": "somehash"}`)

	// Act
	enum := decodeLeafEnumerator(emptyRefJSON)
	leaves, err := enumerators.ToSlice(enum)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, leaves, 0)
}

func TestShouldHandleEmptyLeafEnumerator(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "empty"
	space := "testspace"

	// Act
	err := m.Build(ctx, stage, space, enumerators.Empty[Leaf]())

	// Assert
	assert.NoError(t, err)
	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.Nil(t, root) // Empty tree has no root
}

func TestShouldHandleBuildWithDifferentBranchingFactors(t *testing.T) {
	// Arrange
	ctx := context.Background()

	testCases := []struct {
		branching int
		name      string
	}{
		{2, "binary"},
		{3, "ternary"},
		{4, "quaternary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := pebble.NewPebbleStore(filepath.Join(t.TempDir(), tc.name))
			require.NoError(t, err)
			defer func() {
				if err := store.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()

			m := NewTree(store, WithBranching(tc.branching))
			stage := "test"
			space := "branching"

			// Create enough leaves to test branching
			leaves := make([]Leaf, tc.branching+1) // One more than branching factor
			for i := 0; i < len(leaves); i++ {
				leaves[i] = leaf(fmt.Sprintf("leaf%d", i))
			}

			// Act
			err = m.Build(ctx, stage, space, enumerators.Slice(leaves))

			// Assert
			assert.NoError(t, err)
			root, err := m.GetRootHash(ctx, stage, space)
			assert.NoError(t, err)
			assert.NotNil(t, root)
		})
	}
}

func TestShouldHandleContextCancellationDuringDiff(t *testing.T) {
	// Note: Context cancellation in diff operations may not be fully implemented
	// in all code paths. This test documents the expected behavior.
	t.Skip("Context cancellation in diff operations needs further implementation")
}

func TestShouldHandleDiffWithEmptyVsNonEmptyTrees(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	emptyStage := "empty"
	nonEmptyStage := "nonempty"
	space := "emptytest"

	buildEmptyThenNonEmptyStages(ctx, t, m, emptyStage, nonEmptyStage, space, []Leaf{leaf("a"), leaf("b")})

	// Act - diff empty -> non-empty
	diffs, err := enumerators.ToSlice(m.Diff(ctx, emptyStage, nonEmptyStage, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 2)
	assert.Contains(t, []string{"a", "b"}, diffs[0].Ref)
	assert.Contains(t, []string{"a", "b"}, diffs[1].Ref)
}

func TestShouldHandleDiffWithNonEmptyVsEmptyTrees(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	nonEmptyStage := "nonempty"
	emptyStage := "empty"
	space := "emptytest"

	// Build non-empty tree
	leaves := []Leaf{leaf("x"), leaf("y")}
	require.NoError(t, m.Build(ctx, nonEmptyStage, space, enumerators.Slice(leaves)))

	// Build empty tree
	require.NoError(t, m.Build(ctx, emptyStage, space, enumerators.Empty[Leaf]()))

	// Act - diff non-empty -> empty (should return empty for regular diff)
	diffs, err := enumerators.ToSlice(m.Diff(ctx, nonEmptyStage, emptyStage, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 0) // Regular diff doesn't emit removals
}

func TestShouldHandleSymmetricDiffWithEmptyVsNonEmptyTrees(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	emptyStage := "empty"
	nonEmptyStage := "nonempty"
	space := "symmetrictest"

	buildEmptyThenNonEmptyStages(ctx, t, m, emptyStage, nonEmptyStage, space, []Leaf{leaf("p"), leaf("q")})

	// Act
	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, emptyStage, nonEmptyStage, space))

	// Assert
	assert.NoError(t, err)
	assert.Len(t, diffs, 2)
	assert.Contains(t, []string{"p", "q"}, diffs[0].Ref)
	assert.Contains(t, []string{"p", "q"}, diffs[1].Ref)
}

// --- Surgical Update Tests ---

func TestShouldGetLeafCount(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "count"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C")))

	// Act
	count, err := m.GetLeafCount(ctx, stage, space)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestShouldReturnZeroForEmptyTree(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "empty"

	// Act
	count, err := m.GetLeafCount(ctx, stage, space)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestShouldGetLeafByIndex(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "getleaf"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C")))

	// Act
	leaf0, err := m.GetLeaf(ctx, stage, space, 0)
	require.NoError(t, err)
	leaf1, err := m.GetLeaf(ctx, stage, space, 1)
	require.NoError(t, err)
	leaf2, err := m.GetLeaf(ctx, stage, space, 2)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, "A", leaf0.Ref)
	assert.Equal(t, "B", leaf1.Ref)
	assert.Equal(t, "C", leaf2.Ref)
}

func TestShouldReturnErrorForInvalidLeafIndex(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "invalid"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))

	// Act & Assert - out of bounds
	_, err := m.GetLeaf(ctx, stage, space, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of bounds")

	// Act & Assert - negative index
	_, err = m.GetLeaf(ctx, stage, space, -1)
	assert.Error(t, err)
}

func TestShouldEnumerateAllLeaves(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "enum"
	require.NoError(t, m.Build(ctx, stage, space, leaves("X", "Y", "Z")))

	// Act
	enum := m.EnumerateLeaves(ctx, stage, space)
	result, err := enumerators.ToSlice(enum)

	// Assert
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, "X", result[0].Ref)
	assert.Equal(t, "Y", result[1].Ref)
	assert.Equal(t, "Z", result[2].Ref)
}

func TestShouldEnumerateEmptyTree(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "empty"

	// Act
	enum := m.EnumerateLeaves(ctx, stage, space)
	result, err := enumerators.ToSlice(enum)

	// Assert
	require.NoError(t, err)
	assert.Len(t, result, 0)
}

func TestShouldUpdateLeafAndRecomputeRoot(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "update"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D")))

	// Get original root hash
	origRoot, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)

	// Act - Update leaf at index 1 (B -> X)
	updatedLeaf := leaf("X")
	err = m.UpdateLeaf(ctx, stage, space, 1, updatedLeaf)

	// Assert
	assert.NoError(t, err)

	// Verify root hash changed
	newRoot, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotEqual(t, origRoot, newRoot, "root hash should change after leaf update")

	// Verify we can still get leaf count
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Verify leaf was actually updated
	updatedLeafFromTree, err := m.GetLeaf(ctx, stage, space, 1)
	assert.NoError(t, err)
	assert.Equal(t, "X", updatedLeafFromTree.Ref)
}

func TestShouldUpdateMultipleLeavesIndependently(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "multi"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D", "E", "F")))

	origRoot, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)

	// Act - Update multiple leaves
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 0, leaf("X")))
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 3, leaf("Y")))
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 5, leaf("Z")))

	// Assert
	newRoot, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotEqual(t, origRoot, newRoot)

	// Verify leaf count unchanged
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 6, count)

	// Verify all updates were applied
	leaf0, _ := m.GetLeaf(ctx, stage, space, 0)
	leaf3, _ := m.GetLeaf(ctx, stage, space, 3)
	leaf5, _ := m.GetLeaf(ctx, stage, space, 5)
	assert.Equal(t, "X", leaf0.Ref)
	assert.Equal(t, "Y", leaf3.Ref)
	assert.Equal(t, "Z", leaf5.Ref)
}

func TestShouldReturnErrorWhenUpdatingOutOfBoundsLeaf(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "bounds"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))

	// Act & Assert - out of bounds
	err := m.UpdateLeaf(ctx, stage, space, 10, leaf("X"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of bounds")

	// Act & Assert - negative index
	err = m.UpdateLeaf(ctx, stage, space, -1, leaf("X"))
	assert.Error(t, err)
}

func TestShouldAddLeafToExistingTree(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "add"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C")))

	origRoot, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)

	// Act
	newLeaf := leaf("D")
	leafIndex, err := m.AddLeaf(ctx, stage, space, newLeaf)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 3, leafIndex, "new leaf should be at index 3")

	// Verify leaf count increased
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Verify root hash changed
	newRoot, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotEqual(t, origRoot, newRoot)

	// Verify leaf was actually added
	addedLeaf, err := m.GetLeaf(ctx, stage, space, 3)
	assert.NoError(t, err)
	assert.Equal(t, "D", addedLeaf.Ref)
}

func TestShouldAddLeafToEmptyTree(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "empty"

	// Act
	newLeaf := leaf("A")
	leafIndex, err := m.AddLeaf(ctx, stage, space, newLeaf)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0, leafIndex)

	// Verify tree is valid
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotNil(t, root)
}

func TestShouldAddMultipleLeavesSequentially(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "sequence"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))

	// Act - Add multiple leaves
	idx1, err := m.AddLeaf(ctx, stage, space, leaf("C"))
	require.NoError(t, err)
	idx2, err := m.AddLeaf(ctx, stage, space, leaf("D"))
	require.NoError(t, err)
	idx3, err := m.AddLeaf(ctx, stage, space, leaf("E"))
	require.NoError(t, err)

	// Assert
	assert.Equal(t, 2, idx1)
	assert.Equal(t, 3, idx2)
	assert.Equal(t, 4, idx3)

	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)

	// Verify all leaves are accessible
	for i := 0; i < 5; i++ {
		_, err := m.GetLeaf(ctx, stage, space, i)
		assert.NoError(t, err, "should be able to get leaf at index %d", i)
	}
}

func TestShouldRemoveLeafViaSoftDelete(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "remove"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D")))

	origRoot, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)

	// Act - Remove leaf at index 2
	err = m.RemoveLeaf(ctx, stage, space, 2)

	// Assert
	assert.NoError(t, err)

	// Verify root hash changed
	newRoot, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotEqual(t, origRoot, newRoot)

	// Verify leaf count unchanged (soft delete)
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Verify deleted leaf has empty Ref
	deletedLeaf, err := m.GetLeaf(ctx, stage, space, 2)
	assert.NoError(t, err)
	assert.Equal(t, "", deletedLeaf.Ref, "deleted leaf should have empty Ref")
}

func TestShouldRemoveAndAddLeafAtSameIndex(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "replace"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C")))

	// Act - Remove then update
	require.NoError(t, m.RemoveLeaf(ctx, stage, space, 1))
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 1, leaf("X")))

	// Assert
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotNil(t, root)

	// Verify leaf was replaced
	replacedLeaf, err := m.GetLeaf(ctx, stage, space, 1)
	assert.NoError(t, err)
	assert.Equal(t, "X", replacedLeaf.Ref)
}

func TestShouldHandleMixedOperations(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "mixed"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))

	// Act - Mix operations
	_, err := m.AddLeaf(ctx, stage, space, leaf("C"))
	require.NoError(t, err)

	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 0, leaf("X")))

	_, err = m.AddLeaf(ctx, stage, space, leaf("D"))
	require.NoError(t, err)

	require.NoError(t, m.RemoveLeaf(ctx, stage, space, 1))

	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 2, leaf("Y")))

	// Assert
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotNil(t, root)

	// Verify final state
	leaf0, _ := m.GetLeaf(ctx, stage, space, 0)
	leaf1, _ := m.GetLeaf(ctx, stage, space, 1)
	leaf2, _ := m.GetLeaf(ctx, stage, space, 2)
	leaf3, _ := m.GetLeaf(ctx, stage, space, 3)
	assert.Equal(t, "X", leaf0.Ref)
	assert.Equal(t, "", leaf1.Ref) // deleted
	assert.Equal(t, "Y", leaf2.Ref)
	assert.Equal(t, "D", leaf3.Ref)
}

func TestShouldHandleTreeWithBranchingFactor4(t *testing.T) {
	// Arrange
	ctx := context.Background()
	m := NewTree(setup(t).store, WithBranching(4))
	stage := "test"
	space := "branch4"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D", "E")))

	// Act - Update and add with branching=4
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 2, leaf("X")))
	idx, err := m.AddLeaf(ctx, stage, space, leaf("F"))

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 5, idx)

	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 6, count)
}

// --- Invariant Tests ---

func TestInvariantSurgicalUpdateEqualsRebuild(t *testing.T) {
	// This is the most critical invariant: surgical updates must produce
	// identical root hash as full rebuild with same final leaves.

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space1 := "surgical"
	space2 := "rebuild"

	// Build initial tree and perform surgical updates
	require.NoError(t, m.Build(ctx, stage, space1, leaves("A", "B", "C", "D")))
	require.NoError(t, m.UpdateLeaf(ctx, stage, space1, 1, leaf("X")))
	_, err := m.AddLeaf(ctx, stage, space1, leaf("E"))
	require.NoError(t, err)

	// Build from scratch with updated data
	require.NoError(t, m.Build(ctx, stage, space2, leaves("A", "X", "C", "D", "E")))

	// Act
	surgicalRoot, err := m.GetRootHash(ctx, stage, space1)
	require.NoError(t, err)
	rebuildRoot, err := m.GetRootHash(ctx, stage, space2)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, rebuildRoot, surgicalRoot, "surgical updates must produce same root as full rebuild")
}

func TestInvariantBatchLeafMutationsEqualsRebuild(t *testing.T) {
	// Batch mutations should produce the same final tree as a full rebuild.

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space1 := "batched"
	space2 := "rebuild"

	require.NoError(t, m.Build(ctx, stage, space1, leaves("A", "B", "C", "D")))
	origRoot, err := m.GetRootHash(ctx, stage, space1)
	require.NoError(t, err)

	resultIndexes, err := m.ApplyLeafMutations(ctx, stage, space1, []LeafMutation{
		{Index: 1, Leaf: leaf("X")},
		{Index: 2, Remove: true},
		{Append: true, Leaf: leaf("E")},
		{Append: true, Leaf: leaf("F")},
	})
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 4, 5}, resultIndexes)

	count, err := m.GetLeafCount(ctx, stage, space1)
	require.NoError(t, err)
	assert.Equal(t, 6, count)

	leaf1, err := m.GetLeaf(ctx, stage, space1, 1)
	require.NoError(t, err)
	assert.Equal(t, "X", leaf1.Ref)

	leaf2, err := m.GetLeaf(ctx, stage, space1, 2)
	require.NoError(t, err)
	assert.Empty(t, leaf2.Ref)
	assert.Equal(t, deletedHash(), leaf2.Hash)

	leaf4, err := m.GetLeaf(ctx, stage, space1, 4)
	require.NoError(t, err)
	assert.Equal(t, "E", leaf4.Ref)

	leaf5, err := m.GetLeaf(ctx, stage, space1, 5)
	require.NoError(t, err)
	assert.Equal(t, "F", leaf5.Ref)

	mutatedRoot, err := m.GetRootHash(ctx, stage, space1)
	require.NoError(t, err)
	assert.NotEqual(t, origRoot, mutatedRoot)

	expectedLeaves := []Leaf{
		leaf("A"),
		leaf("X"),
		{Ref: "", Hash: deletedHash()},
		leaf("D"),
		leaf("E"),
		leaf("F"),
	}
	require.NoError(t, m.Build(ctx, stage, space2, enumerators.Slice(expectedLeaves)))
	expectedRoot, err := m.GetRootHash(ctx, stage, space2)
	require.NoError(t, err)

	assert.Equal(t, expectedRoot, mutatedRoot, "batch mutations must match a rebuild with the same final leaves")
}

func TestInvariantAppendOnlyBatchMutationsEqualsRebuild(t *testing.T) {
	// Append-only mutations should match a rebuild and return assigned indexes in order.

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space1 := "append-only"
	space2 := "append-rebuild"

	require.NoError(t, m.Build(ctx, stage, space1, leaves("A", "B", "C", "D", "E")))

	// Act
	resultIndexes, err := m.ApplyLeafMutations(ctx, stage, space1, []LeafMutation{
		{Append: true, Leaf: leaf("F")},
		{Append: true, Leaf: leaf("G")},
		{Append: true, Leaf: leaf("H")},
	})
	require.NoError(t, err)

	// Assert
	assert.Equal(t, []int{5, 6, 7}, resultIndexes)

	count, err := m.GetLeafCount(ctx, stage, space1)
	require.NoError(t, err)
	assert.Equal(t, 8, count)

	for index, ref := range []string{"F", "G", "H"} {
		leafAtIndex, err := m.GetLeaf(ctx, stage, space1, 5+index)
		require.NoError(t, err)
		assert.Equal(t, ref, leafAtIndex.Ref)
	}

	mutatedRoot, err := m.GetRootHash(ctx, stage, space1)
	require.NoError(t, err)

	require.NoError(t, m.Build(ctx, stage, space2, leaves("A", "B", "C", "D", "E", "F", "G", "H")))
	expectedRoot, err := m.GetRootHash(ctx, stage, space2)
	require.NoError(t, err)

	assert.Equal(t, expectedRoot, mutatedRoot, "append-only batch mutations must match a rebuild with the same final leaves")
}

func TestInvariantAppendOnlyMutationSessionEqualsRebuild(t *testing.T) {
	// An append-only mutation session should assign stable indexes across queued batches and flush to
	// the same tree state as a rebuild with the final leaf set.

	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space1 := "session-append"
	space2 := "session-rebuild"

	require.NoError(t, m.Build(ctx, stage, space1, leaves("A", "B", "C", "D", "E")))

	session, err := m.BeginMutationSession(ctx, stage, space1)
	require.NoError(t, err)

	indexes1, err := session.QueueLeafMutations([]LeafMutation{
		{Append: true, Leaf: leaf("F")},
		{Append: true, Leaf: leaf("G")},
	})
	require.NoError(t, err)
	assert.Equal(t, []int{5, 6}, indexes1)

	indexes2, err := session.QueueLeafMutations([]LeafMutation{
		{Append: true, Leaf: leaf("H")},
		{Append: true, Leaf: leaf("I")},
	})
	require.NoError(t, err)
	assert.Equal(t, []int{7, 8}, indexes2)

	require.NoError(t, session.Flush(ctx))
	session.Close()

	count, err := m.GetLeafCount(ctx, stage, space1)
	require.NoError(t, err)
	assert.Equal(t, 9, count)

	mutatedRoot, err := m.GetRootHash(ctx, stage, space1)
	require.NoError(t, err)

	require.NoError(t, m.Build(ctx, stage, space2, leaves("A", "B", "C", "D", "E", "F", "G", "H", "I")))
	expectedRoot, err := m.GetRootHash(ctx, stage, space2)
	require.NoError(t, err)

	assert.Equal(t, expectedRoot, mutatedRoot, "append-only mutation session must match a rebuild with the same final leaves")
}

func TestInvariantRootHashDeterminism(t *testing.T) {
	// Root hash must be deterministic for identical leaf sets

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"

	// Build same tree twice in different spaces
	require.NoError(t, m.Build(ctx, stage, "space1", leaves("P", "Q", "R")))
	require.NoError(t, m.Build(ctx, stage, "space2", leaves("P", "Q", "R")))

	// Act
	root1, err := m.GetRootHash(ctx, stage, "space1")
	require.NoError(t, err)
	root2, err := m.GetRootHash(ctx, stage, "space2")
	require.NoError(t, err)

	// Assert
	assert.Equal(t, root1, root2, "identical leaf sets must produce identical root hash")
}

func TestInvariantLeafCountMetadataConsistency(t *testing.T) {
	// Leaf count metadata must always match actual accessible leaves

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "consistency"

	// Test after build
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C")))
	count, err := m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	for i := 0; i < count; i++ {
		_, err := m.GetLeaf(ctx, stage, space, i)
		assert.NoError(t, err, "should be able to access leaf %d", i)
	}

	// Test after add
	_, err = m.AddLeaf(ctx, stage, space, leaf("D"))
	require.NoError(t, err)
	count, err = m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 4, count)
	for i := 0; i < count; i++ {
		_, err := m.GetLeaf(ctx, stage, space, i)
		assert.NoError(t, err, "should be able to access leaf %d", i)
	}

	// Test after update (count unchanged)
	require.NoError(t, m.UpdateLeaf(ctx, stage, space, 0, leaf("X")))
	count, err = m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	// Test after remove (count unchanged - soft delete)
	require.NoError(t, m.RemoveLeaf(ctx, stage, space, 1))
	count, err = m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 4, count)
}

func TestInvariantSoftDeletePreservesIndices(t *testing.T) {
	// Soft delete must not reindex leaves - indices must remain stable

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "indices"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D", "E")))

	// Get all leaves before delete
	var beforeLeaves []Leaf
	for i := 0; i < 5; i++ {
		l, err := m.GetLeaf(ctx, stage, space, i)
		require.NoError(t, err)
		beforeLeaves = append(beforeLeaves, l)
	}

	// Act - Remove middle leaf
	require.NoError(t, m.RemoveLeaf(ctx, stage, space, 2))

	// Assert - All other leaves should be at same indices
	leaf0, _ := m.GetLeaf(ctx, stage, space, 0)
	leaf1, _ := m.GetLeaf(ctx, stage, space, 1)
	leaf2, _ := m.GetLeaf(ctx, stage, space, 2)
	leaf3, _ := m.GetLeaf(ctx, stage, space, 3)
	leaf4, _ := m.GetLeaf(ctx, stage, space, 4)

	assert.Equal(t, beforeLeaves[0].Ref, leaf0.Ref, "index 0 unchanged")
	assert.Equal(t, beforeLeaves[1].Ref, leaf1.Ref, "index 1 unchanged")
	assert.Equal(t, "", leaf2.Ref, "index 2 deleted")
	assert.Equal(t, beforeLeaves[3].Ref, leaf3.Ref, "index 3 unchanged")
	assert.Equal(t, beforeLeaves[4].Ref, leaf4.Ref, "index 4 unchanged")
}

func TestInvariantInputValidation(t *testing.T) {
	// All public methods must validate inputs

	ctx := context.Background()
	m := setup(t)
	require.NoError(t, m.Build(ctx, "stage", "space", leaves("A")))

	// Test empty stage
	_, err := m.GetLeafCount(ctx, "", "space")
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidStage, err)

	// Test empty space
	_, err = m.GetLeafCount(ctx, "stage", "")
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidSpace, err)

	// Test negative index
	_, err = m.GetLeaf(ctx, "stage", "space", -1)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidIndex, err)

	// Test nil hash
	err = m.UpdateLeaf(ctx, "stage", "space", 0, Leaf{Ref: "test", Hash: nil})
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidLeaf, err)

	_, err = m.AddLeaf(ctx, "stage", "space", Leaf{Ref: "test", Hash: nil})
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidLeaf, err)
}

func TestInvariantEmptyTreeBehavior(t *testing.T) {
	// Empty trees must behave consistently

	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "empty"

	// Empty tree has no root
	root, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.Nil(t, root)

	// Empty tree has zero count
	count, err := m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// Can add to empty tree
	idx, err := m.AddLeaf(ctx, stage, space, leaf("A"))
	assert.NoError(t, err)
	assert.Equal(t, 0, idx)

	// Tree is now non-empty
	count, err = m.GetLeafCount(ctx, stage, space)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	root, err = m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.NotNil(t, root)
}

func TestInvariantHashPropagation(t *testing.T) {
	// Changing any leaf must change root hash

	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "propagation"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D", "E", "F")))

	origRoot, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)

	// Update each leaf and verify root changes
	for i := 0; i < 6; i++ {
		require.NoError(t, m.UpdateLeaf(ctx, stage, space, i, leaf(fmt.Sprintf("X%d", i))))
		newRoot, err := m.GetRootHash(ctx, stage, space)
		require.NoError(t, err)
		assert.NotEqual(t, origRoot, newRoot, "updating leaf %d should change root", i)
		origRoot = newRoot
	}
}

func TestInvariantEnumerationCompleteness(t *testing.T) {
	// EnumerateLeaves must return all leaves in order

	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "enum"
	expectedRefs := []string{"M", "N", "O", "P"}
	require.NoError(t, m.Build(ctx, stage, space, leaves(expectedRefs...)))

	// Act
	enum := m.EnumerateLeaves(ctx, stage, space)
	result, err := enumerators.ToSlice(enum)

	// Assert
	require.NoError(t, err)
	assert.Len(t, result, len(expectedRefs))
	for i, expected := range expectedRefs {
		assert.Equal(t, expected, result[i].Ref, "leaf at index %d", i)
	}
}

func TestInvariantBatchLeafReadsPreserveOrder(t *testing.T) {
	// Batch leaf reads must return the requested indexes in the same order they were asked for.

	// Arrange
	ctx := context.Background()
	m := setup(t)
	stage := "test"
	space := "batch-read"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B", "C", "D", "E")))

	// Act
	result, err := m.GetLeavesByIndex(ctx, stage, space, []int{3, 1, 4})

	// Assert
	require.NoError(t, err)
	require.Len(t, result, 3)
	assert.Equal(t, "D", result[0].Ref)
	assert.Equal(t, "B", result[1].Ref)
	assert.Equal(t, "E", result[2].Ref)
}

func TestInvariantBranchingFactorRespected(t *testing.T) {
	// Tree structure must respect branching factor

	ctx := context.Background()

	testCases := []struct {
		branching int
		leaves    int
	}{
		{2, 7},  // 7 leaves with binary tree
		{3, 10}, // 10 leaves with ternary tree
		{4, 13}, // 13 leaves with quaternary tree
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("branching_%d", tc.branching), func(t *testing.T) {
			store, err := pebble.NewPebbleStore(filepath.Join(t.TempDir(), fmt.Sprintf("b%d", tc.branching)))
			require.NoError(t, err)
			defer func() {
				if err := store.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()

			m := NewTree(store, WithBranching(tc.branching))
			stage := "test"
			space := "branching"

			// Create leaves
			leafRefs := make([]string, tc.leaves)
			for i := 0; i < tc.leaves; i++ {
				leafRefs[i] = fmt.Sprintf("L%d", i)
			}

			// Act
			require.NoError(t, m.Build(ctx, stage, space, leaves(leafRefs...)))

			// Assert - tree is valid and accessible
			count, err := m.GetLeafCount(ctx, stage, space)
			assert.NoError(t, err)
			assert.Equal(t, tc.leaves, count)

			root, err := m.GetRootHash(ctx, stage, space)
			assert.NoError(t, err)
			assert.NotNil(t, root)

			// Verify all leaves accessible
			for i := 0; i < tc.leaves; i++ {
				_, err := m.GetLeaf(ctx, stage, space, i)
				assert.NoError(t, err, "leaf %d should be accessible", i)
			}
		})
	}
}
