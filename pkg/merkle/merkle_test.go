package merkle

import (
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) *Tree {
	path := filepath.Join(t.TempDir(), "merkle")
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return NewTree(store)
}

func hashFor(data string) []byte {
	h := sha256.Sum256([]byte(data))
	return h[:]
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

// --- TESTS ---

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
	root, _, err := m.GetRootHash(ctx, stage, space)
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
	root, _, err := m.GetRootHash(ctx, stage, space)
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
	root, _, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err)
	assert.Nil(t, root)
}

func TestShouldHandleInvalidBranchingFactor(t *testing.T) {
	// Arrange
	store, err := pebble.NewPebbleStore(filepath.Join(t.TempDir(), "invalid"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	// Act
	tree := NewTree(store, WithBranching(1))

	// Assert
	// Since WithBranching checks n >= 2, it should default to 2
	assert.NotNil(t, tree)
}
