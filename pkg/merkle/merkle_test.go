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

func TestMerkleBuildAndRootHash(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage := "blue"
	space := "testspace"
	refs := []string{"A", "B", "C"}

	err := m.Build(ctx, stage, space, leaves(refs...))
	assert.NoError(t, err, "Build should succeed")

	root, _, err := m.GetRootHash(ctx, stage, space)
	assert.NoError(t, err, "GetRootHash should succeed")
	assert.NotNil(t, root, "Root hash should not be nil")
}

func TestMerkleSymmetricDiffAdditions(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"

	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B", "C")))

	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "C", diffs[0].Ref)
}

func TestMerkleSymmetricDiffSubtractions(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"

	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B", "C")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B")))

	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "C", diffs[0].Ref)
}

func TestMerkleSymmetricDiffAddAndRemove(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "diffspace"

	require.NoError(t, m.Build(ctx, stage1, space, leaves("A", "B", "C")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A", "B", "D")))

	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))
	assert.NoError(t, err)
	assert.Len(t, diffs, 2)
	assert.Equal(t, "D", diffs[0].Ref)
	assert.Equal(t, "C", diffs[1].Ref)
}

func TestMerkleSymmetricDiffEmptyVsNonEmpty(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "emptynonempty"

	require.NoError(t, m.Build(ctx, stage1, space, leaves()))
	require.NoError(t, m.Build(ctx, stage2, space, leaves("A")))

	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "A", diffs[0].Ref)
}

func TestMerkleSymmetricDiffNonEmptyVsEmpty(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage1 := "blue"
	stage2 := "green"
	space := "emptynonempty"

	require.NoError(t, m.Build(ctx, stage1, space, leaves("A")))
	require.NoError(t, m.Build(ctx, stage2, space, leaves()))

	diffs, err := enumerators.ToSlice(m.SymmetricDiff(ctx, stage1, stage2, space))
	assert.NoError(t, err)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "A", diffs[0].Ref)
}

func TestMerklePrune(t *testing.T) {
	ctx := t.Context()
	m := setup(t)
	stage := "blue"
	space := "prunespace"
	require.NoError(t, m.Build(ctx, stage, space, leaves("X", "Y")))

	assert.NoError(t, m.Prune(ctx, stage, space), "Prune should succeed")
	root, _, err := m.GetRootHash(ctx, stage, space)
	assert.Nil(t, root, "Root hash should be nil after prune")
	assert.NoError(t, err, "GetRootHash should not error even after prune")
}
