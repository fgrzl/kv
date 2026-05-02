package merkle

import (
	"context"
	"fmt"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func explicitLeaves(items ...Leaf) enumerators.Enumerator[Leaf] {
	return enumerators.Slice(items)
}

func makeSequentialLeaves(count int) []Leaf {
	leaves := make([]Leaf, count)
	for i := 0; i < count; i++ {
		leaves[i] = leaf(fmt.Sprintf("leaf-%02d", i))
	}
	return leaves
}

func mustRootHash(t *testing.T, tree *Tree, ctx context.Context, stage, space string) []byte {
	t.Helper()
	root, _, err := tree.GetRootHash(ctx, stage, space)
	require.NoError(t, err)
	return root
}

func TestLeafCodecShouldRoundTrip(t *testing.T) {
	original := Leaf{Ref: "leaf-42", Hash: ComputeHash([]byte("leaf-42"))}

	encoded := encodeLeafValue(original)
	decoded, err := decodeLeafValue(encoded)

	require.NoError(t, err)
	assert.Equal(t, original.Ref, decoded.Ref)
	assert.Equal(t, original.Hash, decoded.Hash)
}

func TestUpdateLeafShouldMatchFullRebuildAtDepth(t *testing.T) {
	ctx := context.Background()
	actual := setup(t)
	expected := setup(t)
	stage := "deep-update"
	space := "regression"

	initial := makeSequentialLeaves(16)
	require.NoError(t, actual.Build(ctx, stage, space, explicitLeaves(initial...)))

	updated := append([]Leaf(nil), initial...)
	updated[9] = leaf("updated-09")

	require.NoError(t, actual.UpdateLeaf(ctx, stage, space, 9, updated[9]))
	require.NoError(t, expected.Build(ctx, stage, space, explicitLeaves(updated...)))

	assert.Equal(t, mustRootHash(t, expected, ctx, stage, space), mustRootHash(t, actual, ctx, stage, space))
}

func TestAddLeafShouldMatchFullRebuildWhenTreeHeightChanges(t *testing.T) {
	ctx := context.Background()
	actual := setup(t)
	expected := setup(t)
	stage := "deep-add"
	space := "regression"

	initial := makeSequentialLeaves(16)
	require.NoError(t, actual.Build(ctx, stage, space, explicitLeaves(initial...)))

	newLeaf := leaf("leaf-16-new")
	_, err := actual.AddLeaf(ctx, stage, space, newLeaf)
	require.NoError(t, err)

	expectedLeaves := append(append([]Leaf(nil), initial...), newLeaf)
	require.NoError(t, expected.Build(ctx, stage, space, explicitLeaves(expectedLeaves...)))

	assert.Equal(t, mustRootHash(t, expected, ctx, stage, space), mustRootHash(t, actual, ctx, stage, space))
	count, err := actual.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, len(expectedLeaves), count)
}

func TestRemoveLeafShouldMatchFullRebuildWithDeletedMarkerAtDepth(t *testing.T) {
	ctx := context.Background()
	actual := setup(t)
	expected := setup(t)
	stage := "deep-remove"
	space := "regression"

	initial := makeSequentialLeaves(16)
	require.NoError(t, actual.Build(ctx, stage, space, explicitLeaves(initial...)))

	require.NoError(t, actual.RemoveLeaf(ctx, stage, space, 11))

	expectedLeaves := append([]Leaf(nil), initial...)
	expectedLeaves[11] = Leaf{Hash: deletedHash()}
	require.NoError(t, expected.Build(ctx, stage, space, explicitLeaves(expectedLeaves...)))

	assert.Equal(t, mustRootHash(t, expected, ctx, stage, space), mustRootHash(t, actual, ctx, stage, space))
	deleted, err := actual.GetLeaf(ctx, stage, space, 11)
	require.NoError(t, err)
	assert.Equal(t, "", deleted.Ref)
	assert.Equal(t, deletedHash(), deleted.Hash)
}

func TestUpdateLeafOutOfBoundsShouldNotPersistOrphanRow(t *testing.T) {
	ctx := context.Background()
	m := setup(t)
	stage := "oob-update"
	space := "regression"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))
	originalRoot := mustRootHash(t, m, ctx, stage, space)

	invalidIndex := 10
	err := m.UpdateLeaf(ctx, stage, space, invalidIndex, leaf("X"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of bounds")
	assert.Equal(t, originalRoot, mustRootHash(t, m, ctx, stage, space))
	count, err := m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	_, err = m.GetLeaf(ctx, stage, space, invalidIndex)
	assert.Error(t, err)
	orphan, err := m.store.Get(ctx, nodePK(stage, space, 0, invalidIndex))
	require.NoError(t, err)
	assert.Nil(t, orphan)
}

func TestRemoveLeafOutOfBoundsShouldNotPersistOrphanRow(t *testing.T) {
	ctx := context.Background()
	m := setup(t)
	stage := "oob-remove"
	space := "regression"
	require.NoError(t, m.Build(ctx, stage, space, leaves("A", "B")))
	originalRoot := mustRootHash(t, m, ctx, stage, space)

	invalidIndex := 10
	err := m.RemoveLeaf(ctx, stage, space, invalidIndex)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of bounds")
	assert.Equal(t, originalRoot, mustRootHash(t, m, ctx, stage, space))
	count, err := m.GetLeafCount(ctx, stage, space)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	_, err = m.GetLeaf(ctx, stage, space, invalidIndex)
	assert.Error(t, err)
	orphan, err := m.store.Get(ctx, nodePK(stage, space, 0, invalidIndex))
	require.NoError(t, err)
	assert.Nil(t, orphan)
}

func BenchmarkLeafCodecEncode(b *testing.B) {
	leafValue := Leaf{Ref: "benchmark-leaf-0001", Hash: ComputeHash([]byte("benchmark-leaf-0001"))}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encodeLeafValue(leafValue)
	}
}

func BenchmarkLeafCodecDecode(b *testing.B) {
	encoded := encodeLeafValue(Leaf{Ref: "benchmark-leaf-0001", Hash: ComputeHash([]byte("benchmark-leaf-0001"))})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := decodeLeafValue(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
