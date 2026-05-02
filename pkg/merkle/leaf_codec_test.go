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
	root, err := tree.GetRootHash(ctx, stage, space)
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

func TestLeafCodecEdgeCases(t *testing.T) {
	t.Run("empty ref", func(t *testing.T) {
		original := Leaf{Ref: "", Hash: ComputeHash([]byte("x"))}
		encoded := encodeLeafValue(original)
		decoded, err := decodeLeafValue(encoded)
		require.NoError(t, err)
		assert.Equal(t, "", decoded.Ref)
		assert.Equal(t, original.Hash, decoded.Hash)
	})
	t.Run("empty hash", func(t *testing.T) {
		original := Leaf{Ref: "r", Hash: nil}
		encoded := encodeLeafValue(original)
		decoded, err := decodeLeafValue(encoded)
		require.NoError(t, err)
		assert.Equal(t, "r", decoded.Ref)
		assert.Empty(t, decoded.Hash)
	})
	t.Run("truncated buffer", func(t *testing.T) {
		_, err := decodeLeafValue([]byte{0x01, 0x02})
		require.Error(t, err)
	})
	t.Run("refLen exceeds buffer", func(t *testing.T) {
		// refLen=255, but only 4 bytes total -> must error.
		buf := []byte{0xFF, 0x00, 0x00, 0x00}
		_, err := decodeLeafValue(buf)
		require.Error(t, err)
	})
	t.Run("refLen overflow", func(t *testing.T) {
		// refLen=2^32-1 with empty payload after the length: must error, not panic.
		buf := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		_, err := decodeLeafValue(buf)
		require.Error(t, err)
	})
}

func FuzzDecodeLeafValue(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{0x00},
		{0x01, 0x02, 0x03},
		{0x04, 0x00, 0x00, 0x00},
		append([]byte{0x01, 0x00, 0x00, 0x00}, []byte("x")...),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeLeafValue(data)
	})
}

func TestNodeRowKeyOrdering(t *testing.T) {
	// Lexkey-encoded (disc, level, index) tuples must sort numerically by (level, index).
	prev := encodeNodeRowKey(0, 0)
	cases := []struct {
		level, index int
	}{
		{0, 1}, {0, 2}, {0, 100}, {0, 1 << 20},
		{1, 0}, {1, 1}, {1, 100},
		{2, 0}, {3, 0}, {7, 1234},
	}
	for _, c := range cases {
		curr := encodeNodeRowKey(uint8(c.level), uint64(c.index))
		assert.True(t, string(prev) < string(curr), "lex order broken at level=%d index=%d", c.level, c.index)
		prev = curr
	}
}

func TestRowKeyDiscriminatorsAreDistinct(t *testing.T) {
	node := encodeNodeRowKey(0, 0)
	meta := encodeMetaRowKey("leafcount")
	root := encodeRootRowKey()
	assert.NotEqual(t, node, meta)
	assert.NotEqual(t, node, root)
	assert.NotEqual(t, meta, root)
	// node < meta < root by chosen discriminator bytes (0x01 < 0x02 < 0x03).
	assert.True(t, string(node) < string(meta))
	assert.True(t, string(meta) < string(root))
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
