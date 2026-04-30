package merkle

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildBatchItemsShareTreePartition(t *testing.T) {
	m := setup(t)
	stage, space := "lay", "out"
	want := treePartition(stage, space)

	_, items, _, err := m.collectBuildPutBatchItems(stage, space, leaves("x", "y", "z"))
	require.NoError(t, err)
	require.NotEmpty(t, items)

	for _, it := range items {
		assert.True(t, bytes.Equal(it.PK.PartitionKey, want),
			"all build puts must share the tree partition")
	}
}

func TestEnumerateLeavesScanManyInOrder(t *testing.T) {
	ctx := context.Background()
	m := setup(t)
	stage, space := "scan", "many"

	const n = 150
	ls := make([]Leaf, n)
	for i := 0; i < n; i++ {
		ls[i] = leaf(fmt.Sprintf("L%d", i))
	}
	require.NoError(t, m.Build(ctx, stage, space, enumerators.Slice(ls)))

	out, err := enumerators.ToSlice(m.EnumerateLeaves(ctx, stage, space))
	require.NoError(t, err)
	require.Len(t, out, n)
	for i := 0; i < n; i++ {
		assert.Equal(t, fmt.Sprintf("L%d", i), out[i].Ref)
	}
}

func TestPruneRemovesEntireTree(t *testing.T) {
	ctx := context.Background()
	m := setup(t)
	stage, space := "pr", "une"

	require.NoError(t, m.Build(ctx, stage, space, leaves("a", "b")))
	root, _, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)
	require.NotNil(t, root)

	require.NoError(t, m.Prune(ctx, stage, space))
	rootAfter, _, err := m.GetRootHash(ctx, stage, space)
	require.NoError(t, err)
	require.Nil(t, rootAfter)
}
