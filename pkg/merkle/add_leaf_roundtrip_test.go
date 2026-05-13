package merkle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddLeafShouldUseOneGetBatchAndOneBatchForStableHeight(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-add-roundtrip")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	wrap := newCountingKV(base)
	tree := NewTree(wrap)
	require.NoError(t, tree.Build(ctx, "st", "sp", leaves("a", "b", "c")))

	getBatchBefore := wrap.getBatchCalls.Load()
	batchBefore := wrap.batchCalls.Load()
	index, err := tree.AddLeaf(ctx, "st", "sp", leaf("d"))
	require.NoError(t, err)

	assert.Equal(t, 3, index)
	assert.Equal(t, int64(1), wrap.getBatchCalls.Load()-getBatchBefore)
	assert.Equal(t, int64(1), wrap.batchCalls.Load()-batchBefore)
}
