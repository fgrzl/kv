package merkle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecomputeAllInternalNodesShouldStreamThroughSingleBatchChunksCall(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-recompute-full")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	wrap := newCountingKV(base)
	tree := NewTree(wrap, WithBatchSize(2))
	require.NoError(t, tree.Build(ctx, "st", "sp", leaves("a", "b", "c", "d", "e")))

	wrap.enumerateCalls.Store(0)
	wrap.batchChunksCalls.Store(0)

	require.NoError(t, tree.recomputeAllInternalNodes(ctx, "st", "sp"))

	assert.Equal(t, int64(1), wrap.enumerateCalls.Load())
	assert.Equal(t, int64(1), wrap.batchChunksCalls.Load())
}
