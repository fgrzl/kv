package merkle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadMissingNodeHashesShouldMixSmallAndLargeRangesInOneCall(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-range-load")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	wrap := newCountingKV(base)
	tree := NewTree(wrap)
	require.NoError(t, tree.Build(ctx, "st", "sp", benchLeaves(128)))

	partition := treePartition("st", "sp")
	parentIndexes := append(make([]int, 0, 80), func() []int {
		indexes := make([]int, 0, 15)
		for i := 0; i < 15; i++ {
			indexes = append(indexes, i)
		}
		for i := 32; i < 64; i++ {
			indexes = append(indexes, i)
		}
		return indexes
	}()...)

	loaded, err := tree.loadMissingNodeHashesInPartition(ctx, partition, 0, 128, parentIndexes, nil)
	require.NoError(t, err)
	assert.Len(t, loaded, 94)
	assert.Equal(t, int64(1), wrap.getBatchCalls.Load())
	assert.Equal(t, int64(1), wrap.enumerateCalls.Load())
}
