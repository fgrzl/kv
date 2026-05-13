package merkle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateLeafShouldErrorWhenPaddingSiblingIsMissing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-padding-path")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	tree := NewTree(base, WithBranching(3))
	require.NoError(t, tree.Build(ctx, "st", "sp", leaves("a", "b")))
	require.NoError(t, base.Remove(ctx, nodePK("st", "sp", 0, 2)))

	err = tree.UpdateLeaf(ctx, "st", "sp", 0, leaf("updated-a"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing sibling")
}
