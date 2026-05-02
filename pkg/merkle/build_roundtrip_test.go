package merkle

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingKV wraps kv.KV to count round-trip operations used by Build and related paths.
type countingKV struct {
	inner kv.KV

	batchCalls       atomic.Int64
	batchChunksCalls atomic.Int64
	putCalls         atomic.Int64
	getCalls         atomic.Int64
	getBatchCalls    atomic.Int64
	removeRangeCalls atomic.Int64
	insertCalls      atomic.Int64
	removeCalls      atomic.Int64
	removeBatchCalls atomic.Int64
	queryCalls       atomic.Int64
	enumerateCalls   atomic.Int64
}

func newCountingKV(inner kv.KV) *countingKV {
	return &countingKV{inner: inner}
}

func (c *countingKV) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	c.getCalls.Add(1)
	return c.inner.Get(ctx, pk)
}

func (c *countingKV) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]kv.BatchGetResult, error) {
	c.getBatchCalls.Add(1)
	return c.inner.GetBatch(ctx, keys...)
}

func (c *countingKV) Insert(ctx context.Context, item *kv.Item) error {
	c.insertCalls.Add(1)
	return c.inner.Insert(ctx, item)
}

func (c *countingKV) Put(ctx context.Context, item *kv.Item) error {
	c.putCalls.Add(1)
	return c.inner.Put(ctx, item)
}

func (c *countingKV) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	c.removeCalls.Add(1)
	return c.inner.Remove(ctx, pk)
}

func (c *countingKV) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	c.removeBatchCalls.Add(1)
	return c.inner.RemoveBatch(ctx, keys...)
}

func (c *countingKV) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {
	c.removeRangeCalls.Add(1)
	return c.inner.RemoveRange(ctx, rangeKey)
}

func (c *countingKV) Query(ctx context.Context, queryArgs kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {
	c.queryCalls.Add(1)
	return c.inner.Query(ctx, queryArgs, sort)
}

func (c *countingKV) Enumerate(ctx context.Context, queryArgs kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	c.enumerateCalls.Add(1)
	return c.inner.Enumerate(ctx, queryArgs)
}

func (c *countingKV) Batch(ctx context.Context, items []*kv.BatchItem) error {
	c.batchCalls.Add(1)
	return c.inner.Batch(ctx, items)
}

func (c *countingKV) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	c.batchChunksCalls.Add(1)
	// Chunk here and route through Batch so Batch call counts reflect physical writes.
	return enumerators.ForEach(
		enumerators.ChunkByCount(items, chunkSize),
		func(chunk enumerators.Enumerator[*kv.BatchItem]) error {
			var batch []*kv.BatchItem
			if err := enumerators.ForEach(chunk, func(item *kv.BatchItem) error {
				batch = append(batch, item)
				return nil
			}); err != nil {
				return err
			}
			if len(batch) > 0 {
				return c.Batch(ctx, batch)
			}
			return nil
		},
	)
}

func (c *countingKV) Close() error {
	return c.inner.Close()
}

func TestBuildUsesSingleBatchChunksCall(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-counting")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	wrap := newCountingKV(base)
	tree := NewTree(wrap, WithBatchSize(500)) // multiple physical chunks for inner.Batch

	const n = 1200
	leaves := benchLeaves(n)
	require.NoError(t, tree.Build(ctx, "st", "sp", leaves))

	assert.Equal(t, int64(1), wrap.batchChunksCalls.Load(), "Build should stream through one BatchChunks call")
	// Inner store still performs one Batch per chunk.
	assert.GreaterOrEqual(t, wrap.batchCalls.Load(), int64(2), "chunked writes should map to multiple inner Batch calls")

	root, err := tree.GetRootHash(ctx, "st", "sp")
	require.NoError(t, err)
	assert.NotNil(t, root)
}

func TestBuildWithSkipPruneAvoidsRemoveRange(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-skipprune")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	wrap := newCountingKV(base)
	tree := NewTree(wrap, WithSkipPruneOnBuild(true))
	require.NoError(t, tree.Build(ctx, "st", "sp", leaves("a", "b")))

	assert.Equal(t, int64(0), wrap.removeRangeCalls.Load())
}

func TestUpdateLeafUsesSingleGetBatchForPathRecompute(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-update-roundtrips")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	wrap := newCountingKV(base)
	tree := NewTree(wrap)
	require.NoError(t, tree.Build(ctx, "st", "sp", benchLeaves(16)))

	getBatchBefore := wrap.getBatchCalls.Load()
	batchBefore := wrap.batchCalls.Load()
	putBefore := wrap.putCalls.Load()
	require.NoError(t, tree.UpdateLeaf(ctx, "st", "sp", 9, leaf("updated-09")))

	assert.Equal(t, int64(1), wrap.getBatchCalls.Load()-getBatchBefore, "UpdateLeaf should prefetch all recompute siblings in one GetBatch")
	assert.Equal(t, int64(1), wrap.batchCalls.Load()-batchBefore, "UpdateLeaf should persist leaf + recomputed path + root in one Batch")
	assert.Equal(t, int64(0), wrap.putCalls.Load()-putBefore, "UpdateLeaf should not issue any standalone Puts")
}

func TestAddLeafStableHeightUsesSingleGetBatchForPathRecompute(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "merkle-add-roundtrips")
	base, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	wrap := newCountingKV(base)
	tree := NewTree(wrap)
	require.NoError(t, tree.Build(ctx, "st", "sp", benchLeaves(3)))

	getBatchBefore := wrap.getBatchCalls.Load()
	batchBefore := wrap.batchCalls.Load()
	putBefore := wrap.putCalls.Load()
	index, err := tree.AddLeaf(ctx, "st", "sp", leaf("added-03"))
	require.NoError(t, err)

	assert.Equal(t, 3, index)
	assert.Equal(t, int64(1), wrap.getBatchCalls.Load()-getBatchBefore, "stable-height AddLeaf should prefetch all recompute siblings in one GetBatch")
	assert.Equal(t, int64(1), wrap.batchCalls.Load()-batchBefore, "stable-height AddLeaf should persist leaf + padding + leafcount + path + root in one Batch")
	assert.Equal(t, int64(0), wrap.putCalls.Load()-putBefore, "stable-height AddLeaf should not issue any standalone Puts")
}
