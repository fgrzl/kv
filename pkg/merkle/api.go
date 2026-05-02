package merkle

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Build builds and persists a Merkle tree from a leaf enumerator.
func (m *Tree) Build(ctx context.Context, stage, space string, leaves enumerators.Enumerator[Leaf]) error {
	ctx, span := tracer.Start(ctx, "merkle.Build",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
		))
	defer span.End()

	slog.DebugContext(ctx, "starting Merkle tree build", "stage", stage, "space", space)
	if !m.skipPruneOnBuild {
		if err := m.pruneOldNodes(ctx, stage, space); err != nil {
			err = fmt.Errorf("prune old nodes for %s/%s: %w", stage, space, err)
			slog.ErrorContext(ctx, "failed to prune old nodes", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}
	paddedNodes, batchItems, actualLeafCount, err := m.collectBuildPutBatchItems(stage, space, leaves)
	if err != nil {
		err = fmt.Errorf("collect build batch items for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to collect build batch items", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(paddedNodes) == 0 {
		slog.DebugContext(ctx, "Merkle tree build completed with no leaves", "stage", stage, "space", space)
		span.SetAttributes(attribute.Int("leaves", 0))
		return nil
	}

	if err := m.store.BatchChunks(ctx, enumerators.Slice(batchItems), m.batchSize); err != nil {
		err = fmt.Errorf("persist merkle build for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to persist merkle build", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "Merkle tree build completed", "stage", stage, "space", space, "leaves", actualLeafCount)
	span.SetAttributes(attribute.Int("leaves", actualLeafCount))
	return nil
}

// Diff returns leaves that are present in 'curr' but not in 'prev' (one-way diff).
func (m *Tree) Diff(ctx context.Context, prev, curr, space string) enumerators.Enumerator[Leaf] {
	return m.diffNode(ctx, prev, curr, space, rootRef)
}

// SymmetricDiff returns added and removed leaves between two trees (curr vs prev, prev vs curr).
func (m *Tree) SymmetricDiff(ctx context.Context, prev, curr, space string) enumerators.Enumerator[Leaf] {

	visited := make(map[string]struct{})

	return enumerators.Chain(

		// prev -> curr track visited
		enumerators.Map(
			m.diffNode(ctx, prev, curr, space, rootRef),
			func(leaf Leaf) (Leaf, error) {
				visited[leaf.Ref] = struct{}{}
				return leaf, nil
			}),

		// curr -> prev filter visited
		enumerators.FilterMap(
			m.diffNode(ctx, curr, prev, space, rootRef),
			func(leaf Leaf) (Leaf, bool, error) {
				_, ok := visited[leaf.Ref]
				return leaf, !ok, nil
			}))
}

// GetRootHash returns the Merkle root hash for a given stage/space.
func (m *Tree) GetRootHash(ctx context.Context, stage, space string) ([]byte, error) {
	hash, _, err := m.getHash(ctx, stage, space, rootRef)
	return hash, err
}

// Prune removes all Merkle nodes for the given stage and space.
func (m *Tree) Prune(ctx context.Context, stage, space string) error {
	slog.DebugContext(ctx, "pruning Merkle tree", "stage", stage, "space", space)
	rangeKey := treeRangeKey(stage, space)
	err := m.store.RemoveRange(ctx, rangeKey)
	if err != nil {
		slog.ErrorContext(ctx, "failed to prune Merkle tree", "stage", stage, "space", space, "err", err)
		return err
	}
	slog.DebugContext(ctx, "Merkle tree pruned", "stage", stage, "space", space)
	return nil
}

// GetLeafCount returns the number of actual leaves in the tree (excluding padding).
// Note: Soft-deleted leaves (via RemoveLeaf) are still counted.
func (m *Tree) GetLeafCount(ctx context.Context, stage, space string) (int, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return 0, err
	}
	return m.getLeafCountMetadata(ctx, stage, space)
}

// GetLeaf retrieves a specific leaf by index.
// Returns the leaf data, or an error if the index is out of bounds.
func (m *Tree) GetLeaf(ctx context.Context, stage, space string, leafIndex int) (Leaf, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return Leaf{}, err
	}
	if leafIndex < 0 {
		return Leaf{}, ErrInvalidIndex
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return Leaf{}, err
	}
	if leafIndex >= leafCount {
		return Leaf{}, fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}

	pkVal := nodePK(stage, space, 0, leafIndex)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return Leaf{}, fmt.Errorf("failed to get leaf: %w", err)
	}
	if item == nil {
		return Leaf{}, fmt.Errorf("leaf not found at index %d", leafIndex)
	}

	leaf, err := decodeLeafValue(item.Value)
	if err != nil {
		return Leaf{}, fmt.Errorf("failed to decode leaf: %w", err)
	}
	return leaf, nil
}

// EnumerateLeaves returns a lazy enumerator over all leaves in the tree.
// The enumeration includes soft-deleted leaves (with empty Ref field).
// It scans level-0 node rows in index order via one partition range query (not N Get calls).
func (m *Tree) EnumerateLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	if err := validateStageSpace(stage, space); err != nil {
		return enumerators.Error[Leaf](err)
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return enumerators.Error[Leaf](err)
	}
	if leafCount == 0 {
		return enumerators.Empty[Leaf]()
	}

	args := nodeLevelRangeQuery(stage, space, 0, 0, leafCount-1)
	return enumerators.Map(
		m.store.Enumerate(ctx, args),
		func(item *kv.Item) (Leaf, error) {
			if item == nil {
				return Leaf{}, fmt.Errorf("nil item in leaf enumeration")
			}
			leaf, err := decodeLeafValue(item.Value)
			if err != nil {
				return Leaf{}, fmt.Errorf("decode leaf in enumeration: %w", err)
			}
			return leaf, nil
		},
	)
}

// UpdateLeaf updates a single leaf and recomputes the path to the root.
// This is a surgical update - only the affected path is recomputed, not the entire tree.
func (m *Tree) UpdateLeaf(ctx context.Context, stage, space string, leafIndex int, newLeaf Leaf) error {
	if err := validateStageSpace(stage, space); err != nil {
		return err
	}
	if leafIndex < 0 {
		return ErrInvalidIndex
	}
	if newLeaf.Hash == nil {
		return ErrInvalidLeaf
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return err
	}
	return m.updateLeafKnownCount(ctx, stage, space, leafIndex, leafCount, newLeaf)
}

// updateLeafKnownCount is the inner update path used by UpdateLeaf and RemoveLeaf to avoid
// double-fetching leaf count metadata.
func (m *Tree) updateLeafKnownCount(ctx context.Context, stage, space string, leafIndex, leafCount int, newLeaf Leaf) error {
	ctx, span := tracer.Start(ctx, "merkle.UpdateLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("leafIndex", leafIndex),
		))
	defer span.End()

	slog.DebugContext(ctx, "updating leaf", "stage", stage, "space", space, "leafIndex", leafIndex)

	if leafIndex >= leafCount {
		err := fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
		slog.ErrorContext(ctx, "leaf index out of bounds", "stage", stage, "space", space, "leafIndex", leafIndex, "leafCount", leafCount)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Combine the leaf write with the path-recompute writes into a single Batch.
	leafWrite := &kv.BatchItem{
		PK:    nodePK(stage, space, 0, leafIndex),
		Value: encodeLeafValue(newLeaf),
		Op:    kv.Put,
	}
	writes, err := m.computePathToRootWrites(ctx, stage, space, leafIndex, leafCount, newLeaf.Hash, nil, []*kv.BatchItem{leafWrite})
	if err != nil {
		slog.ErrorContext(ctx, "failed to compute path-to-root writes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := m.store.Batch(ctx, writes); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf and path", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "leaf updated", "stage", stage, "space", space, "leafIndex", leafIndex)
	return nil
}

// AddLeaf appends a new leaf to the tree and updates affected nodes.
// Returns the index at which the leaf was added.
//
// PERFORMANCE: O(log N) updates in most cases. When adding the leaf increases tree height
// (e.g., going from 15 to 16 leaves with branching=2), all internal nodes are recomputed,
// but this is still faster than full rebuild for large trees and happens rarely.
//
// INVARIANT: Leaf indices are append-only and stable. New leaf always gets index = current leaf count.
func (m *Tree) AddLeaf(ctx context.Context, stage, space string, newLeaf Leaf) (int, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return 0, err
	}
	if newLeaf.Hash == nil {
		return 0, ErrInvalidLeaf
	}

	ctx, span := tracer.Start(ctx, "merkle.AddLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
		))
	defer span.End()

	slog.DebugContext(ctx, "adding leaf", "stage", stage, "space", space)

	// Get current leaf count
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	newLeafCount := leafCount + 1
	oldMaxLevel := m.calculateMaxLevel(m.getPaddedCount(leafCount))
	newMaxLevel := m.calculateMaxLevel(m.getPaddedCount(newLeafCount))
	rebuild := newMaxLevel != oldMaxLevel || leafCount == 0

	if rebuild {
		// Tree height changed: full internal-node recompute. Persist the leaf, padding,
		// and updated leafcount first so the rebuild reads consistent state.
		partition := treePartition(stage, space)
		paddedCount := m.getPaddedCount(newLeafCount)
		pre := make([]*kv.BatchItem, 0, paddedCount-leafCount+1)
		pre = append(pre, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, leafCount),
			Value: encodeLeafValue(newLeaf),
			Op:    kv.Put,
		})
		for i := newLeafCount; i < paddedCount; i++ {
			pre = append(pre, &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, i),
				Value: cachedPaddingLeafVal,
				Op:    kv.Put,
			})
		}
		pre = append(pre, &kv.BatchItem{
			PK:    metaPKInPartition(partition, leafCountKey),
			Value: strconv.AppendInt(nil, int64(newLeafCount), 10),
			Op:    kv.Put,
		})
		if err := m.store.Batch(ctx, pre); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return 0, err
		}
		if err := m.recomputeAllInternalNodes(ctx, stage, space); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return 0, err
		}
		slog.DebugContext(ctx, "leaf added (rebuilt)", "stage", stage, "space", space, "leafIndex", leafCount)
		span.SetAttributes(attribute.Int("leafIndex", leafCount))
		return leafCount, nil
	}

	// Fast path: tree height unchanged. Combine leaf + padding + leafcount + path-recompute
	// + root cache into a SINGLE Batch on top of one GetBatch for sibling reads. Net cost
	// for AddLeaf in steady state: 1 Get(leafcount) + 1 GetBatch(siblings) + 1 Batch(writes).
	// Padding leaves and the new leaf are passed to the path recompute as known-hashes so
	// the GetBatch doesn't try to read rows that haven't been written yet.
	partition := treePartition(stage, space)
	paddedCount := m.getPaddedCount(newLeafCount)
	pre := make([]*kv.BatchItem, 0, paddedCount-leafCount+1)
	known := make(map[int][]byte, paddedCount-leafCount)
	pre = append(pre, &kv.BatchItem{
		PK:    nodePKInPartition(partition, 0, leafCount),
		Value: encodeLeafValue(newLeaf),
		Op:    kv.Put,
	})
	known[leafCount] = newLeaf.Hash
	for i := newLeafCount; i < paddedCount; i++ {
		pre = append(pre, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, i),
			Value: cachedPaddingLeafVal,
			Op:    kv.Put,
		})
		known[i] = cachedPaddingHash
	}
	pre = append(pre, &kv.BatchItem{
		PK:    metaPKInPartition(partition, leafCountKey),
		Value: strconv.AppendInt(nil, int64(newLeafCount), 10),
		Op:    kv.Put,
	})

	writes, err := m.computePathToRootWrites(ctx, stage, space, leafCount, newLeafCount, newLeaf.Hash, known, pre)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	if err := m.store.Batch(ctx, writes); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	slog.DebugContext(ctx, "leaf added", "stage", stage, "space", space, "leafIndex", leafCount)
	span.SetAttributes(attribute.Int("leafIndex", leafCount))
	return leafCount, nil
}

// RemoveLeaf marks a leaf as deleted (soft delete) and recomputes the path.
//
// IMPORTANT: This is a soft delete. The leaf is replaced with a deletion marker
// but the leaf count remains unchanged, and the leaf index is preserved.
// To check if a leaf is deleted, verify if its Ref field is empty.
//
// Rationale: Hard deletion would require reindexing all subsequent leaves,
// invalidating any external references to leaf indices. Soft delete maintains
// index stability at the cost of some storage overhead.
func (m *Tree) RemoveLeaf(ctx context.Context, stage, space string, leafIndex int) error {
	if err := validateStageSpace(stage, space); err != nil {
		return err
	}
	if leafIndex < 0 {
		return ErrInvalidIndex
	}

	ctx, span := tracer.Start(ctx, "merkle.RemoveLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("leafIndex", leafIndex),
		))
	defer span.End()

	slog.DebugContext(ctx, "removing leaf", "stage", stage, "space", space, "leafIndex", leafIndex)

	// INVARIANT: Soft delete preserves leaf indices. Hard delete would require reindexing
	// all subsequent leaves, breaking external references and catalog diffs.
	// Create a deleted marker leaf with domain-separated hash
	deletedLeaf := Leaf{
		Ref:  "",
		Hash: deletedHash(),
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Update with the deleted marker (reuses the leaf count we just fetched).
	if err := m.updateLeafKnownCount(ctx, stage, space, leafIndex, leafCount, deletedLeaf); err != nil {
		slog.ErrorContext(ctx, "failed to remove leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "leaf removed", "stage", stage, "space", space, "leafIndex", leafIndex)
	return nil
}

func (m *Tree) pruneOldNodes(ctx context.Context, stage, space string) error {
	return m.store.RemoveRange(ctx, treeRangeKey(stage, space))
}
