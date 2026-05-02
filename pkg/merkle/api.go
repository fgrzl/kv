package merkle

import (
	"context"
	"fmt"
	"log/slog"

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

// GetRootHash returns the Merkle root hash and underlying value for a given stage/space.
func (m *Tree) GetRootHash(ctx context.Context, stage, space string) ([]byte, []byte, error) {
	return m.getHash(ctx, stage, space, rootRef)
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

	ctx, span := tracer.Start(ctx, "merkle.UpdateLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("leafIndex", leafIndex),
		))
	defer span.End()

	slog.DebugContext(ctx, "updating leaf", "stage", stage, "space", space, "leafIndex", leafIndex)

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if leafIndex >= leafCount {
		err := fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
		slog.ErrorContext(ctx, "leaf index out of bounds", "stage", stage, "space", space, "leafIndex", leafIndex, "leafCount", leafCount)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Persist the updated leaf
	pkVal := nodePK(stage, space, 0, leafIndex)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: encodeLeafValue(newLeaf)}); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Recompute path to root
	if err := m.recomputePathToRootWithLeafHash(ctx, stage, space, leafIndex, leafCount, newLeaf.Hash); err != nil {
		slog.ErrorContext(ctx, "failed to recompute path to root", "stage", stage, "space", space, "err", err)
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

	// Persist the new leaf
	pkVal := nodePK(stage, space, 0, leafCount)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: encodeLeafValue(newLeaf)}); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Check if we need to add padding
	newLeafCount := leafCount + 1
	if newLeafCount%m.branching != 0 {
		// Calculate target padded count
		paddedCount := ((newLeafCount / m.branching) + 1) * m.branching
		// Add padding nodes if needed
		if err := m.ensurePaddingExists(ctx, stage, space, newLeafCount, paddedCount); err != nil {
			slog.ErrorContext(ctx, "failed to add padding", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return 0, err
		}
	}

	// Update leaf count metadata BEFORE recomputing nodes
	if err := m.setLeafCountMetadata(ctx, stage, space, newLeafCount); err != nil {
		slog.ErrorContext(ctx, "failed to update leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Recompute affected paths
	if err := m.recomputeAffectedNodes(ctx, stage, space, leafCount, newLeafCount, newLeaf.Hash); err != nil {
		slog.ErrorContext(ctx, "failed to recompute affected nodes", "stage", stage, "space", space, "err", err)
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

	// Update with the deleted marker
	if err := m.UpdateLeaf(ctx, stage, space, leafIndex, deletedLeaf); err != nil {
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
