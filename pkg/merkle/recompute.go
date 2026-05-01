package merkle

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// recomputePathToRoot recomputes all parent hashes from a leaf to the root.
// PERFORMANCE: O(log N) nodes updated = tree height = log_branching(padded_leaf_count)
func (m *Tree) recomputePathToRoot(ctx context.Context, stage, space string, leafIndex int) error {
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return fmt.Errorf("get leaf count in recomputePathToRoot: %w", err)
	}

	return m.recomputePathToRootWithLeafCount(ctx, stage, space, leafIndex, leafCount)
}

func (m *Tree) recomputePathToRootWithLeafCount(ctx context.Context, stage, space string, leafIndex, leafCount int) error {
	if leafIndex >= leafCount {
		return fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}
	partition := treePartition(stage, space)

	// Calculate max level from current tree structure
	paddedLeafCount := m.getPaddedCount(leafCount)
	maxLevel := m.calculateMaxLevel(paddedLeafCount)
	var rootHash []byte

	// Recompute from leaf's parent up to root (O(log N) operations)
	currentIndex := leafIndex
	for level := 1; level <= maxLevel; level++ {
		parentIndex := currentIndex / m.branching

		// Get all sibling hashes for this parent
		childHashes, err := m.getChildHashesInPartition(ctx, partition, level-1, parentIndex)
		if err != nil {
			return err
		}

		// Compute parent hash
		parentHash := hashByteSlices(childHashes)

		// Store parent node
		pkVal := nodePKInPartition(partition, level, parentIndex)
		if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: parentHash}); err != nil {
			return err
		}
		if level == maxLevel {
			rootHash = parentHash
		}

		currentIndex = parentIndex
	}

	if maxLevel == 0 {
		var err error
		rootHash, _, err = m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
		if err != nil {
			return fmt.Errorf("get root hash for single-leaf tree: %w", err)
		}
	}

	if rootHash == nil {
		return fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}

	pkVal := rootPKInPartition(partition)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: rootHash}); err != nil {
		return fmt.Errorf("store root hash for %s/%s: %w", stage, space, err)
	}

	return nil
}

// getHash retrieves the stored hash and raw value for a node position.
func (m *Tree) getHash(ctx context.Context, stage, space string, ref NodePosition) ([]byte, []byte, error) {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = rootPK(stage, space)
	} else {
		pkVal = nodePK(stage, space, ref.Level, ref.Index)
	}
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return nil, nil, fmt.Errorf("store get: %w", err)
	}
	if item == nil {
		return nil, nil, nil
	}
	hash, err := m.hashFromStoredValue(ref.Level, item.Value)
	if err != nil {
		return nil, nil, err
	}
	return hash, item.Value, nil
}

func (m *Tree) getChildHashesInPartition(ctx context.Context, partition lexkey.LexKey, level, parentIndex int) ([][]byte, error) {
	// Reuse childBuf to avoid allocation on every call
	m.childBuf = m.childBuf[:0]
	m.childPKBuf = m.childPKBuf[:0]

	for i := 0; i < m.branching; i++ {
		childIndex := parentIndex*m.branching + i
		m.childPKBuf = append(m.childPKBuf, nodePKInPartition(partition, level, childIndex))
	}

	results, err := m.store.GetBatch(ctx, m.childPKBuf...)
	if err != nil {
		return nil, fmt.Errorf("store get batch: %w", err)
	}

	for _, result := range results {
		if !result.Found || result.Item == nil {
			continue
		}

		childHash, err := m.hashFromStoredValue(level, result.Item.Value)
		if err != nil {
			return nil, err
		}
		if childHash != nil {
			m.childBuf = append(m.childBuf, childHash)
		}
	}

	return m.childBuf, nil
}
func (m *Tree) loadLevelNodeHashesInOrderInPartition(ctx context.Context, partition lexkey.LexKey, level, nodeCount int) ([][]byte, error) {
	if nodeCount <= 0 {
		return nil, nil
	}
	args := nodeLevelRangeQueryInPartition(partition, level, 0, nodeCount-1)
	items, err := enumerators.ToSlice(m.store.Enumerate(ctx, args))
	if err != nil {
		return nil, err
	}
	if len(items) != nodeCount {
		return nil, fmt.Errorf("loadLevelNodeHashesInOrder: expected %d nodes at level %d, got %d", nodeCount, level, len(items))
	}
	out := make([][]byte, 0, nodeCount)
	for _, item := range items {
		if item == nil {
			return nil, fmt.Errorf("loadLevelNodeHashesInOrder: nil item at level %d", level)
		}
		h, err := m.hashFromStoredValue(level, item.Value)
		if err != nil {
			return nil, err
		}
		if h == nil {
			return nil, fmt.Errorf("loadLevelNodeHashesInOrder: nil hash at level %d", level)
		}
		out = append(out, h)
	}
	return out, nil
}

func (m *Tree) hashFromStoredValue(level int, value []byte) ([]byte, error) {
	if level != 0 {
		return value, nil
	}

	leaf, err := decodeLeafValue(value)
	if err != nil {
		return nil, err
	}

	return leaf.Hash, nil
}

// updateRootHash updates the root hash from the topmost internal node.
// The root hash is stored at both node/(maxLevel,0) and the root row key.
// The root row is a read optimization: GetRootHash can retrieve the root without
// loading maxLevel metadata or probing storage. The authoritative root is always
// the hash at (maxLevel, index=0); the root row is a cached copy.
func (m *Tree) updateRootHash(ctx context.Context, stage, space string, maxLevel int) error {
	if maxLevel < 0 {
		// Empty tree: no root to update
		return nil
	}
	rootHash, _, err := m.getHash(ctx, stage, space, NodePosition{Level: maxLevel, Index: 0})
	if err != nil {
		return fmt.Errorf("get root hash at level %d for %s/%s: %w", maxLevel, stage, space, err)
	}
	if rootHash == nil {
		// Should never happen: internal invariant violation
		return fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}
	pkVal := rootPK(stage, space)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: rootHash}); err != nil {
		return fmt.Errorf("store root hash for %s/%s: %w", stage, space, err)
	}
	return nil
}

// getPaddedCount returns the padded leaf count aligned to branching factor.
// INVARIANT: Padded count ensures complete tree levels (all internal nodes have exactly
// branching children, except possibly the rightmost node at each level).
func (m *Tree) getPaddedCount(leafCount int) int {
	if leafCount < 0 {
		panic(fmt.Sprintf("getPaddedCount: negative leaf count %d", leafCount))
	}
	if leafCount == 0 || leafCount%m.branching == 0 {
		return leafCount
	}
	return ((leafCount / m.branching) + 1) * m.branching
}

// calculateMaxLevel calculates the maximum tree level for a given leaf count.
// Returns 0 for single-leaf trees, -1 for empty trees.
// Level 0 = leaves, Level 1 = first internal level, etc.
func (m *Tree) calculateMaxLevel(leafCount int) int {
	if leafCount < 0 {
		panic(fmt.Sprintf("calculateMaxLevel: negative leaf count %d", leafCount))
	}
	if leafCount == 0 {
		return -1
	}
	if leafCount == 1 {
		return 0
	}
	level := 0
	nodes := leafCount
	for nodes > 1 {
		nodes = (nodes + m.branching - 1) / m.branching
		level++
	}
	return level
}

// ensurePaddingExists ensures padding leaves exist from start to end index.
// Uses batch operations for efficiency.
func (m *Tree) ensurePaddingExists(ctx context.Context, stage, space string, start, end int) error {
	if start >= end {
		return nil
	}
	partition := treePartition(stage, space)

	// INVARIANT: Padding aligns leaf count to branching factor for balanced tree structure
	paddingLeaf := Leaf{Hash: paddingHash()}
	val, err := json.Marshal(paddingLeaf)
	if err != nil {
		return err
	}

	// Batch padding creation for better performance
	batch := make([]*kv.BatchItem, 0, end-start)
	for i := start; i < end; i++ {
		pkVal := nodePKInPartition(partition, 0, i)
		// During AddLeaf, we know padding doesn't exist, so skip the Get check
		// This is safe because we control when padding is created
		batch = append(batch, &kv.BatchItem{PK: pkVal, Value: val, Op: kv.Put})
	}

	if len(batch) > 0 {
		return m.store.Batch(ctx, batch)
	}
	return nil
}

// recomputeAffectedNodes recomputes all nodes affected by adding a new leaf.
func (m *Tree) recomputeAffectedNodes(ctx context.Context, stage, space string, oldCount, newCount int) error {
	// Check if tree structure changed (new level added or empty tree)
	oldMaxLevel := m.calculateMaxLevel(m.getPaddedCount(oldCount))
	newMaxLevel := m.calculateMaxLevel(m.getPaddedCount(newCount))

	if newMaxLevel != oldMaxLevel || oldCount == 0 {
		return m.recomputeAllInternalNodes(ctx, stage, space)
	}

	// Otherwise just update the path from the new leaf
	return m.recomputePathToRootWithLeafCount(ctx, stage, space, oldCount, newCount)
}

// recomputeAllInternalNodes rebuilds all internal nodes from current leaves.
// Called when tree structure changes (height increase) or after corruption recovery.
// PERFORMANCE: O(N) for full rebuild, but rare - only when adding leaf changes tree height.
func (m *Tree) recomputeAllInternalNodes(ctx context.Context, stage, space string) error {
	partition := treePartition(stage, space)
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return fmt.Errorf("get leaf count in recomputeAllInternalNodes: %w", err)
	}
	if leafCount == 0 {
		// Empty tree: clear max level metadata
		return m.setMaxLevelMetadata(ctx, stage, space, -1)
	}

	// INVARIANT: Pad leaf count to multiple of branching factor for balanced tree
	paddedCount := m.getPaddedCount(leafCount)
	if paddedCount != leafCount {
		// Ensure padding leaves exist
		if err := m.ensurePaddingExists(ctx, stage, space, leafCount, paddedCount); err != nil {
			return err
		}
	}

	maxLevel := m.calculateMaxLevel(paddedCount)

	// Special case: single leaf (maxLevel == 0)
	if maxLevel == 0 {
		// Root hash is just the single leaf's hash
		leafHash, _, err := m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
		if err != nil {
			return err
		}
		if leafHash != nil {
			pkVal := rootPKInPartition(partition)
			if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: leafHash}); err != nil {
				return err
			}
		}
		return nil
	}

	// Recompute each level bottom-up (one range scan per level to load child hashes).
	for level := 1; level <= maxLevel; level++ {
		nodesAtPrevLevel := paddedCount
		for l := 1; l < level; l++ {
			nodesAtPrevLevel = (nodesAtPrevLevel + m.branching - 1) / m.branching
		}

		nodesAtThisLevel := (nodesAtPrevLevel + m.branching - 1) / m.branching
		prevLevel := level - 1
		prevHashes, err := m.loadLevelNodeHashesInOrderInPartition(ctx, partition, prevLevel, nodesAtPrevLevel)
		if err != nil {
			return err
		}

		batch := make([]*kv.BatchItem, 0, nodesAtThisLevel)
		for parentIdx := 0; parentIdx < nodesAtThisLevel; parentIdx++ {
			start := parentIdx * m.branching
			if start >= nodesAtPrevLevel {
				break
			}
			end := min(start+m.branching, nodesAtPrevLevel)
			childHashes := prevHashes[start:end]
			if len(childHashes) == 0 {
				continue
			}
			parentHash := hashByteSlices(childHashes)
			pkVal := nodePKInPartition(partition, level, parentIdx)
			batch = append(batch, &kv.BatchItem{PK: pkVal, Value: parentHash, Op: kv.Put})
		}

		if err := m.commitBatchIfNeeded(ctx, batch); err != nil {
			return err
		}
	}

	// Update root hash and persist max level metadata
	if err := m.updateRootHash(ctx, stage, space, maxLevel); err != nil {
		return fmt.Errorf("update root hash in recomputeAllInternalNodes: %w", err)
	}
	return m.setMaxLevelMetadata(ctx, stage, space, maxLevel)
}
