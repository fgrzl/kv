package merkle

import (
	"context"
	"fmt"
	"strconv"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

type pathReadLevel struct {
	parentLevel    int
	childLevel     int
	parentIndex    int
	pathChildIndex int
	childHashes    [][]byte
}

type pathReadSlot struct {
	levelOffset int
	childOffset int
	childLevel  int
	childIndex  int
}

// computePathToRootWrites resolves siblings, computes parent hashes along the
// leaf->root path, and returns a single batch containing the supplied prependedWrites
// (e.g. leaf put, padding puts, metadata puts) followed by the recomputed path and
// the root cache row. Issues exactly one GetBatch for sibling resolution.
//
// knownLeafHashes provides level-0 hashes the caller already has in memory (e.g. a
// freshly-added leaf or padding leaves not yet persisted). Indexes present in this
// map are NOT read from storage; their hashes are used directly for parent computation.
func (m *Tree) computePathToRootWrites(ctx context.Context, stage, space string, leafIndex, leafCount int, leafHash []byte, knownLeafHashes map[int][]byte, prependedWrites []*kv.BatchItem) ([]*kv.BatchItem, error) {
	if leafIndex >= leafCount {
		return nil, fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}
	partition := treePartition(stage, space)

	paddedLeafCount := m.getPaddedCount(leafCount)
	maxLevel := m.calculateMaxLevel(paddedLeafCount)
	if maxLevel == 0 {
		rootHash := leafHash
		if rootHash == nil {
			var err error
			rootHash, _, err = m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
			if err != nil {
				return nil, fmt.Errorf("get root hash for single-leaf tree: %w", err)
			}
		}
		if rootHash == nil {
			return nil, fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
		}
		writes := append(make([]*kv.BatchItem, 0, len(prependedWrites)+1), prependedWrites...)
		writes = append(writes, &kv.BatchItem{
			PK:    rootPKInPartition(partition),
			Value: rootHash,
			Op:    kv.Put,
		})
		return writes, nil
	}

	levelNodeCounts := m.levelNodeCounts(paddedLeafCount)
	pathLevels, readKeys, readSlots := m.planPathReads(partition, levelNodeCounts, leafIndex, leafHash != nil, knownLeafHashes)

	// Pre-fill any level-0 sibling hashes the caller already knows.
	for i, pathLevel := range pathLevels {
		if pathLevel.childLevel != 0 || len(knownLeafHashes) == 0 {
			continue
		}
		for childOffset := 0; childOffset < m.branching; childOffset++ {
			childIndex := pathLevel.parentIndex*m.branching + childOffset
			if childIndex == pathLevel.pathChildIndex {
				continue
			}
			if hash, ok := knownLeafHashes[childIndex]; ok {
				pathLevels[i].childHashes[childOffset] = hash
			}
		}
	}

	if len(readKeys) > 0 {
		results, err := m.store.GetBatch(ctx, readKeys...)
		if err != nil {
			return nil, fmt.Errorf("store get batch for recompute path: %w", err)
		}
		for resultIndex, result := range results {
			readSlot := readSlots[resultIndex]
			if !result.Found || result.Item == nil {
				if readSlot.childLevel == 0 && readSlot.childIndex >= paddedLeafCount {
					pathLevels[readSlot.levelOffset].childHashes[readSlot.childOffset] = cachedPaddingHash
					continue
				}
				return nil, fmt.Errorf("%w: missing sibling at level %d for path recompute (leaf %d)",
					errInvalidTreeState, readSlot.childLevel, leafIndex)
			}
			childHash, err := m.hashFromStoredValue(readSlot.childLevel, result.Item.Value)
			if err != nil {
				return nil, err
			}
			if childHash == nil {
				return nil, fmt.Errorf("%w: nil sibling hash at level %d for path recompute (leaf %d)",
					errInvalidTreeState, readSlot.childLevel, leafIndex)
			}
			pathLevels[readSlot.levelOffset].childHashes[readSlot.childOffset] = childHash
		}
	}

	writes := make([]*kv.BatchItem, 0, len(prependedWrites)+maxLevel+1)
	writes = append(writes, prependedWrites...)
	var rootHash []byte
	pendingHash := leafHash
	for _, pathLevel := range pathLevels {
		m.childBuf = m.childBuf[:0]
		for childOffset, childHash := range pathLevel.childHashes {
			childIndex := pathLevel.parentIndex*m.branching + childOffset
			if childIndex == pathLevel.pathChildIndex && pendingHash != nil {
				m.childBuf = append(m.childBuf, pendingHash)
				continue
			}
			if childHash != nil {
				m.childBuf = append(m.childBuf, childHash)
			}
		}

		parentHash := hashByteSlices(m.childBuf)
		writes = append(writes, &kv.BatchItem{
			PK:    nodePKInPartition(partition, pathLevel.parentLevel, pathLevel.parentIndex),
			Value: parentHash,
			Op:    kv.Put,
		})

		pendingHash = parentHash
		rootHash = parentHash
	}
	if rootHash == nil {
		return nil, fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}

	writes = append(writes, &kv.BatchItem{
		PK:    rootPKInPartition(partition),
		Value: rootHash,
		Op:    kv.Put,
	})

	return writes, nil
}

func (m *Tree) planPathReads(partition lexkey.LexKey, levelNodeCounts []int, leafIndex int, hasLeafHash bool, knownLeafHashes map[int][]byte) ([]pathReadLevel, []lexkey.PrimaryKey, []pathReadSlot) {
	maxLevel := len(levelNodeCounts) - 1
	pathLevels := make([]pathReadLevel, 0, maxLevel)
	readKeys := make([]lexkey.PrimaryKey, 0, maxLevel*max(1, m.branching-1))
	readSlots := make([]pathReadSlot, 0, cap(readKeys))

	currentIndex := leafIndex
	for parentLevel := 1; parentLevel <= maxLevel; parentLevel++ {
		childLevel := parentLevel - 1
		parentIndex := currentIndex / m.branching
		pathChildIndex := currentIndex
		childHashes := make([][]byte, m.branching)

		for childOffset := 0; childOffset < m.branching; childOffset++ {
			childIndex := parentIndex*m.branching + childOffset
			if childIndex >= levelNodeCounts[childLevel] {
				break
			}
			if childIndex == pathChildIndex && (parentLevel > 1 || hasLeafHash) {
				continue
			}
			// Caller may already have level-0 sibling hashes in memory (e.g. just-written
			// padding leaves). Skip the storage read for those positions; the caller
			// will pre-fill the slot before parent computation.
			if childLevel == 0 {
				if _, known := knownLeafHashes[childIndex]; known {
					continue
				}
			}
			readKeys = append(readKeys, nodePKInPartition(partition, childLevel, childIndex))
			readSlots = append(readSlots, pathReadSlot{
				levelOffset: len(pathLevels),
				childOffset: childOffset,
				childLevel:  childLevel,
			})
		}

		pathLevels = append(pathLevels, pathReadLevel{
			parentLevel:    parentLevel,
			childLevel:     childLevel,
			parentIndex:    parentIndex,
			pathChildIndex: pathChildIndex,
			childHashes:    childHashes,
		})
		currentIndex = parentIndex
	}

	return pathLevels, readKeys, readSlots
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

	// Batch padding creation for better performance
	batch := make([]*kv.BatchItem, 0, end-start)
	for i := start; i < end; i++ {
		pkVal := nodePKInPartition(partition, 0, i)
		batch = append(batch, &kv.BatchItem{PK: pkVal, Value: cachedPaddingLeafVal, Op: kv.Put})
	}

	if len(batch) > 0 {
		return m.store.Batch(ctx, batch)
	}
	return nil
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

	// Recompute each level bottom-up. Per-level loads are sequential because level N+1
	// reads stored hashes from level N, so the hashes for a level must be computed
	// before the next level starts. We still stream the accumulated writes through a
	// single BatchChunks call so the store can chunk and upload without extra roundtrips.
	counts := m.levelNodeCounts(paddedCount)
	prevHashes, err := m.loadLevelNodeHashesInOrderInPartition(ctx, partition, 0, counts[0])
	if err != nil {
		return err
	}
	var topRootHash []byte
	levelStreams := make([]enumerators.Enumerator[*kv.BatchItem], 0, max(0, maxLevel))
	for level := 1; level <= maxLevel; level++ {
		nodesAtPrevLevel := counts[level-1]
		nodesAtThisLevel := counts[level]
		nextHashes := make([][]byte, 0, nodesAtThisLevel)
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
			batch = append(batch, &kv.BatchItem{
				PK:    nodePKInPartition(partition, level, parentIdx),
				Value: parentHash,
				Op:    kv.Put,
			})
			nextHashes = append(nextHashes, parentHash)
			if level == maxLevel && parentIdx == 0 {
				topRootHash = parentHash
			}
		}

		// Top level: fold root cache and metadata into the same Batch.
		if level == maxLevel {
			if topRootHash == nil {
				return fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
			}
			batch = append(batch,
				&kv.BatchItem{PK: rootPKInPartition(partition), Value: topRootHash, Op: kv.Put},
				&kv.BatchItem{
					PK:    metaPKInPartition(partition, maxLevelKey),
					Value: strconv.AppendInt(nil, int64(maxLevel), 10),
					Op:    kv.Put,
				},
			)
		}
		levelStreams = append(levelStreams, enumerators.Slice(batch))
		prevHashes = nextHashes
	}
	return m.store.BatchChunks(ctx, enumerators.Chain(levelStreams...), m.batchSize)
}
