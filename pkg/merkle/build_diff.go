package merkle

import (
	"bytes"
	"context"
	"fmt"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/zeebo/blake3"
)

// collectBuildPutBatchItems materializes all Put operations for a full tree build.
// paddedNodes is the padded leaf tier; actualLeafCount is the pre-padding leaf count.
func (m *Tree) collectBuildPutBatchItems(stage, space string, leaves enumerators.Enumerator[Leaf]) (paddedNodes []Branch, batchItems []*kv.BatchItem, actualLeafCount int, err error) {
	partition := treePartition(stage, space)
	currNodes := make([]Branch, 0, 1000)
	batchItems = make([]*kv.BatchItem, 0, 1024)
	index := 0

	if err := enumerators.ForEach(leaves, func(leaf Leaf) error {
		batchItems = append(batchItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, index),
			Value: encodeLeafValue(leaf),
			Op:    kv.Put,
		})
		currNodes = append(currNodes, Branch{Hash: leaf.Hash, Level: 0, Index: index})
		index++
		return nil
	}); err != nil {
		return nil, nil, 0, err
	}
	if len(currNodes) == 0 {
		return nil, nil, 0, nil
	}
	actualLeafCount = len(currNodes)

	for len(currNodes)%m.branching != 0 {
		idx := len(currNodes)
		batchItems = append(batchItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, idx),
			Value: cachedPaddingLeafVal,
			Op:    kv.Put,
		})
		currNodes = append(currNodes, Branch{Hash: cachedPaddingHash, Level: 0, Index: idx})
	}

	nodes := currNodes
	level := 1
	for len(nodes) > 1 {
		var next []Branch
		var batch []*kv.BatchItem
		next, batch = m.hashNodeLevelInPartition(partition, nodes, level)
		batchItems = append(batchItems, batch...)
		nodes = next
		level++
	}
	if len(nodes) != 1 {
		return nil, nil, 0, fmt.Errorf("collectBuildPutBatchItems: expected 1 root node, got %d", len(nodes))
	}
	root := nodes[0]
	batchItems = append(batchItems, &kv.BatchItem{
		PK:    rootPKInPartition(partition),
		Value: root.Hash,
		Op:    kv.Put,
	})

	maxLevel := m.calculateMaxLevel(len(currNodes))
	batchItems = append(batchItems,
		&kv.BatchItem{
			PK:    metaPKInPartition(partition, leafCountKey),
			Value: []byte(fmt.Sprintf("%d", actualLeafCount)),
			Op:    kv.Put,
		},
		&kv.BatchItem{
			PK:    metaPKInPartition(partition, maxLevelKey),
			Value: []byte(fmt.Sprintf("%d", maxLevel)),
			Op:    kv.Put,
		},
	)

	return currNodes, batchItems, actualLeafCount, nil
}
func (m *Tree) hashNodeLevelInPartition(partition lexkey.LexKey, nodes []Branch, level int) ([]Branch, []*kv.BatchItem) {
	numGroups := (len(nodes) + m.branching - 1) / m.branching
	next := make([]Branch, 0, numGroups)
	batch := make([]*kv.BatchItem, 0, numGroups)

	for i := 0; i < len(nodes); i += m.branching {
		end := min(i+m.branching, len(nodes))
		sum := hashNodes(nodes[i:end])
		pkVal := nodePKInPartition(partition, level, i/m.branching)
		batch = append(batch, &kv.BatchItem{PK: pkVal, Value: sum, Op: kv.Put})
		next = append(next, Branch{Hash: sum, Level: level, Index: i / m.branching})
	}
	return next, batch
}

func (m *Tree) commitBatchIfNeeded(ctx context.Context, batch []*kv.BatchItem) error {
	if len(batch) == 0 {
		return nil
	}
	return m.store.Batch(ctx, batch)
}

func hashNodes(nodes []Branch) []byte {
	h := hasherPool.Get().(*blake3.Hasher)
	h.Reset()
	for _, n := range nodes {
		h.Write(n.Hash)
	}
	sum := h.Sum(nil)
	hasherPool.Put(h)
	return sum
}

func (m *Tree) diffNode(ctx context.Context, prev, curr, space string, ref NodePosition) enumerators.Enumerator[Leaf] {
	prevHash, _, errPrev := m.getHash(ctx, prev, space, ref)
	currHash, currVal, errCurr := m.getHash(ctx, curr, space, ref)

	if errPrev != nil || errCurr != nil {
		var combined error
		if errPrev != nil && errCurr != nil {
			combined = fmt.Errorf("get hash errors: prev=%v curr=%v", errPrev, errCurr)
		} else if errPrev != nil {
			combined = fmt.Errorf("get hash prev: %w", errPrev)
		} else {
			combined = fmt.Errorf("get hash curr: %w", errCurr)
		}
		return enumerators.Error[Leaf](combined)
	}

	if ref == rootRef {
		if prevHash == nil && currHash == nil {
			return enumerators.Empty[Leaf]()
		}
		if prevHash == nil && currHash != nil {
			return m.scanLeaves(ctx, curr, space)
		}
		if currHash == nil && prevHash != nil {
			return enumerators.Empty[Leaf]()
		}
	}

	if prevHash != nil && currHash != nil && bytes.Equal(prevHash, currHash) {
		return enumerators.Empty[Leaf]()
	}

	if ref.Level == 0 {
		if currHash != nil && (prevHash == nil || !bytes.Equal(prevHash, currHash)) {
			return decodeLeafEnumerator(currVal)
		}
		return enumerators.Empty[Leaf]()
	}

	if prevHash == nil && currHash != nil {
		return m.walkSubtree(ctx, curr, space, ref)
	}
	if currHash == nil {
		return enumerators.Empty[Leaf]()
	}

	if ref.Level < 0 {
		ref.Level = m.findMaxLevel(ctx, curr, space)
		if ref.Level < 0 {
			return enumerators.Empty[Leaf]()
		}
	}

	chunks := m.diffChildNodes(ctx, prev, curr, space, ref)
	if len(chunks) == 0 {
		return enumerators.Empty[Leaf]()
	}
	return enumerators.FlatMap(enumerators.Slice(chunks), func(child NodePosition) enumerators.Enumerator[Leaf] {
		return m.diffNode(ctx, prev, curr, space, child)
	})
}

func decodeLeafEnumerator(val []byte) enumerators.Enumerator[Leaf] {
	if val == nil {
		return enumerators.Empty[Leaf]()
	}
	leaf, err := decodeLeafValue(val)
	if err == nil && leaf.Ref != "" {
		return enumerators.Slice([]Leaf{leaf})
	}
	return enumerators.Empty[Leaf]()
}

func (m *Tree) diffChildNodes(ctx context.Context, prev, curr, space string, ref NodePosition) []NodePosition {
	chunks := make([]NodePosition, 0, m.branching)
	if m.branching <= 0 {
		return chunks
	}

	childLevel := ref.Level - 1
	prevPartition := treePartition(prev, space)
	currPartition := treePartition(curr, space)
	childNodes := make([]NodePosition, m.branching)
	prevKeys := make([]lexkey.PrimaryKey, m.branching)
	currKeys := make([]lexkey.PrimaryKey, m.branching)

	for i := 0; i < m.branching; i++ {
		child := NodePosition{Level: childLevel, Index: ref.Index*m.branching + i}
		childNodes[i] = child
		prevKeys[i] = nodePKInPartition(prevPartition, child.Level, child.Index)
		currKeys[i] = nodePKInPartition(currPartition, child.Level, child.Index)
	}

	prevResults, prevErr := m.store.GetBatch(ctx, prevKeys...)
	currResults, currErr := m.store.GetBatch(ctx, currKeys...)

	for i := 0; i < m.branching; i++ {
		prevFound := false
		if prevErr == nil {
			prevFound = prevResults[i].Found && prevResults[i].Item != nil
		} else {
			prevHash, _, _ := m.getHash(ctx, prev, space, childNodes[i])
			prevFound = prevHash != nil
		}

		currFound := false
		if currErr == nil {
			currFound = currResults[i].Found && currResults[i].Item != nil
		} else {
			currHash, _, _ := m.getHash(ctx, curr, space, childNodes[i])
			currFound = currHash != nil
		}

		if prevFound || currFound {
			chunks = append(chunks, childNodes[i])
		}
	}

	return chunks
}

func (m *Tree) findMaxLevel(ctx context.Context, stage, space string) int {
	if level, ok, err := m.getMaxLevelMetadata(ctx, stage, space); err == nil && ok {
		return level
	}

	for h := 0; ; h++ {
		pkVal := nodePK(stage, space, h, 0)
		item, err := m.store.Get(ctx, pkVal)
		if err != nil || item == nil {
			return h - 1
		}
	}
}

func (m *Tree) walkSubtree(ctx context.Context, stage, space string, ref NodePosition) enumerators.Enumerator[Leaf] {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = rootPK(stage, space)
	} else {
		pkVal = nodePK(stage, space, ref.Level, ref.Index)
	}
	item, err := m.store.Get(ctx, pkVal)
	if err != nil || item == nil {
		return enumerators.Empty[Leaf]()
	}
	if ref.Level == 0 {
		return decodeLeafEnumerator(item.Value)
	}
	chunks := make([]NodePosition, 0, m.branching)
	for i := 0; i < m.branching; i++ {
		chunks = append(chunks, NodePosition{Level: ref.Level - 1, Index: ref.Index*m.branching + i})
	}
	return enumerators.FlatMap(enumerators.Slice(chunks), func(child NodePosition) enumerators.Enumerator[Leaf] {
		return m.walkSubtree(ctx, stage, space, child)
	})
}

// scanLeaves enumerates all non-deleted leaves for a given stage/space.
// Used for empty/non-empty diff edge cases. Returns a lazy enumerator.
func (m *Tree) scanLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	return enumerators.FilterMap(
		m.EnumerateLeaves(ctx, stage, space),
		func(leaf Leaf) (Leaf, bool, error) {
			return leaf, leaf.Ref != "", nil
		},
	)
}
