package merkle

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fgrzl/lexkey"
)

// GetLeavesByIndex returns the leaves for the provided indexes in the same order as the input.
//
// The method is optimized for callers that already resolved refs to indexes and want a single
// batch read instead of repeated GetLeaf calls. Missing or out-of-range indexes are treated as
// errors because they indicate either a stale mapping or tree corruption.
func (m *Tree) GetLeavesByIndex(ctx context.Context, stage, space string, leafIndexes []int) ([]Leaf, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return nil, err
	}
	if len(leafIndexes) == 0 {
		return []Leaf{}, nil
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return nil, err
	}

	partition := treePartition(stage, space)
	pks := make([]lexkey.PrimaryKey, len(leafIndexes))
	for i, leafIndex := range leafIndexes {
		if leafIndex < 0 {
			return nil, fmt.Errorf("leaf index %d must be non-negative", leafIndex)
		}
		if leafIndex >= leafCount {
			return nil, fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
		}
		pks[i] = nodePKInPartition(partition, 0, leafIndex)
	}

	results, err := m.store.GetBatch(ctx, pks...)
	if err != nil {
		return nil, fmt.Errorf("store get batch: %w", err)
	}

	leaves := make([]Leaf, len(results))
	for i, result := range results {
		if !result.Found || result.Item == nil {
			return nil, fmt.Errorf("leaf not found at index %d", leafIndexes[i])
		}
		leaf, err := decodeLeafValue(result.Item.Value)
		if err != nil {
			return nil, fmt.Errorf("unmarshal leaf at index %d: %w", leafIndexes[i], err)
		}
		leaves[i] = leaf
	}

	return leaves, nil
}

func decodeLeafValue(value []byte) (Leaf, error) {
	leaf := leafPool.Get().(*Leaf)
	defer func() {
		*leaf = Leaf{}
		leafPool.Put(leaf)
	}()

	if err := json.Unmarshal(value, leaf); err != nil {
		return Leaf{}, fmt.Errorf("unmarshal leaf: %w", err)
	}

	return *leaf, nil
}
