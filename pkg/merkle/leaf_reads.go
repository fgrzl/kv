package merkle

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/fgrzl/lexkey"
)

// cachedPaddingLeafVal is the precomputed binary encoding of the canonical padding leaf.
// Shared read-only across all padding writes; avoids per-call encoding.
var cachedPaddingLeafVal = mustEncodeLeafValue(Leaf{Hash: cachedPaddingHash})

func mustEncodeLeafValue(leaf Leaf) []byte {
	v, err := encodeLeafValueErr(leaf)
	if err != nil {
		panic(err)
	}
	return v
}

// encodeLeafValue encodes a Leaf to its compact binary wire format.
// Format: [4-byte uint32 LE: ref length][ref bytes][hash bytes]
// Panics if leaf.Ref length exceeds math.MaxUint32 (impossible on practical inputs).
func encodeLeafValue(leaf Leaf) []byte {
	return mustEncodeLeafValue(leaf)
}

func encodeLeafValueErr(leaf Leaf) ([]byte, error) {
	lr := len(leaf.Ref)
	if lr > math.MaxUint32 {
		return nil, fmt.Errorf("leaf ref length %d exceeds uint32 max", lr)
	}
	refLen := uint32(lr)
	buf := make([]byte, 4+int(refLen)+len(leaf.Hash))
	binary.LittleEndian.PutUint32(buf, refLen)
	copy(buf[4:], leaf.Ref)
	copy(buf[4+int(refLen):], leaf.Hash)
	return buf, nil
}

// decodeLeafValue decodes a Leaf from its compact binary wire format.
// Format: [4-byte uint32 LE: ref length][ref bytes][hash bytes]
func decodeLeafValue(value []byte) (Leaf, error) {
	if len(value) < 4 {
		return Leaf{}, fmt.Errorf("leaf value too short: %d bytes", len(value))
	}
	refLen := int(binary.LittleEndian.Uint32(value))
	if refLen > len(value)-4 {
		return Leaf{}, fmt.Errorf("leaf value corrupted: ref length %d exceeds data size %d", refLen, len(value))
	}
	hash := make([]byte, len(value)-(4+refLen))
	copy(hash, value[4+refLen:])
	return Leaf{
		Ref:  string(value[4 : 4+refLen]),
		Hash: hash,
	}, nil
}

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
			return nil, fmt.Errorf("decode leaf at index %d: %w", leafIndexes[i], err)
		}
		leaves[i] = leaf
	}

	return leaves, nil
}
