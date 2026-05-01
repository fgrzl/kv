package merkle

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// LeafMutation describes one ordered, index-based mutation against a Merkle tree leaf.
//
// If Append is true, the mutation appends a new leaf at the current tree end and Index is ignored.
// If Remove is true, the mutation soft-deletes the leaf at Index and Leaf.Hash is ignored.
// Otherwise the mutation updates the existing leaf at Index with Leaf.
type LeafMutation struct {
	Index  int
	Append bool
	Remove bool
	Leaf   Leaf
}

// ApplyLeafMutations applies ordered leaf mutations in a single batch-oriented pass.
//
// The method is index-based on purpose: callers that own higher-level ref/index mapping can
// resolve refs once, then hand the Merkle overlay an efficient batch of leaf mutations.
// Appends are assigned consecutive indexes at the end of the current tree and the returned
// slice reports the effective index for every input mutation in order.
func (m *Tree) ApplyLeafMutations(ctx context.Context, stage, space string, mutations []LeafMutation) (_ []int, err error) {
	if err := validateStageSpace(stage, space); err != nil {
		return nil, err
	}
	if len(mutations) == 0 {
		return []int{}, nil
	}

	ctx, span := tracer.Start(ctx, "merkle.ApplyLeafMutations",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("mutations", len(mutations)),
		),
	)
	defer span.End()

	slog.DebugContext(ctx, "applying leaf mutations", "stage", stage, "space", space, "mutations", len(mutations))

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	oldMaxLevel := m.calculateMaxLevel(m.getPaddedCount(leafCount))

	partition := treePartition(stage, space)
	resultIndexes := make([]int, len(mutations))
	changedIndexes := make([]int, 0, len(mutations))
	finalWrites := make(map[int]*kv.BatchItem, len(mutations))
	appendCount := 0

	for i, mutation := range mutations {
		switch {
		case mutation.Append && mutation.Remove:
			err = fmt.Errorf("append and remove cannot both be true for mutation %d", i)
			return nil, err
		case mutation.Append:
			if mutation.Leaf.Hash == nil {
				err = fmt.Errorf("append mutation %d requires a non-nil hash", i)
				return nil, err
			}

			index := leafCount + appendCount
			appendCount++
			resultIndexes[i] = index
			changedIndexes = append(changedIndexes, index)

			val, marshalErr := json.Marshal(mutation.Leaf)
			if marshalErr != nil {
				err = fmt.Errorf("marshal append mutation %d: %w", i, marshalErr)
				return nil, err
			}
			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: val,
				Op:    kv.Put,
			}

		case mutation.Remove:
			if mutation.Index < 0 {
				err = fmt.Errorf("remove mutation %d requires a non-negative index", i)
				return nil, err
			}
			if mutation.Index >= leafCount {
				err = fmt.Errorf("remove mutation %d targets index %d out of bounds (count: %d)", i, mutation.Index, leafCount)
				return nil, err
			}

			index := mutation.Index
			resultIndexes[i] = index
			changedIndexes = append(changedIndexes, index)

			deletedLeaf := Leaf{Ref: "", Hash: deletedHash()}
			val, marshalErr := json.Marshal(deletedLeaf)
			if marshalErr != nil {
				err = fmt.Errorf("marshal remove mutation %d: %w", i, marshalErr)
				return nil, err
			}
			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: val,
				Op:    kv.Put,
			}

		default:
			if mutation.Index < 0 {
				err = fmt.Errorf("update mutation %d requires a non-negative index", i)
				return nil, err
			}
			if mutation.Index >= leafCount {
				err = fmt.Errorf("update mutation %d targets index %d out of bounds (count: %d)", i, mutation.Index, leafCount)
				return nil, err
			}
			if mutation.Leaf.Hash == nil {
				err = fmt.Errorf("update mutation %d requires a non-nil hash", i)
				return nil, err
			}

			index := mutation.Index
			resultIndexes[i] = index
			changedIndexes = append(changedIndexes, index)

			val, marshalErr := json.Marshal(mutation.Leaf)
			if marshalErr != nil {
				err = fmt.Errorf("marshal update mutation %d: %w", i, marshalErr)
				return nil, err
			}
			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: val,
				Op:    kv.Put,
			}
		}
	}

	leafWriteIndexes := make([]int, 0, len(finalWrites))
	for index := range finalWrites {
		leafWriteIndexes = append(leafWriteIndexes, index)
	}
	sort.Ints(leafWriteIndexes)
	leafItems := make([]*kv.BatchItem, 0, len(leafWriteIndexes))
	for _, index := range leafWriteIndexes {
		leafItems = append(leafItems, finalWrites[index])
	}

	newLeafCount := leafCount + appendCount
	if len(leafItems) > 0 {
		if err := m.store.BatchChunks(ctx, enumerators.Slice(leafItems), m.batchSize); err != nil {
			slog.ErrorContext(ctx, "failed to persist leaf mutations", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}

	if appendCount > 0 {
		paddedCount := m.getPaddedCount(newLeafCount)
		if paddedCount != newLeafCount {
			if err := m.ensurePaddingExists(ctx, stage, space, newLeafCount, paddedCount); err != nil {
				slog.ErrorContext(ctx, "failed to add padding for batched mutations", "stage", stage, "space", space, "err", err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			for index := newLeafCount; index < paddedCount; index++ {
				changedIndexes = append(changedIndexes, index)
			}
		}
	}

	rootHash, err := m.recomputeChangedPaths(ctx, stage, space, changedIndexes, newLeafCount)
	if err != nil {
		slog.ErrorContext(ctx, "failed to recompute affected nodes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	metadataItems := []*kv.BatchItem{
		{
			PK:    rootPKInPartition(partition),
			Value: rootHash,
			Op:    kv.Put,
		},
	}
	if appendCount > 0 {
		metadataItems = append(metadataItems,
			&kv.BatchItem{
				PK:    metaPKInPartition(partition, leafCountKey),
				Value: []byte(fmt.Sprintf("%d", newLeafCount)),
				Op:    kv.Put,
			},
		)
		newMaxLevel := m.calculateMaxLevel(m.getPaddedCount(newLeafCount))
		if newMaxLevel != oldMaxLevel {
			metadataItems = append(metadataItems,
				&kv.BatchItem{
					PK:    metaPKInPartition(partition, maxLevelKey),
					Value: []byte(fmt.Sprintf("%d", newMaxLevel)),
					Op:    kv.Put,
				},
			)
		}
	}
	if err := m.commitBatchIfNeeded(ctx, metadataItems); err != nil {
		slog.ErrorContext(ctx, "failed to persist root metadata", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	slog.DebugContext(ctx, "leaf mutations applied", "stage", stage, "space", space, "mutations", len(mutations), "appends", appendCount)
	span.SetAttributes(attribute.Int("applies", len(mutations)), attribute.Int("appends", appendCount))
	return resultIndexes, nil
}

func (m *Tree) recomputeChangedPaths(ctx context.Context, stage, space string, changedLeafIndexes []int, leafCount int) ([]byte, error) {
	changedLeafIndexes = uniqueSortedInts(changedLeafIndexes)
	if len(changedLeafIndexes) == 0 {
		return nil, nil
	}

	partition := treePartition(stage, space)
	maxLevel := m.calculateMaxLevel(m.getPaddedCount(leafCount))
	currentChanged := changedLeafIndexes
	nextChanged := make([]int, 0, len(currentChanged))
	batch := make([]*kv.BatchItem, 0, len(currentChanged))
	var rootHash []byte

	for level := 1; level <= maxLevel; level++ {
		nextChanged = nextChanged[:0]
		var lastParentIndex int
		hasLastParent := false
		for _, index := range currentChanged {
			parentIndex := index / m.branching
			if hasLastParent && parentIndex == lastParentIndex {
				continue
			}
			nextChanged = append(nextChanged, parentIndex)
			lastParentIndex = parentIndex
			hasLastParent = true
		}

		if len(nextChanged) == 0 {
			break
		}

		parentHashes, err := m.getParentHashesInPartition(ctx, partition, level, nextChanged)
		if err != nil {
			return nil, err
		}

		batch = batch[:0]
		for i, parentIndex := range nextChanged {
			parentHash := parentHashes[i]
			if level == maxLevel {
				rootHash = parentHash
			}
			batch = append(batch, &kv.BatchItem{
				PK:    nodePKInPartition(partition, level, parentIndex),
				Value: parentHash,
				Op:    kv.Put,
			})
		}

		if err := m.commitBatchIfNeeded(ctx, batch); err != nil {
			return nil, err
		}
		currentChanged, nextChanged = nextChanged, currentChanged[:0]
	}

	if maxLevel == 0 {
		var err error
		rootHash, _, err = m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
		if err != nil {
			return nil, fmt.Errorf("get root hash for single-leaf tree after batched mutations: %w", err)
		}
	}
	if rootHash == nil {
		return nil, fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}
	return rootHash, nil
}

func (m *Tree) getParentHashesInPartition(ctx context.Context, partition lexkey.LexKey, parentLevel int, parentIndexes []int) ([][]byte, error) {
	if len(parentIndexes) == 0 {
		return nil, nil
	}

	childLevel := parentLevel - 1
	childKeys := make([]lexkey.PrimaryKey, 0, len(parentIndexes)*m.branching)
	for _, parentIndex := range parentIndexes {
		for i := 0; i < m.branching; i++ {
			childIndex := parentIndex*m.branching + i
			childKeys = append(childKeys, nodePKInPartition(partition, childLevel, childIndex))
		}
	}

	results, err := m.store.GetBatch(ctx, childKeys...)
	if err != nil {
		return nil, fmt.Errorf("store get batch: %w", err)
	}

	parentHashes := make([][]byte, 0, len(parentIndexes))
	childHashes := make([][]byte, 0, m.branching)
	resultIndex := 0
	for range parentIndexes {
		childHashes = childHashes[:0]
		for i := 0; i < m.branching; i++ {
			result := results[resultIndex]
			resultIndex++
			if !result.Found || result.Item == nil {
				continue
			}

			childHash, err := m.hashFromStoredValue(childLevel, result.Item.Value)
			if err != nil {
				return nil, err
			}
			if childHash != nil {
				childHashes = append(childHashes, childHash)
			}
		}
		parentHashes = append(parentHashes, hashByteSlices(childHashes))
	}

	return parentHashes, nil
}

func uniqueSortedInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}

	sort.Ints(values)
	writeIndex := 0
	for readIndex := 1; readIndex < len(values); readIndex++ {
		if values[readIndex] == values[writeIndex] {
			continue
		}
		writeIndex++
		values[writeIndex] = values[readIndex]
	}
	return values[:writeIndex+1]
}
