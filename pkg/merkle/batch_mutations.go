package merkle

import (
	"context"
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

const rangeLoadThreshold = 32

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
	if isAppendOnlyBatch(mutations) {
		resultIndexes, err := m.applyAppendOnlyMutations(ctx, stage, space, leafCount, oldMaxLevel, mutations)
		if err != nil {
			slog.ErrorContext(ctx, "failed to apply append-only mutations", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		slog.DebugContext(ctx, "append-only leaf mutations applied", "stage", stage, "space", space, "mutations", len(mutations))
		span.SetAttributes(attribute.Int("applies", len(mutations)), attribute.Int("appends", len(mutations)))
		return resultIndexes, nil
	}

	partition := treePartition(stage, space)
	resultIndexes := make([]int, len(mutations))
	finalWrites := make(map[int]*kv.BatchItem, len(mutations))
	changedLeafHashes := make(map[int][]byte, len(mutations)+m.branching)
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

			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: encodeLeafValue(mutation.Leaf),
				Op:    kv.Put,
			}
			changedLeafHashes[index] = mutation.Leaf.Hash

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

			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: encodeLeafValue(Leaf{Hash: cachedDeletedHash}),
				Op:    kv.Put,
			}
			changedLeafHashes[index] = cachedDeletedHash

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

			finalWrites[index] = &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: encodeLeafValue(mutation.Leaf),
				Op:    kv.Put,
			}
			changedLeafHashes[index] = mutation.Leaf.Hash
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
	currentIndexes := append(make([]int, 0, len(leafWriteIndexes)+m.branching), leafWriteIndexes...)
	if appendCount > 0 {
		paddedCount := m.getPaddedCount(newLeafCount)
		for index := newLeafCount; index < paddedCount; index++ {
			leafItems = append(leafItems, &kv.BatchItem{
				PK:    nodePKInPartition(partition, 0, index),
				Value: cachedPaddingLeafVal,
				Op:    kv.Put,
			})
			changedLeafHashes[index] = cachedPaddingHash
			currentIndexes = append(currentIndexes, index)
		}
	}

	rootHash, internalItems, err := m.computeInternalNodeWrites(ctx, partition, m.getPaddedCount(newLeafCount), changedLeafHashes, currentIndexes)
	if err != nil {
		slog.ErrorContext(ctx, "failed to recompute affected nodes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	allItems := make([]*kv.BatchItem, 0, len(leafItems)+len(internalItems)+3)
	allItems = append(allItems, leafItems...)
	allItems = append(allItems, internalItems...)
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
	allItems = append(allItems, metadataItems...)
	if err := m.store.BatchChunks(ctx, enumerators.Slice(allItems), m.batchSize); err != nil {
		slog.ErrorContext(ctx, "failed to persist root metadata", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	slog.DebugContext(ctx, "leaf mutations applied", "stage", stage, "space", space, "mutations", len(mutations), "appends", appendCount)
	span.SetAttributes(attribute.Int("applies", len(mutations)), attribute.Int("appends", appendCount))
	return resultIndexes, nil
}

func isAppendOnlyBatch(mutations []LeafMutation) bool {
	if len(mutations) == 0 {
		return false
	}
	for _, mutation := range mutations {
		if !mutation.Append || mutation.Remove {
			return false
		}
	}
	return true
}

func (m *Tree) applyAppendOnlyMutations(ctx context.Context, stage, space string, leafCount, oldMaxLevel int, mutations []LeafMutation) ([]int, error) {
	partition := treePartition(stage, space)
	resultIndexes := make([]int, len(mutations))
	newLeafCount := leafCount + len(mutations)
	paddedCount := m.getPaddedCount(newLeafCount)
	newMaxLevel := m.calculateMaxLevel(paddedCount)

	leafItems := make([]*kv.BatchItem, 0, paddedCount-leafCount)
	currentHashes := make(map[int][]byte, paddedCount-leafCount)
	currentIndexes := make([]int, 0, paddedCount-leafCount)
	for i, mutation := range mutations {
		if mutation.Leaf.Hash == nil {
			return nil, fmt.Errorf("append mutation %d requires a non-nil hash", i)
		}

		index := leafCount + i
		resultIndexes[i] = index

		leafItems = append(leafItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, index),
			Value: encodeLeafValue(mutation.Leaf),
			Op:    kv.Put,
		})
		currentHashes[index] = mutation.Leaf.Hash
		currentIndexes = append(currentIndexes, index)
	}

	for index := newLeafCount; index < paddedCount; index++ {
		leafItems = append(leafItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, index),
			Value: cachedPaddingLeafVal,
			Op:    kv.Put,
		})
		currentHashes[index] = cachedPaddingHash
		currentIndexes = append(currentIndexes, index)
	}

	rootHash, internalItems, err := m.computeInternalNodeWrites(ctx, partition, paddedCount, currentHashes, currentIndexes)
	if err != nil {
		return nil, err
	}

	if rootHash == nil {
		return nil, fmt.Errorf("%w: append-only apply produced nil root for %s/%s", errInvalidTreeState, stage, space)
	}

	items := make([]*kv.BatchItem, 0, len(leafItems)+len(internalItems)+3)
	items = append(items, leafItems...)
	items = append(items, internalItems...)
	items = append(items,
		&kv.BatchItem{
			PK:    rootPKInPartition(partition),
			Value: rootHash,
			Op:    kv.Put,
		},
		&kv.BatchItem{
			PK:    metaPKInPartition(partition, leafCountKey),
			Value: []byte(fmt.Sprintf("%d", newLeafCount)),
			Op:    kv.Put,
		},
	)
	if newMaxLevel != oldMaxLevel {
		items = append(items, &kv.BatchItem{
			PK:    metaPKInPartition(partition, maxLevelKey),
			Value: []byte(fmt.Sprintf("%d", newMaxLevel)),
			Op:    kv.Put,
		})
	}

	if err := m.store.BatchChunks(ctx, enumerators.Slice(items), m.batchSize); err != nil {
		return nil, err
	}

	return resultIndexes, nil
}

func (m *Tree) computeInternalNodeWrites(ctx context.Context, partition lexkey.LexKey, paddedCount int, currentHashes map[int][]byte, currentIndexes []int) ([]byte, []*kv.BatchItem, error) {
	if paddedCount <= 0 {
		return nil, nil, nil
	}

	levelNodeCounts := m.levelNodeCounts(paddedCount)
	rootHash := currentHashes[0]
	internalItems := make([]*kv.BatchItem, 0, len(currentIndexes)*max(1, len(levelNodeCounts)-1))
	childHashes := make([][]byte, 0, m.branching)

	for parentLevel := 1; parentLevel < len(levelNodeCounts); parentLevel++ {
		parentIndexes := changedParentIndexesFromSorted(currentIndexes, m.branching)
		if len(parentIndexes) == 0 {
			break
		}

		loadedHashes, err := m.loadMissingNodeHashesInPartition(ctx, partition, parentLevel-1, levelNodeCounts[parentLevel-1], parentIndexes, currentHashes)
		if err != nil {
			return nil, nil, err
		}

		nextHashes := make(map[int][]byte, len(parentIndexes))
		for _, parentIndex := range parentIndexes {
			childHashes = childHashes[:0]
			for offset := 0; offset < m.branching; offset++ {
				childIndex := parentIndex*m.branching + offset
				if childIndex >= levelNodeCounts[parentLevel-1] {
					break
				}
				if childHash, ok := currentHashes[childIndex]; ok {
					childHashes = append(childHashes, childHash)
					continue
				}
				if childHash, ok := loadedHashes[childIndex]; ok && childHash != nil {
					childHashes = append(childHashes, childHash)
				}
			}

			parentHash := hashByteSlices(childHashes)
			nextHashes[parentIndex] = parentHash
			internalItems = append(internalItems, &kv.BatchItem{
				PK:    nodePKInPartition(partition, parentLevel, parentIndex),
				Value: parentHash,
				Op:    kv.Put,
			})
			if parentLevel == len(levelNodeCounts)-1 {
				rootHash = parentHash
			}
		}

		currentHashes = nextHashes
		currentIndexes = parentIndexes
	}

	return rootHash, internalItems, nil
}

func (m *Tree) levelNodeCounts(leafCount int) []int {
	if leafCount <= 0 {
		return nil
	}

	counts := []int{leafCount}
	for nodes := leafCount; nodes > 1; {
		nodes = (nodes + m.branching - 1) / m.branching
		counts = append(counts, nodes)
	}
	return counts
}

func changedParentIndexesFromSorted(sortedIndexes []int, branching int) []int {
	if len(sortedIndexes) == 0 {
		return nil
	}

	parentIndexes := make([]int, 0, len(sortedIndexes))
	lastParent := -1
	for _, index := range sortedIndexes {
		parentIndex := index / branching
		if parentIndex == lastParent {
			continue
		}
		parentIndexes = append(parentIndexes, parentIndex)
		lastParent = parentIndex
	}
	return parentIndexes
}

func (m *Tree) loadMissingNodeHashesInPartition(ctx context.Context, partition lexkey.LexKey, level, nodeCount int, parentIndexes []int, knownHashes map[int][]byte) (map[int][]byte, error) {
	m.bulkChildIdxBuf = m.bulkChildIdxBuf[:0]
	m.bulkChildPKBuf = m.bulkChildPKBuf[:0]
	for _, parentIndex := range parentIndexes {
		for offset := 0; offset < m.branching; offset++ {
			childIndex := parentIndex*m.branching + offset
			if childIndex >= nodeCount {
				break
			}
			if _, ok := knownHashes[childIndex]; ok {
				continue
			}
			m.bulkChildIdxBuf = append(m.bulkChildIdxBuf, childIndex)
			m.bulkChildPKBuf = append(m.bulkChildPKBuf, nodePKInPartition(partition, level, childIndex))
		}
	}
	if len(m.bulkChildPKBuf) == 0 {
		return nil, nil
	}

	ranges := contiguousRanges(m.bulkChildIdxBuf)
	batchIndexes := make([]int, 0, len(m.bulkChildIdxBuf))
	batchKeys := make([]lexkey.PrimaryKey, 0, len(m.bulkChildPKBuf))
	loadedHashes := make(map[int][]byte, len(m.bulkChildIdxBuf))
	readCursor := 0
	for _, indexRange := range ranges {
		rangeLen := indexRange.end - indexRange.start + 1
		if rangeLen < rangeLoadThreshold {
			for index := indexRange.start; index <= indexRange.end; index++ {
				batchIndexes = append(batchIndexes, index)
				batchKeys = append(batchKeys, m.bulkChildPKBuf[readCursor])
				readCursor++
			}
			continue
		}

		items, err := enumerators.ToSlice(m.store.Enumerate(ctx, nodeLevelRangeQueryInPartition(partition, level, indexRange.start, indexRange.end)))
		if err != nil {
			return nil, fmt.Errorf("store enumerate: %w", err)
		}
		if len(items) != rangeLen {
			return nil, fmt.Errorf("load missing node hashes: expected %d items at level %d for range [%d,%d], got %d", rangeLen, level, indexRange.start, indexRange.end, len(items))
		}
		for offset, item := range items {
			readCursor++
			if item == nil {
				return nil, fmt.Errorf("load missing node hashes: nil item at level %d for range [%d,%d]", level, indexRange.start, indexRange.end)
			}
			childHash, err := m.hashFromStoredValue(level, item.Value)
			if err != nil {
				return nil, err
			}
			if childHash != nil {
				loadedHashes[indexRange.start+offset] = childHash
			}
		}
	}

	if len(batchKeys) == 0 {
		return loadedHashes, nil
	}

	results, err := m.store.GetBatch(ctx, batchKeys...)
	if err != nil {
		return nil, fmt.Errorf("store get batch: %w", err)
	}

	for i, result := range results {
		if !result.Found || result.Item == nil {
			continue
		}

		childHash, err := m.hashFromStoredValue(level, result.Item.Value)
		if err != nil {
			return nil, err
		}
		if childHash != nil {
			loadedHashes[batchIndexes[i]] = childHash
		}
	}

	return loadedHashes, nil
}

type intRange struct {
	start int
	end   int
}

func contiguousRanges(sortedIndexes []int) []intRange {
	if len(sortedIndexes) == 0 {
		return nil
	}

	ranges := make([]intRange, 0, 1)
	start := sortedIndexes[0]
	end := start
	for _, index := range sortedIndexes[1:] {
		if index == end+1 {
			end = index
			continue
		}
		ranges = append(ranges, intRange{start: start, end: end})
		start = index
		end = index
	}
	ranges = append(ranges, intRange{start: start, end: end})
	return ranges
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
