package merkle

import (
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Row-key discriminator bytes. A single-byte prefix is shorter than the legacy
// "node"/"meta"/"root" string prefixes and packs more keys per OData GetBatch
// filter on Azure Table Storage. The byte values are chosen so that the natural
// sort order is node < meta < root, putting tree data first in any partition scan.
//
// Bumping any of these requires bumping formatVersion (see state.go).
const (
	discNode byte = 0x01
	discMeta byte = 0x02
	discRoot byte = 0x03
)

// validateStageSpace validates that stage and space are non-empty.
func validateStageSpace(stage, space string) error {
	if stage == "" {
		return ErrInvalidStage
	}
	if space == "" {
		return ErrInvalidSpace
	}
	return nil
}

func treePartition(stage, space string) lexkey.LexKey {
	return lexkey.Encode("merkle", stage, space)
}

// treeRangeKey covers every row in the Merkle tree partition for this stage/space.
func treeRangeKey(stage, space string) lexkey.RangeKey {
	return lexkey.NewRangeKeyFull(treePartition(stage, space))
}

// nodeLevelRangeQuery returns a partition-scoped row-key Between query for Merkle nodes at
// the given level with indices in [firstIndex, lastIndexInclusive].
// INVARIANT: lexkey encodes int parts as fixed-width big-endian (sign-flipped),
// so Encode(disc, level, index) is monotonic in (level, index).
func nodeLevelRangeQuery(stage, space string, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return nodeLevelRangeQueryInPartition(treePartition(stage, space), level, firstIndex, lastIndexInclusive)
}

func nodeLevelRangeQueryInPartition(partition lexkey.LexKey, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return kv.QueryArgs{
		PartitionKey: partition,
		StartRowKey:  encodeNodeRowKey(uint8(level), uint64(firstIndex)),
		EndRowKey:    encodeNodeRowKey(uint8(level), uint64(lastIndexInclusive)),
		Operator:     kv.Between,
	}
}

// encodeNodeRowKey returns the canonical row-key bytes for an internal/leaf node.
// Layout: [disc:1][sep][level:uint8][sep][index:uint64].
func encodeNodeRowKey(level uint8, index uint64) lexkey.LexKey {
	return lexkey.Encode([]byte{discNode}, level, index)
}

func encodeMetaRowKey(metaName string) lexkey.LexKey {
	return lexkey.Encode([]byte{discMeta}, metaName)
}

func encodeRootRowKey() lexkey.LexKey {
	return lexkey.Encode([]byte{discRoot})
}

func nodePK(stage, space string, level, index int) lexkey.PrimaryKey {
	return nodePKInPartition(treePartition(stage, space), level, index)
}

func nodePKInPartition(partition lexkey.LexKey, level, index int) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(partition, encodeNodeRowKey(uint8(level), uint64(index)))
}

func rootPK(stage, space string) lexkey.PrimaryKey {
	return rootPKInPartition(treePartition(stage, space))
}

func rootPKInPartition(partition lexkey.LexKey) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(partition, encodeRootRowKey())
}

func metaPK(stage, space, metaName string) lexkey.PrimaryKey {
	return metaPKInPartition(treePartition(stage, space), metaName)
}

func metaPKInPartition(partition lexkey.LexKey, metaName string) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(partition, encodeMetaRowKey(metaName))
}
