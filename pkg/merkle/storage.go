package merkle

import (
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
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
// Requires lexkey ordering of Encode("node", level, index) to follow integer index order.
func nodeLevelRangeQuery(stage, space string, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return nodeLevelRangeQueryInPartition(treePartition(stage, space), level, firstIndex, lastIndexInclusive)
}

func nodeLevelRangeQueryInPartition(partition lexkey.LexKey, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return kv.QueryArgs{
		PartitionKey: partition,
		StartRowKey:  lexkey.Encode("node", level, firstIndex),
		EndRowKey:    lexkey.Encode("node", level, lastIndexInclusive),
		Operator:     kv.Between,
	}
}

func nodePK(stage, space string, level, index int) lexkey.PrimaryKey {
	return nodePKInPartition(treePartition(stage, space), level, index)
}

func nodePKInPartition(partition lexkey.LexKey, level, index int) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		partition,
		lexkey.Encode("node", level, index),
	)
}

func rootPK(stage, space string) lexkey.PrimaryKey {
	return rootPKInPartition(treePartition(stage, space))
}

func rootPKInPartition(partition lexkey.LexKey) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(partition, lexkey.Encode(rootKey))
}

func metaPK(stage, space, metaName string) lexkey.PrimaryKey {
	return metaPKInPartition(treePartition(stage, space), metaName)
}

func metaPKInPartition(partition lexkey.LexKey, metaName string) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		partition,
		lexkey.Encode(metaPrefix, metaName),
	)
}
