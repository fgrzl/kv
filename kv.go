package kv

import "github.com/fgrzl/enumerators"

// KV represents the interface for key-value database operations.
type KV interface {

	// Read
	Get(primaryKey PrimaryKey) (*Item, error)
	GetBatch(keys ...PrimaryKey) ([]*Item, error)
	Query(queryArgs QueryArgs, sort SortDirection) ([]*Item, error)

	// enumerate the items in the database that match the query arguments.
	// The results are returned in ascending order when the query operator GreaterThan, or GreaterThanOrEqual, Bwtween, or StartsWith.
	// The results are returned in descending order when the query operator is LessThan or LessThanOrEqual.
	Enumerate(queryArgs QueryArgs) enumerators.Enumerator[*Item]

	// Write
	Put(item *Item) error

	// Remove
	Remove(primaryKey PrimaryKey) error
	RemoveBatch(keys ...PrimaryKey) error
	RemoveRange(rangeKey RangeKey) error

	// Batch
	Batch(items []*BatchItem) error
	BatchChunks(items enumerators.Enumerator[*BatchItem], chunkSize int) error

	// Close
	Close() error
}

func NewPrimaryKey(partitionKey, rowKey EncodedKey) PrimaryKey {
	return PrimaryKey{
		PartitionKey: partitionKey,
		RowKey:       rowKey,
	}
}

type PrimaryKey struct {
	PartitionKey EncodedKey
	RowKey       EncodedKey
}

func (pk *PrimaryKey) Encode() EncodedKey {
	result := make([]byte, 0, len(pk.PartitionKey)+len(pk.RowKey)+1)
	result = append(result, pk.PartitionKey...)
	result = append(result, 0x00)
	result = append(result, pk.RowKey...)
	return result
}

func NewRangeKey(partitionKey, startRowKey, endRowKey EncodedKey) RangeKey {
	return RangeKey{
		PartitionKey: partitionKey,
		StartRowKey:  startRowKey,
		EndRowKey:    endRowKey,
	}
}

type RangeKey struct {
	PartitionKey EncodedKey
	StartRowKey  EncodedKey
	EndRowKey    EncodedKey
}

func (rk *RangeKey) Encode() (lower, upper EncodedKey) {
	lower = encodeLowerBoundary(rk.PartitionKey, rk.StartRowKey) // Lower bound ends with 0x00
	upper = encodeUpperBoundary(rk.PartitionKey, rk.EndRowKey)   // Upper bound ends with 0xFF
	return lower, upper
}

// Helper function to encode boundaries with a given terminator
func encodeLowerBoundary(partitionKey, rowKey []byte) EncodedKey {
	result := make([]byte, 0, len(partitionKey)+len(rowKey)+1)
	result = append(result, partitionKey...)
	result = append(result, 0x00)
	result = append(result, rowKey...)

	return result
}

// Helper function to encode boundaries with a given terminator
func encodeUpperBoundary(partitionKey, rowKey []byte) EncodedKey {
	// Pre-allocate with space for two delimiters (partition separator + end marker)
	result := make([]byte, 0, len(partitionKey)+len(rowKey)+2)
	result = append(result, partitionKey...)
	result = append(result, 0x00) // PartitionKey separator
	result = append(result, rowKey...)
	result = append(result, 0xFF) // End-of-range marker
	return result
}

// Item represents a key-value pair.
type Item struct {
	PK    PrimaryKey
	Value []byte
}

type BatchItem struct {
	Op    Operation
	PK    PrimaryKey
	Value []byte
}

type Operation int

const (
	NoOp Operation = iota
	Put
	Remove
)

// QueryArgs defines the parameters for querying items.
type QueryArgs struct {
	PartitionKey EncodedKey

	// StartKey specifies the starting key for the query.
	// For Equal queries, it indicates the key to match.
	// For GreaterThan and GreaterThanOrEqual queries, it defines the lower bound (exclusive or inclusive, respectively).
	// For Between queries, it specifies the beginning (inclusive) of the range.
	// For LessThan, LessThanOrEqual, and StartsWith queries, it is either used as a prefix or ignored based on the context.
	StartRowKey EncodedKey

	// EndKey specifies the ending key for the query.
	// For Equal queries, it is ignored.
	// For LessThan and LessThanOrEqual queries, it defines the upper bound (exclusive or inclusive, respectively).
	// For Between queries, it indicates the end of the range.
	// For GreaterThan, GreaterThanOrEqual, and StartsWith queries, it is either used as a prefix or ignored based on the context.
	EndRowKey EncodedKey

	// Operator specifies the type of comparison to perform.
	Operator QueryOperator

	// Limit defines the maximum number of results to return.
	Limit int
}

// QueryOperator defines the type of query operation.
type QueryOperator int

const (
	None QueryOperator = iota
	Equal
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
	Between
	StartsWith
)

// SortDirection defines the sorting direction for queries.
type SortDirection int

const (
	NoneDirection SortDirection = iota
	Ascending
	Descending
)
