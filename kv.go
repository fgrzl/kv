package kv

import "github.com/fgrzl/enumerators"

// KV represents the interface for key-value database operations.
type KV interface {

	// Read
	Get(key EncodedKey) (*Item, error)
	GetBatch(keys ...EncodedKey) ([]*Item, error)
	Query(queryArgs *QueryArgs, sort SortDirection) ([]*Item, error)

	// enumerate the items in the database that match the query arguments.
	// The results are returned in ascending order when the query operator GreaterThan, or GreaterThanOrEqual, Bwtween, or StartsWith.
	// The results are returned in descending order when the query operator is LessThan or LessThanOrEqual.
	Enumerate(queryArgs *QueryArgs) enumerators.Enumerator[*Item]

	// Write
	Put(item *Item) error
	PutBatch(items []*Item) error
	PutChunks(items enumerators.Enumerator[*Item], chunkSize int) error

	// Remove
	Remove(key EncodedKey) error
	RemoveRange(startKey, endKey EncodedKey) error
	RemoveBatch(keys ...EncodedKey) error

	// Close
	Close() error
}

// Item represents a key-value pair.
type Item struct {
	Key   EncodedKey
	Value []byte
}

// QueryArgs defines the parameters for querying items.
type QueryArgs struct {
	// StartKey specifies the starting key for the query.
	// For Equal queries, it indicates the key to match.
	// For GreaterThan and GreaterThanOrEqual queries, it defines the lower bound (exclusive or inclusive, respectively).
	// For Between queries, it specifies the beginning (inclusive) of the range.
	// For LessThan, LessThanOrEqual, and StartsWith queries, it is either used as a prefix or ignored based on the context.
	StartKey EncodedKey

	// EndKey specifies the ending key for the query.
	// For Equal queries, it is ignored.
	// For LessThan and LessThanOrEqual queries, it defines the upper bound (exclusive or inclusive, respectively).
	// For Between queries, it indicates the end of the range.
	// For GreaterThan, GreaterThanOrEqual, and StartsWith queries, it is not used.
	EndKey EncodedKey

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
