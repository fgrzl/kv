package kv

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
)

// Item represents a key-value pair stored in the KV store.
type Item struct {
	PK    lexkey.PrimaryKey
	Value []byte
}

// BatchItem represents an operation to perform in a batch.
type BatchItem struct {
	Op    BatchOp
	PK    lexkey.PrimaryKey
	Value []byte
}

// BatchOp defines the type of batch operation.
type BatchOp int

const (
	NoOp BatchOp = iota
	Put
	Delete
)

// QueryArgs defines parameters for querying the KV store.
type QueryArgs struct {
	PartitionKey lexkey.LexKey
	StartRowKey  lexkey.LexKey
	EndRowKey    lexkey.LexKey
	Operator     QueryOperator
	Limit        int
}

// QueryOperator defines the type of query operation.
type QueryOperator int

const (
	Scan QueryOperator = iota
	Equal
	GreaterThan
	GreaterThanOrEqual
	LessThan
	LessThanOrEqual
	Between
	StartsWith
)

var (
	ErrAlreadyExists         = errors.New("item already exists")
	ErrInvalidBatchOperation = errors.New("invalid batch operation")
	ErrInvalidLexKey         = errors.New("invalid encoded lexkey")
	ErrBackendUnavailable    = errors.New("backend unavailable")
	ErrBackendExecution      = errors.New("backend execution failed")
)

// SortDirection defines the sorting order for query results.
type SortDirection int

const (
	Ascending SortDirection = iota
	Descending
)

// ReverseItems reverses a slice of Item pointers in place.
func ReverseItems(items []*Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

// SortItems sorts []*kv.Item by PartitionKey and RowKey in ascending or descending order.
func SortItems(items []*Item, direction SortDirection) {
	sort.Slice(items, func(i, j int) bool {
		// Compare PartitionKey first
		if cmp := bytes.Compare(items[i].PK.PartitionKey, items[j].PK.PartitionKey); cmp != 0 {
			if direction == Ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		// If PartitionKeys are equal, compare RowKey
		if direction == Ascending {
			return bytes.Compare(items[i].PK.RowKey, items[j].PK.RowKey) < 0
		}
		return bytes.Compare(items[i].PK.RowKey, items[j].PK.RowKey) > 0
	})
}

// KV defines the interface for a key-value store.
type KV interface {
	Get(ctx context.Context, pk lexkey.PrimaryKey) (*Item, error)
	GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]*Item, error)
	Insert(ctx context.Context, item *Item) error
	Put(ctx context.Context, item *Item) error
	Remove(ctx context.Context, pk lexkey.PrimaryKey) error
	RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error
	RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error
	Query(ctx context.Context, queryArgs QueryArgs, sort SortDirection) ([]*Item, error)
	Enumerate(ctx context.Context, queryArgs QueryArgs) enumerators.Enumerator[*Item]
	Batch(ctx context.Context, items []*BatchItem) error
	BatchChunks(ctx context.Context, items enumerators.Enumerator[*BatchItem], chunkSize int) error
	Close() error
}
