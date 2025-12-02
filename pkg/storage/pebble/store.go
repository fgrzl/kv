package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/enumerators"
	kv "github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// PebbleDB defines the interface for Pebble database operations.
type PebbleDB interface {
	Get(key []byte) ([]byte, io.Closer, error)
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, opts *pebble.WriteOptions) error
	NewBatch(opts ...pebble.BatchOption) *pebble.Batch
	NewIterWithContext(ctx context.Context, opts *pebble.IterOptions) (*pebble.Iterator, error)
	Close() error
}

// PebbleBatch defines the interface for Pebble batch operations.
type PebbleBatch interface {
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, opts *pebble.WriteOptions) error
	DeleteRange(start, end []byte, opts *pebble.WriteOptions) error
	Commit(opts *pebble.WriteOptions) error
	Close() error
}

type store struct {
	db       PebbleDB
	disposed sync.Once
}

// NewPebbleStore creates a new Pebble-backed kv.KV store.
func NewPebbleStore(path string, opts ...Option) (kv.KV, error) {
	options := NewOptions(opts...) // returns *pebble.Options
	db, err := pebble.Open(path, options)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble database: %w", err)
	}
	store, err := NewPebbleStoreWithDB(db)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(context.Background(), "Pebble store initialized", "path", path)
	return store, nil
}

// NewPebbleStoreWithDB creates a new Pebble-backed kv.KV store with a provided database.
// This is primarily for testing purposes.
func NewPebbleStoreWithDB(db PebbleDB) (kv.KV, error) {
	return &store{db: db}, nil
}

// Get returns a single item by primary key, or nil if not found.
func (s *store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	value, closer, err := s.db.Get(pk.Encode())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	return &kv.Item{PK: pk, Value: append([]byte{}, value...)}, nil
}

// GetBatch returns a slice of items for the given primary keys.
func (s *store) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]*kv.Item, error) {
	results := make([]*kv.Item, 0, len(keys))
	for _, pk := range keys {
		value, closer, err := s.db.Get(pk.Encode())
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, err
		}
		results = append(results, &kv.Item{PK: pk, Value: append([]byte{}, value...)})
		closer.Close()
	}
	return results, nil
}

// Query returns all items matching the query args and sort order.
func (s *store) Query(ctx context.Context, queryArgs kv.QueryArgs, sortOrder kv.SortDirection) ([]*kv.Item, error) {
	enumerator := s.Enumerate(ctx, queryArgs)
	slice, err := enumerators.ToSlice(enumerator)
	if err != nil {
		return nil, err
	}
	ascending := queryArgs.Operator != kv.LessThan && queryArgs.Operator != kv.LessThanOrEqual
	if (sortOrder == kv.Ascending && !ascending) || (sortOrder == kv.Descending && ascending) {
		kv.ReverseItems(slice)
	}
	return slice, nil
}

// Enumerate returns an enumerator for all items matching the query.
func (s *store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	if args.Operator == kv.Equal {
		return s.enumerateEqual(ctx, args)
	}
	return s.enumerateRange(ctx, args)
}

// Insert inserts a new item, returning an error if the key exists.
func (s *store) Insert(ctx context.Context, item *kv.Item) error {
	key := item.PK.Encode()
	_, closer, err := s.db.Get(key)
	if err == nil {
		closer.Close()
		return fmt.Errorf("key already exists: %v", item.PK)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("insert check failed: %w", err)
	}
	return s.db.Set(key, item.Value, pebble.Sync)
}

// Put stores an item, replacing any existing value.
func (s *store) Put(ctx context.Context, item *kv.Item) error {
	return s.db.Set(item.PK.Encode(), item.Value, pebble.Sync)
}

// Remove deletes an item by primary key.
func (s *store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	return s.db.Delete(pk.Encode(), pebble.Sync)
}

// RemoveBatch deletes multiple items by primary key.
func (s *store) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, pk := range keys {
		if err := batch.Delete(pk.Encode(), nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

// RemoveRange deletes all items in a key range.
func (s *store) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(
		lexkey.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.StartRowKey).Encode(),
		lexkey.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.EndRowKey).Encode(),
		nil,
	); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

// Batch executes batch operations (Put/Delete) atomically.
func (s *store) Batch(ctx context.Context, items []*kv.BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, item := range items {
		if err := applyBatchOp(batch, item); err != nil {
			return fmt.Errorf("batch operation failed for key %v: %w", item.PK, err)
		}
	}
	return batch.Commit(pebble.Sync)
}

// BatchChunks splits items into chunks and executes them as batches.
func (s *store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	chunks := enumerators.ChunkByCount(items, chunkSize)
	return enumerators.ForEach(chunks, func(chunk enumerators.Enumerator[*kv.BatchItem]) error {
		batch := s.db.NewBatch()
		defer batch.Close()
		err := enumerators.ForEach(chunk, func(item *kv.BatchItem) error {
			return applyBatchOp(batch, item)
		})
		if err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	})
}

// Close closes the Pebble database.
func (s *store) Close() error {
	var closeErr error
	s.disposed.Do(func() {
		closeErr = s.db.Close()
	})
	return closeErr
}

// --- Private Helpers ---

func (s *store) enumerateEqual(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	pk := lexkey.PrimaryKey{PartitionKey: args.PartitionKey, RowKey: args.StartRowKey}
	item, err := s.Get(ctx, pk)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}
	if item == nil {
		return enumerators.Empty[*kv.Item]()
	}
	return enumerators.Slice([]*kv.Item{item})
}

func (s *store) enumerateRange(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	satisfies, seek, move := getOperatorFunctions(args.Operator)
	rangeKey := lexkey.RangeKey{
		PartitionKey: args.PartitionKey,
		StartRowKey:  args.StartRowKey,
		EndRowKey:    args.EndRowKey,
	}
	lower, upper := rangeKey.Encode(true)
	opts := &pebble.IterOptions{LowerBound: lower, UpperBound: upper}

	iter, err := s.db.NewIterWithContext(ctx, opts)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}
	if !seek(iter, opts) {
		defer iter.Close()
		if iter.Error() != nil {
			return enumerators.Error[*kv.Item](iter.Error())
		}
		return enumerators.Empty[*kv.Item]()
	}

	var counter int
	return enumerators.FilterMap(
		Enumerator(iter, opts, seek, move),
		func(kvp KeyValuePair) (*kv.Item, bool, error) {
			if args.Limit > 0 && counter >= args.Limit {
				return nil, false, nil
			}
			pk, err := lexkey.DecodePrimaryKey(iter.Key())
			if err != nil {
				return nil, false, err
			}
			if !satisfies(pk, rangeKey) {
				return nil, false, nil
			}
			counter++
			return &kv.Item{PK: pk, Value: append([]byte{}, iter.Value()...)}, true, nil
		},
	)
}

func applyBatchOp(batch PebbleBatch, item *kv.BatchItem) error {
	key := item.PK.Encode()
	switch item.Op {
	case kv.NoOp:
		return nil
	case kv.Put:
		return batch.Set(key, item.Value, nil)
	case kv.Delete:
		return batch.Delete(key, nil)
	default:
		return fmt.Errorf("unsupported batch op: %v", item.Op)
	}
}

// getOperatorFunctions returns (satisfies, seek, move) functions for a query operator.
func getOperatorFunctions(operator kv.QueryOperator) (
	func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool,
	func(iter *pebble.Iterator, opts *pebble.IterOptions) bool,
	func(iter *pebble.Iterator) bool,
) {
	switch operator {
	case kv.GreaterThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.StartRowKey) > 0
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	case kv.GreaterThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	case kv.LessThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.EndRowKey) < 0
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekLT(opts.UpperBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Prev()
			}
	case kv.LessThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekLT(opts.UpperBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Prev()
			}
	case kv.Between:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return true // Iterator bounds already handle the range filtering
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	case kv.StartsWith:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.HasPrefix(pk.RowKey, rk.StartRowKey)
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	case kv.Scan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return true
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	default:
		return func(_ lexkey.PrimaryKey, _ lexkey.RangeKey) bool {
				return true
			}, func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
				return iter.SeekGE(opts.LowerBound)
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	}
}
