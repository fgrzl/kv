package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

type store struct {
	db       *pebble.DB
	disposed sync.Once
}

func NewPebbleStore(path string, opts ...Option) (kv.KV, error) {
	options := NewOptions(opts...) // returns *pebble.Options
	db, err := pebble.Open(path, options)
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

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

func (s *store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	if args.Operator == kv.Equal {
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

	satisfies, seek, move := getOperatorFunctions(args.Operator)
	rangeKey := lexkey.RangeKey{
		PartitionKey: args.PartitionKey,
		StartRowKey:  args.StartRowKey,
		EndRowKey:    args.EndRowKey,
	}
	lower, upper := rangeKey.Encode(true)

	opts := &pebble.IterOptions{LowerBound: lower, UpperBound: upper}
	iter, err := s.db.NewIter(opts)
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
	enum := enumerators.FilterMap(
		Enumerator(iter, opts, seek, move),
		func(kvp KeyValuePair) (*kv.Item, bool, error) {
			if args.Limit > 0 && counter >= args.Limit {
				return nil, false, nil
			}
			pk := lexkey.DecodePrimaryKey(iter.Key())
			if satisfies(pk, rangeKey) {
				counter++
				return &kv.Item{PK: pk, Value: append([]byte{}, iter.Value()...)}, true, nil
			}
			return nil, false, nil
		},
	)
	return enum
}

func (s *store) Insert(ctx context.Context, item *kv.Item) error {
	key := item.PK.Encode()

	// First try to read the key
	_, closer, err := s.db.Get(key)
	if err == nil {
		closer.Close()
		return fmt.Errorf("key already exists: %v", item.PK)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("insert check failed: %w", err)
	}

	// Race possible here – mitigate using Sync + manual retry logic if needed
	err = s.db.Set(key, item.Value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

func (s *store) Put(ctx context.Context, item *kv.Item) error {
	return s.db.Set(item.PK.Encode(), item.Value, pebble.Sync)
}

func (s *store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	return s.db.Delete(pk.Encode(), pebble.Sync)
}

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

func (s *store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	defer items.Dispose()
	chunks := enumerators.ChunkByCount(items, chunkSize)
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return fmt.Errorf("failed to retrieve chunk: %w", err)
		}
		batch := s.db.NewBatch()
		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				batch.Close()
				return fmt.Errorf("failed to retrieve item in chunk: %w", err)
			}
			if err := applyBatchOp(batch, item); err != nil {
				batch.Close()
				return fmt.Errorf("batch operation failed for key %v: %w", item.PK, err)
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			batch.Close()
			return fmt.Errorf("failed to commit batch: %w", err)
		}
		batch.Close()
	}
	return nil
}

func (s *store) Close() error {
	var closeErr error
	s.disposed.Do(func() {
		closeErr = s.db.Close()
	})
	return closeErr
}

func applyBatchOp(batch *pebble.Batch, item *kv.BatchItem) error {
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
				return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0 &&
					bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
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
	default:
		return func(_ lexkey.PrimaryKey, _ lexkey.RangeKey) bool {
				return true
			}, func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
				return iter.First()
			}, func(iter *pebble.Iterator) bool {
				return iter.Next()
			}
	}
}
