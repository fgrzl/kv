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

type Option func(*pebble.Options)

func WithTableCacheShards(n int) Option {
	return func(opts *pebble.Options) {
		opts.Experimental.TableCacheShards = n
	}
}

type store struct {
	db       *pebble.DB
	disposed sync.Once
}

func NewPebbleStore(path string, opts ...Option) (kv.KV, error) {
	options := &pebble.Options{}
	for _, opt := range opts {
		opt(options)
	}

	db, err := pebble.Open(path, options)
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (s *store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	key := pk.Encode()
	value, closer, err := s.db.Get(key)
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
	var results []*kv.Item

	for _, pk := range keys {
		key := pk.Encode()
		value, closer, err := s.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // Skip missing keys
			}
			return nil, err // Return on other errors
		}

		// Ensure closer is always called to release resources
		func() {
			defer closer.Close()
			results = append(results, &kv.Item{PK: pk, Value: append([]byte{}, value...)})
		}()
	}

	if len(results) == 0 {
		return []*kv.Item{}, nil
	}

	return results, nil
}

func (s *store) Query(ctx context.Context, queryArgs kv.QueryArgs, sortOrder kv.SortDirection) ([]*kv.Item, error) {
	enumerator := s.Enumerate(ctx, queryArgs)
	slice, err := enumerators.ToSlice(enumerator)
	if err != nil {
		return nil, err
	}

	// Determine the order in which items were enumerated.
	// For LessThan and LessThanOrEqual queries, we iterate in descending order;
	// otherwise, we iterate in ascending order.
	isAscendingEnumerated := true
	switch queryArgs.Operator {
	case kv.LessThan, kv.LessThanOrEqual:
		isAscendingEnumerated = false
	default:
		isAscendingEnumerated = true
	}

	// If the enumerated order doesn't match the requested sort order, simply reverse the slice.
	switch sortOrder {
	case kv.Ascending:
		if !isAscendingEnumerated {
			kv.ReverseItems(slice)
		}
	case kv.Descending:
		if isAscendingEnumerated {
			kv.ReverseItems(slice)
		}
	}

	return slice, nil
}

func (s *store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {

	// Shortcut for Equal operator
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

	// Define iteration behavior based on operator
	satisfies, seek, move := getOperatorFunctions(args.Operator)

	rangeKey := lexkey.RangeKey{
		PartitionKey: args.PartitionKey,
		StartRowKey:  args.StartRowKey,
		EndRowKey:    args.EndRowKey,
	}

	lower, upper := rangeKey.Encode(true)

	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}

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

	enumerator := enumerators.FilterMap(
		Enumerator(iter, opts, seek, move),
		func(kvp KeyValuePair) (*kv.Item, bool, error) {
			if args.Limit > 0 && counter >= args.Limit {
				return nil, false, nil
			}

			pk := lexkey.NewPrimaryKey(
				append([]byte{}, iter.Key()[:len(args.PartitionKey)]...),
				append([]byte{}, iter.Key()[len(args.PartitionKey)+1:]...),
			)

			if satisfies(pk, rangeKey) {
				item := &kv.Item{
					PK:    pk,
					Value: append([]byte{}, iter.Value()...),
				}
				counter++
				return item, true, nil
			}
			return nil, false, nil
		})

	return enumerator
}

func (s *store) Insert(ctx context.Context, item *kv.Item) error {
	return errors.ErrUnsupported
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

	lower := lexkey.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.StartRowKey)
	upper := lexkey.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.EndRowKey)

	if err := batch.DeleteRange(lower.Encode(), upper.Encode(), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *store) Batch(ctx context.Context, items []*kv.BatchItem) error {
	if len(items) == 0 {
		return nil // No operations to perform
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, item := range items {
		key := item.PK.Encode()
		var err error

		switch item.Op {
		case kv.NoOp:
			continue
		case kv.Put:
			err = batch.Set(key, item.Value, nil)
		case kv.Delete:
			err = batch.Delete(key, nil)
		}

		if err != nil {
			return fmt.Errorf("batch operation failed for key %v: %w", item.PK, err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("batch commit failed: %w", err)
	}

	return nil
}

func (s *store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	defer items.Dispose()

	chunks := enumerators.ChunkByCount(items, chunkSize)
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return fmt.Errorf("failed to retrieve chunk: %w", err)
		}

		// Create a new batch for this chunk
		batch := s.db.NewBatch()

		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				// Close batch immediately on error
				batch.Close()
				return fmt.Errorf("failed to retrieve item in chunk: %w", err)
			}

			key := item.PK.Encode()
			var opErr error

			switch item.Op {
			case kv.NoOp:
				continue
			case kv.Put:
				opErr = batch.Set(key, item.Value, nil)
			case kv.Delete:
				opErr = batch.Delete(key, nil)
			}

			if opErr != nil {
				batch.Close()
				return fmt.Errorf("batch operation failed for key %v: %w", item.PK, opErr)
			}
		}

		// Commit the batch and close it immediately after
		if err := batch.Commit(pebble.Sync); err != nil {
			batch.Close()
			return fmt.Errorf("failed to commit batch: %w", err)
		}
		// Explicitly close the batch after processing the chunk
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

func getOperatorFunctions(operator kv.QueryOperator) (
	func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool,
	func(iter *pebble.Iterator, opts *pebble.IterOptions) bool,
	func(iter *pebble.Iterator) bool,
) {

	switch operator {
	case kv.GreaterThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.StartRowKey) > 0
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekGE(opts.LowerBound) },
			func(iter *pebble.Iterator) bool { return iter.Next() }

	case kv.GreaterThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekGE(opts.LowerBound) },
			func(iter *pebble.Iterator) bool { return iter.Next() }

	case kv.LessThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.EndRowKey) < 0
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekLT(opts.UpperBound) },
			func(iter *pebble.Iterator) bool { return iter.Prev() }

	case kv.LessThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekLT(opts.UpperBound) },
			func(iter *pebble.Iterator) bool { return iter.Prev() }

	case kv.Between:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0 &&
					bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekGE(opts.LowerBound) },
			func(iter *pebble.Iterator) bool { return iter.Next() }

	case kv.StartsWith:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
				return bytes.HasPrefix(pk.RowKey, rk.StartRowKey)
			},
			func(iter *pebble.Iterator, opts *pebble.IterOptions) bool { return iter.SeekGE(opts.LowerBound) },
			func(iter *pebble.Iterator) bool { return iter.Next() }

	default:
		return func(_ lexkey.PrimaryKey, _ lexkey.RangeKey) bool { return true },
			func(iter *pebble.Iterator, _ *pebble.IterOptions) bool { return iter.First() },
			func(iter *pebble.Iterator) bool { return iter.Next() }
	}
}
