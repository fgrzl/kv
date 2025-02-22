package pebble

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
)

type store struct {
	db       *pebble.DB
	disposed sync.Once
}

func NewPebbleStore(path string) (kv.KV, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (s *store) Get(pk kv.PrimaryKey) (*kv.Item, error) {
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

func (s *store) GetBatch(keys ...kv.PrimaryKey) ([]*kv.Item, error) {
	var results []*kv.Item

	for _, pk := range keys {
		key := pk.Encode()
		value, closer, err := s.db.Get(key)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}
		results = append(results, &kv.Item{PK: pk, Value: append([]byte{}, value...)})
		closer.Close()
	}

	return results, nil
}

func (s *store) Query(queryArgs kv.QueryArgs, sortOrder kv.SortDirection) ([]*kv.Item, error) {
	enumerator := s.Enumerate(queryArgs)
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
			reverseItems(slice)
		}
	case kv.Descending:
		if isAscendingEnumerated {
			reverseItems(slice)
		}
	}

	return slice, nil
}

func (s *store) Enumerate(args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	var counter int

	// Short-circuit for Equal query
	if args.Operator == kv.Equal {
		pk := kv.PrimaryKey{PartitionKey: args.PartitionKey, RowKey: args.StartRowKey}
		item, err := s.Get(pk)
		if err != nil {
			return enumerators.Error[*kv.Item](err)
		}
		if item == nil {
			return enumerators.Empty[*kv.Item]()
		}
		return enumerators.Slice([]*kv.Item{item})
	}

	limit := args.Limit

	var satisfiesOperator func(key kv.PrimaryKey, rangeKey kv.RangeKey) bool
	var seek func(iter *pebble.Iterator, opts *pebble.IterOptions) bool
	var move func(iter *pebble.Iterator) bool

	rangeKey := kv.NewRangeKey(args.PartitionKey, args.StartRowKey, args.EndRowKey)
	lower, upper := rangeKey.Encode()
	options := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}

	switch args.Operator {

	case kv.GreaterThan:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rangeKey.StartRowKey) > 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Next()
		}

	case kv.GreaterThanOrEqual:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rangeKey.StartRowKey) >= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Next()
		}

	case kv.LessThan:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rangeKey.EndRowKey) < 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekLT(opts.UpperBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Prev()
		}

	case kv.LessThanOrEqual:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rangeKey.EndRowKey) <= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekLT(opts.UpperBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Prev()
		}

	case kv.Between:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rangeKey.StartRowKey) >= 0 && bytes.Compare(pk.RowKey, rangeKey.EndRowKey) <= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Next()
		}

	case kv.StartsWith:

		satisfiesOperator = func(pk kv.PrimaryKey, rangeKey kv.RangeKey) bool {
			return bytes.HasPrefix(pk.RowKey, rangeKey.StartRowKey)
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Next()
		}

	default:
		satisfiesOperator = func(_ kv.PrimaryKey, _ kv.RangeKey) bool {
			return true
		}
		seek = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.First()
		}
		move = func(iter *pebble.Iterator) bool {
			return iter.Next()
		}
	}

	iter, err := s.db.NewIter(options)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}

	if !seek(iter, options) {
		if iter.Error() != nil {
			return enumerators.Error[*kv.Item](iter.Error())
		}
		return enumerators.Empty[*kv.Item]()
	}

	generator := enumerators.GenerateAndDispose(func() (*kv.Item, bool, error) {

		for iter.Valid() {
			if limit > 0 && counter >= limit {
				return nil, false, nil
			}
			var item *kv.Item
			// split the key into partition key and row key
			pk := kv.NewPrimaryKey(
				append([]byte{}, iter.Key()[:len(args.PartitionKey)]...),
				append([]byte{}, iter.Key()[len(args.PartitionKey)+1:]...),
			)

			if satisfiesOperator(pk, rangeKey) {

				item = &kv.Item{
					PK:    pk,
					Value: append([]byte{}, iter.Value()...),
				}
				counter++

				// move the iterator to the next key
				move(iter)
				return item, true, nil
			}
			move(iter)
		}

		return nil, false, iter.Error()

	}, func() {
		iter.Close()
	})

	return generator
}

func (s *store) Put(item *kv.Item) error {
	return s.db.Set(item.PK.Encode(), item.Value, pebble.Sync)
}

func (s *store) Remove(pk kv.PrimaryKey) error {
	return s.db.Delete(pk.Encode(), pebble.Sync)
}

func (s *store) RemoveBatch(keys ...kv.PrimaryKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, pk := range keys {
		if err := batch.Delete(pk.Encode(), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) RemoveRange(rangeKey kv.RangeKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	lower := kv.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.StartRowKey)
	upper := kv.NewPrimaryKey(rangeKey.PartitionKey, rangeKey.EndRowKey)

	if err := batch.DeleteRange(lower.Encode(), upper.Encode(), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *store) Batch(items []*kv.BatchItem) error {

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, item := range items {
		key := item.PK.Encode()
		switch item.Op {
		case kv.NoOp:
			continue
		case kv.Put:
			if err := batch.Set(key, item.Value, nil); err != nil {
				return err
			}
		case kv.Remove:
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) BatchChunks(items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	chunks := enumerators.ChunkByCount(items, chunkSize)
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return err
		}

		batch := s.db.NewBatch()

		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				return err
			}
			key := item.PK.Encode()
			switch item.Op {
			case kv.NoOp:
				continue
			case kv.Put:
				if err := batch.Set(key, item.Value, nil); err != nil {
					batch.Close()
					return err
				}
			case kv.Remove:
				if err := batch.Delete(key, nil); err != nil {
					batch.Close()
					return err
				}
			}
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			batch.Close()
			return err
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

func reverseItems(items []*kv.Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}
