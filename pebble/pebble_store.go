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

func (s *store) Get(key kv.EncodedKey) (*kv.Item, error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return &kv.Item{Key: append([]byte{}, key...), Value: append([]byte{}, value...)}, nil
}

func (s *store) GetBatch(keys ...kv.EncodedKey) ([]*kv.Item, error) {

	var results []*kv.Item
	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	// Retrieve each key within the batch
	for _, key := range keys {
		value, closer, err := batch.Get(key)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}
		item := &kv.Item{Key: key, Value: append([]byte{}, value...)}

		results = append(results, item)

		if err := closer.Close(); err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (s *store) Query(queryArgs *kv.QueryArgs, sortOrder kv.SortDirection) ([]*kv.Item, error) {
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

func reverseItems(items []*kv.Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func (s *store) Enumerate(queryArgs *kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	var counter int

	// Short-circuit for Equal query
	if queryArgs.Operator == kv.Equal {
		item, err := s.Get(queryArgs.StartKey)
		if err != nil {
			return enumerators.Error[*kv.Item](err)
		}
		if item == nil {
			return enumerators.Empty[*kv.Item]()
		}
		return enumerators.Slice([]*kv.Item{item})
	}

	limit := queryArgs.Limit

	var satisfiesOperator func(key kv.EncodedKey) bool
	var seek func(iter *pebble.Iterator) bool
	var move func(iter *pebble.Iterator) bool
	options := &pebble.IterOptions{}

	switch queryArgs.Operator {
	case kv.GreaterThan:
		options.LowerBound = queryArgs.StartKey
		satisfiesOperator = func(key kv.EncodedKey) bool { return bytes.Compare(key, queryArgs.StartKey) > 0 }
		seek = func(iter *pebble.Iterator) bool { return iter.SeekGE(queryArgs.StartKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Next() }
	case kv.GreaterThanOrEqual:
		options.LowerBound = queryArgs.StartKey
		satisfiesOperator = func(key kv.EncodedKey) bool { return bytes.Compare(key, queryArgs.StartKey) >= 0 }
		seek = func(iter *pebble.Iterator) bool { return iter.SeekGE(queryArgs.StartKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Next() }
	case kv.LessThan:
		options.UpperBound = queryArgs.EndKey
		satisfiesOperator = func(key kv.EncodedKey) bool { return bytes.Compare(key, queryArgs.EndKey) < 0 }
		seek = func(iter *pebble.Iterator) bool { return iter.SeekLT(queryArgs.EndKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Prev() }
	case kv.LessThanOrEqual:
		options.UpperBound = queryArgs.EndKey
		satisfiesOperator = func(key kv.EncodedKey) bool { return bytes.Compare(key, queryArgs.EndKey) <= 0 }
		seek = func(iter *pebble.Iterator) bool { return iter.SeekLT(queryArgs.EndKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Prev() }
	case kv.Between:
		options.LowerBound = queryArgs.StartKey
		options.UpperBound = queryArgs.EndKey
		satisfiesOperator = func(key kv.EncodedKey) bool {
			return bytes.Compare(key, queryArgs.StartKey) >= 0 && bytes.Compare(key, queryArgs.EndKey) <= 0
		}
		seek = func(iter *pebble.Iterator) bool { return iter.SeekGE(queryArgs.StartKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Next() }
	case kv.StartsWith:
		options.LowerBound = queryArgs.StartKey
		satisfiesOperator = func(key kv.EncodedKey) bool { return bytes.HasPrefix(key, queryArgs.StartKey) }
		seek = func(iter *pebble.Iterator) bool { return iter.SeekGE(queryArgs.StartKey) }
		move = func(iter *pebble.Iterator) bool { return iter.Next() }
	default:
		satisfiesOperator = func(_ kv.EncodedKey) bool { return true }
		seek = func(iter *pebble.Iterator) bool { return iter.First() }
		move = func(iter *pebble.Iterator) bool { return iter.Next() }
	}

	iter, err := s.db.NewIter(options)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}

	if !seek(iter) {
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
			key := iter.Key()
			if satisfiesOperator(key) {

				// Copy the key and value to avoid mutation
				item = &kv.Item{
					Key:   append([]byte{}, key...),
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
	return s.db.Set([]byte(item.Key), item.Value, pebble.Sync)
}

func (s *store) PutBatch(items []*kv.Item) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, item := range items {
		if err := batch.Set([]byte(item.Key), item.Value, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) PutChunks(items enumerators.Enumerator[*kv.Item], chunkSize int) error {
	chunks := enumerators.ChunkByCount(items, chunkSize)
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return err
		}

		batch := s.db.NewBatch()
		defer batch.Close()

		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				return err
			}
			if err := batch.Set([]byte(item.Key), item.Value, nil); err != nil {
				return err
			}
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) Remove(key kv.EncodedKey) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}

func (s *store) RemoveBatch(keys ...kv.EncodedKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete([]byte(key), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) RemoveRange(start, end kv.EncodedKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(start, end, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *store) Close() error {
	var closeErr error
	s.disposed.Do(func() {
		closeErr = s.db.Close()
	})
	return closeErr
}
