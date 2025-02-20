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

	value, closer, err := s.db.Get(key.Encode())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return &kv.Item{Key: key, Value: append([]byte{}, value...)}, nil
}

func (s *store) GetBatch(keys ...kv.EncodedKey) ([]*kv.Item, error) {
	var results []*kv.Item

	for _, key := range keys {
		value, closer, err := s.db.Get(key.Encode())
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}
		results = append(results, &kv.Item{Key: key, Value: append([]byte{}, value...)})
		closer.Close()
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

	var satisfiesOperator func(key []byte, opts *pebble.IterOptions) bool
	var seek func(iter *pebble.Iterator, opts *pebble.IterOptions) bool
	var move func(iter *pebble.Iterator, opts *pebble.IterOptions) bool
	options := &pebble.IterOptions{}

	switch queryArgs.Operator {

	case kv.GreaterThan:

		options.LowerBound = queryArgs.StartKey.Encode()

		if !queryArgs.EndKey.IsEmpty() {
			options.UpperBound = queryArgs.EndKey.Encode()
		} else {
			options.UpperBound = queryArgs.StartKey.EncodeLastInPrefix()
		}

		satisfiesOperator = func(key []byte, opts *pebble.IterOptions) bool {
			return bytes.Compare(key, opts.LowerBound) > 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Next()
		}

	case kv.GreaterThanOrEqual:

		options.LowerBound = queryArgs.StartKey.Encode()

		if !queryArgs.EndKey.IsEmpty() {
			options.UpperBound = queryArgs.EndKey.Encode()
		} else {
			options.UpperBound = queryArgs.StartKey.EncodeLastInPrefix()
		}

		satisfiesOperator = func(key []byte, opts *pebble.IterOptions) bool {
			return bytes.Compare(key, opts.LowerBound) >= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Next()
		}

	case kv.LessThan:

		options.UpperBound = queryArgs.EndKey.Encode()

		if !queryArgs.StartKey.IsEmpty() {
			options.LowerBound = queryArgs.StartKey.EncodeFirst()
		} else {
			options.LowerBound = queryArgs.EndKey.EncodeFirstInPrefix()
		}

		satisfiesOperator = func(key []byte, opts *pebble.IterOptions) bool {
			return bytes.Compare(key, opts.UpperBound) < 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekLT(opts.UpperBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Prev()
		}

	case kv.LessThanOrEqual:

		options.UpperBound = queryArgs.EndKey.Encode()

		if !queryArgs.StartKey.IsEmpty() {
			options.LowerBound = queryArgs.StartKey.EncodeFirst()
		} else {
			options.LowerBound = queryArgs.EndKey.EncodeFirstInPrefix()
		}

		satisfiesOperator = func(key []byte, opts *pebble.IterOptions) bool {
			return bytes.Compare(key, opts.UpperBound) <= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekLT(opts.UpperBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Prev()
		}

	case kv.Between:

		options.LowerBound = queryArgs.StartKey.Encode()
		options.UpperBound = queryArgs.EndKey.EncodeLast() // pebble is exclusive we want inclusive

		satisfiesOperator = func(key []byte, opts *pebble.IterOptions) bool {
			return bytes.Compare(key, opts.LowerBound) >= 0 && bytes.Compare(key, opts.UpperBound) <= 0
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Next()
		}

	case kv.StartsWith:
		options.LowerBound = queryArgs.StartKey.Encode()
		options.UpperBound = queryArgs.StartKey.EncodeLastInPrefix()
		satisfiesOperator = func(key []byte, options *pebble.IterOptions) bool {
			return bytes.HasPrefix(key, options.LowerBound)
		}
		seek = func(iter *pebble.Iterator, opts *pebble.IterOptions) bool {
			return iter.SeekGE(opts.LowerBound)
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.Next()
		}

	default:
		satisfiesOperator = func(_ []byte, _ *pebble.IterOptions) bool {
			return true
		}
		seek = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
			return iter.First()
		}
		move = func(iter *pebble.Iterator, _ *pebble.IterOptions) bool {
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
			key := iter.Key()
			if satisfiesOperator(key, options) {

				// Copy the key and value to avoid mutation
				var encodedKey kv.EncodedKey
				encodedKey.Decode(append([]byte{}, key...))
				item = &kv.Item{
					Key:   encodedKey,
					Value: append([]byte{}, iter.Value()...),
				}
				counter++

				// move the iterator to the next key
				move(iter, options)
				return item, true, nil
			}
			move(iter, options)
		}

		return nil, false, iter.Error()

	}, func() {
		iter.Close()
	})

	return generator
}

func (s *store) Put(item *kv.Item) error {
	return s.db.Set(item.Key.Encode(), item.Value, pebble.Sync)
}

func (s *store) Remove(key kv.EncodedKey) error {
	return s.db.Delete(key.Encode(), pebble.Sync)
}

func (s *store) RemoveBatch(keys ...kv.EncodedKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete(key.Encode(), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) RemoveRange(start, end kv.EncodedKey) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(start.Encode(), end.EncodeLast(), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *store) Batch(items []*kv.BatchItem) error {

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, item := range items {
		switch item.Op {
		case kv.NoOp:
			continue
		case kv.Put:
			if err := batch.Set(item.Key.Encode(), item.Value, nil); err != nil {
				return err
			}
		case kv.Remove:
			if err := batch.Delete(item.Key.Encode(), nil); err != nil {
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
			switch item.Op {
			case kv.NoOp:
				continue
			case kv.Put:
				if err := batch.Set(item.Key.Encode(), item.Value, nil); err != nil {
					batch.Close()
					return err
				}
			case kv.Remove:
				if err := batch.Delete(item.Key.Encode(), nil); err != nil {
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
