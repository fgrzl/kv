package pebble

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/kv"
)

type store struct {
	db *pebble.DB
}

func NewPebbleStore(path string) (*store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (kv *store) Put(item *kv.Item) error {
	return kv.db.Set([]byte(item.Key), item.Value, pebble.Sync)
}

func (s *store) PutNodes(items []*kv.Item) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, item := range items {
		if err := batch.Set([]byte(item.Key), item.Value, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) Remove(key string) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}

func (s *store) RemoveItems(keys ...string) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete([]byte(key), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *store) GetItem(id string) (*kv.Item, error) {
	value, closer, err := s.db.Get([]byte(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return &kv.Item{Key: id, Value: append([]byte{}, value...)}, nil
}

func (s *store) Query(queryArgs *kv.QueryArgs) ([]*kv.Item, error) {
	var results []*kv.Item
	limit := queryArgs.Limit // Assuming queryArgs has a Limit field

	// Assuming queryArgs has fields like StartKey, EndKey, and SortOrder
	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(queryArgs.StartKey),
		UpperBound: []byte(queryArgs.EndKey),
	}
	iter, err := s.db.NewIter(iterOptions)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Check the sort order and iterate accordingly
	if queryArgs.Direction == kv.Descending {
		for iter.Last(); iter.Valid() && (limit == 0 || len(results) < limit); iter.Prev() {
			key := string(iter.Key())
			value := iter.Value()
			results = append(results, &kv.Item{Key: key, Value: append([]byte{}, value...)})
		}
	} else {
		for iter.First(); iter.Valid() && (limit == 0 || len(results) < limit); iter.Next() {
			key := string(iter.Key())
			value := iter.Value()
			results = append(results, &kv.Item{Key: key, Value: append([]byte{}, value...)})
		}
	}

	return results, nil
}

func (s *store) Close() error {
	return s.db.Close()
}
