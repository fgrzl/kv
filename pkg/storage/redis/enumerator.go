package redis

import (
	"context"
	"sync"

	"github.com/fgrzl/enumerators"
	"github.com/redis/go-redis/v9"
)

var _ enumerators.Enumerator[string] = (*enumerator)(nil)

// enumerator wraps a Pebble iterator and implements Enumerator.
type enumerator struct {
	ctx      context.Context
	iter     *redis.ScanIterator
	disposed sync.Once
}

// NewPebbleEnumerator creates a new enumerator for a given Pebble iterator.
func RedisEnumerator(ctx context.Context, iter *redis.ScanIterator) *enumerator {
	return &enumerator{
		ctx:  ctx,
		iter: iter,
	}
}

// MoveNext advances the iterator and returns whether there is a next element.
func (e *enumerator) MoveNext() bool {
	if e.iter == nil {
		return false
	}

	return e.iter.Next(e.ctx)
}

// Current returns the current key-value pair or an error if iteration is invalid.
func (e *enumerator) Current() (string, error) {
	if e.iter.Err() != nil {
		return "", e.iter.Err()
	}

	return e.iter.Val(), nil
}

// Err returns any encountered error.
func (e *enumerator) Err() error {
	return e.iter.Err()
}

// Dispose closes the iterator.
func (e *enumerator) Dispose() {
	e.disposed.Do(func() {
		e.iter = nil
	})
}
