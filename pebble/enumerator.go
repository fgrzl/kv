package pebble

import (
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
)

var _ enumerators.Enumerator[KeyValuePair] = (*enumerator)(nil)

// enumerator wraps a Pebble iterator and implements Enumerator.
type enumerator struct {
	opts     *pebble.IterOptions
	iter     *pebble.Iterator
	seek     func(*pebble.Iterator, *pebble.IterOptions) bool
	next     func(*pebble.Iterator) bool
	seeked   bool
	valid    bool
	err      error
	disposed sync.Once
}

// NewPebbleEnumerator creates a new enumerator for a given Pebble iterator.
func Enumerator(
	iter *pebble.Iterator,
	opts *pebble.IterOptions,
	seek func(*pebble.Iterator, *pebble.IterOptions) bool,
	next func(*pebble.Iterator) bool) *enumerator {
	return &enumerator{
		iter: iter,
		opts: opts,
		seek: seek,
		next: next,
	}
}

// MoveNext advances the iterator and returns whether there is a next element.
func (e *enumerator) MoveNext() bool {
	if e.iter == nil {
		return false
	}

	if !e.seeked {
		e.valid = e.seek(e.iter, e.opts)
		if !e.valid {
			e.err = e.iter.Error()
		}
		e.seeked = true
		return e.valid
	}

	e.valid = e.next(e.iter)
	if !e.valid {
		e.err = e.iter.Error()
	}
	return e.valid
}

// Current returns the current key-value pair or an error if iteration is invalid.
func (e *enumerator) Current() (KeyValuePair, error) {

	if !e.valid {
		return KeyValuePair{}, errors.New("iterator is not valid")
	}

	key := append([]byte(nil), e.iter.Key()...)
	value := append([]byte(nil), e.iter.Value()...)

	return KeyValuePair{
		Key:   key,
		Value: value,
	}, nil

}

// Err returns any encountered error.
func (e *enumerator) Err() error {
	return e.err
}

// Dispose closes the iterator.
func (e *enumerator) Dispose() {
	e.disposed.Do(func() {
		e.iter.Close()
		e.iter = nil
	})
}

type KeyValuePair struct {
	Key   lexkey.LexKey
	Value []byte
}
