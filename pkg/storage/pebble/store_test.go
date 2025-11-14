package pebble

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
)

// mockCloser implements io.Closer for mocking.
type mockCloser struct{}

func (m *mockCloser) Close() error {
	return nil
}

// mockPebbleDB is a mock implementation of PebbleDB for testing.
type mockPebbleDB struct {
	data map[string][]byte
}

func newMockPebbleDB() *mockPebbleDB {
	return &mockPebbleDB{
		data: make(map[string][]byte),
	}
}

func (m *mockPebbleDB) Get(key []byte) ([]byte, io.Closer, error) {
	value, ok := m.data[string(key)]
	if !ok {
		return nil, nil, pebble.ErrNotFound
	}
	return value, &mockCloser{}, nil
}

func (m *mockPebbleDB) Set(key, value []byte, opts *pebble.WriteOptions) error {
	m.data[string(key)] = value
	return nil
}

func (m *mockPebbleDB) Delete(key []byte, opts *pebble.WriteOptions) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockPebbleDB) NewBatch(opts ...pebble.BatchOption) *pebble.Batch {
	// For unit tests, we don't use batch operations, so panic if called.
	panic("NewBatch not implemented in mock")
}

func (m *mockPebbleDB) NewIterWithContext(ctx context.Context, opts *pebble.IterOptions) (*pebble.Iterator, error) {
	// For unit tests, we don't use iterators, so return nil.
	return nil, nil
}

func (m *mockPebbleDB) Close() error {
	return nil
}

func TestShouldGetItemFromPebbleStore(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	err = mockDB.Set(pk.Encode(), value, nil)
	assert.NoError(t, err)

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, pk, item.PK)
	assert.Equal(t, value, item.Value)
}

func TestShouldReturnNilWhenGettingNonExistentItem(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestShouldPutItemInPebbleStore(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Act
	err = store.Put(context.Background(), item)

	// Assert
	assert.NoError(t, err)

	// Verify it was stored
	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldInsertItemInPebbleStore(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Act
	err = store.Insert(context.Background(), item)

	// Assert
	assert.NoError(t, err)

	// Verify it was stored
	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldFailInsertWhenItemExistsInPebbleStore(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Insert first time
	err = store.Insert(context.Background(), item)
	assert.NoError(t, err)

	// Act - Insert again
	err = store.Insert(context.Background(), item)

	// Assert
	assert.Error(t, err)
}

func TestShouldRemoveItemFromPebbleStore(t *testing.T) {
	// Arrange
	mockDB := newMockPebbleDB()
	store, err := NewPebbleStoreWithDB(mockDB)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Put item first
	err = store.Put(context.Background(), item)
	assert.NoError(t, err)

	// Act
	err = store.Remove(context.Background(), pk)

	// Assert
	assert.NoError(t, err)

	// Verify it was removed
	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.Nil(t, retrieved)
}
