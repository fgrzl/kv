package kv_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pebble"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Setup function initializes a test database and ensures cleanup after the test.
func setup(t *testing.T) kv.KV {
	// Arrange
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, fmt.Sprintf("db_%v.pebble", uuid.NewString()))

	// Act
	store, err := pebble.NewPebbleStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize Pebble DB: %v", err)
	}

	// Cleanup after test
	t.Cleanup(func() {
		store.Close()
	})

	return store
}

// -----------------
// Test Dataset
// -----------------
var TestDataset = []*kv.BatchItem{
	{Op: kv.Put, Key: kv.EncodedKey("a"), Value: []byte("A")},
	{Op: kv.Put, Key: kv.EncodedKey("b"), Value: []byte("B")},
	{Op: kv.Put, Key: kv.EncodedKey("c"), Value: []byte("C")},
	{Op: kv.Put, Key: kv.EncodedKey("d"), Value: []byte("D")},
	{Op: kv.Put, Key: kv.EncodedKey("e"), Value: []byte("E")},
	{Op: kv.Put, Key: kv.EncodedKey("f"), Value: []byte("F")},
	{Op: kv.Put, Key: kv.EncodedKey("g"), Value: []byte("G")},
}

// -----------------
// Tests for KV Store Methods
// -----------------

func TestPut(t *testing.T) {
	t.Run("Should store and retrieve an item correctly", func(t *testing.T) {
		// Arrange
		db := setup(t)
		item := &kv.Item{Key: kv.EncodedKey("key1"), Value: []byte("value1")}

		// Act
		err := db.Put(item)

		// Assert
		result, errGet := db.Get(item.Key)
		assert.NoError(t, err)
		assert.NoError(t, errGet)
		assert.NotNil(t, result)
		assert.Equal(t, item.Value, result.Value)
	})
}

func TestPutBatch(t *testing.T) {
	t.Run("Should store multiple items", func(t *testing.T) {
		// Arrange
		db := setup(t)
		items := []*kv.BatchItem{
			{Op: kv.Put, Key: kv.EncodedKey("key1"), Value: []byte("value1")},
			{Op: kv.Put, Key: kv.EncodedKey("key2"), Value: []byte("value2")},
		}

		// Act
		err := db.Batch(items)

		// Assert
		result1, _ := db.Get(items[0].Key)
		result2, _ := db.Get(items[1].Key)
		assert.NoError(t, err)
		assert.NotNil(t, result1)
		assert.NotNil(t, result2)
		assert.Equal(t, items[0].Value, result1.Value)
		assert.Equal(t, items[1].Value, result2.Value)
	})
}

func TestRemove(t *testing.T) {
	t.Run("Should delete an existing item", func(t *testing.T) {
		// Arrange
		db := setup(t)
		item := &kv.Item{Key: kv.EncodedKey("key1"), Value: []byte("value1")}
		db.Put(item)

		// Act
		err := db.Remove(item.Key)

		// Assert
		result, _ := db.Get(item.Key)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestQuery_ExactMatch(t *testing.T) {
	// Arrange
	db := setup(t)
	err := db.Batch(TestDataset)
	require.NoError(t, err)

	query := &kv.QueryArgs{
		StartKey: kv.EncodedKey("b"),
		Operator: kv.Equal,
	}

	// Act
	results, err := db.Query(query, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, kv.EncodedKey("b"), results[0].Key)
}

func TestQuery_GreaterThan(t *testing.T) {
	// Arrange
	db := setup(t)
	err := db.Batch(TestDataset)
	require.NoError(t, err)

	args := &kv.QueryArgs{
		StartKey: kv.EncodedKey("c"),
		Operator: kv.GreaterThan,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 4) // "d", "e", "f", "g"
	assert.Equal(t, kv.EncodedKey("d"), results[0].Key)
}

func TestQuery_LessThan(t *testing.T) {
	// Arrange
	db := setup(t)
	err := db.Batch(TestDataset)
	require.NoError(t, err)

	args := &kv.QueryArgs{
		EndKey:   kv.EncodedKey("d"),
		Operator: kv.LessThan,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 3) // "a", "b", "c"
	assert.Equal(t, kv.EncodedKey("a"), results[0].Key)
}

func TestQuery_Between(t *testing.T) {
	// Arrange
	db := setup(t)
	err := db.Batch(TestDataset)
	require.NoError(t, err)

	args := &kv.QueryArgs{
		StartKey: kv.EncodedKey("b"),
		EndKey:   kv.EncodedKey("e"),
		Operator: kv.Between,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 3) // "b", "c", "d"
	assert.Equal(t, kv.EncodedKey("b"), results[0].Key)
}
