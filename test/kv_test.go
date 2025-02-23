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

var partitionKey = kv.EncodeKey("test")
var sampleData = []*kv.Item{
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("a")), Value: []byte("A")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("b")), Value: []byte("B")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("c")), Value: []byte("C")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("d")), Value: []byte("D")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("e")), Value: []byte("E")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("f")), Value: []byte("F")},
	{PK: kv.NewPrimaryKey(partitionKey, kv.EncodeKey("g")), Value: []byte("G")},
}

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

	var batch []*kv.BatchItem
	for _, item := range sampleData {
		batch = append(batch, &kv.BatchItem{Op: kv.Put, PK: item.PK, Value: item.Value})
	}
	err = store.Batch(batch)
	require.NoError(t, err)

	// Cleanup after test
	t.Cleanup(func() {
		store.Close()
	})

	return store
}

func TestPut(t *testing.T) {
	// Arrange
	db := setup(t)
	item := &kv.Item{PK: kv.NewPrimaryKey(kv.EncodeKey("put-test"), kv.EncodeKey(1)), Value: []byte("hello world")}

	// Act
	err := db.Put(item)

	// Assert
	result, errGet := db.Get(item.PK)
	assert.NoError(t, err)
	assert.NoError(t, errGet)
	assert.NotNil(t, result)
	assert.Equal(t, item.PK, result.PK)
	assert.Equal(t, item.Value, result.Value)
}

func TestRemove(t *testing.T) {

	// Arrange
	db := setup(t)
	item := sampleData[0]
	result, errGet := db.Get(item.PK)
	require.NoError(t, errGet)
	require.NotNil(t, result)

	// Act
	err := db.Remove(item.PK)

	// Assert
	assert.NoError(t, err)
	result, errGet = db.Get(item.PK)
	assert.NoError(t, err)
	assert.NoError(t, errGet)
	assert.Nil(t, result)
}

func TestQuery_ExactMatch(t *testing.T) {
	// Arrange
	db := setup(t)

	query := kv.QueryArgs{
		PartitionKey: partitionKey,
		StartRowKey:  kv.EncodeKey("b"),
		Operator:     kv.Equal,
	}

	// Act
	results, err := db.Query(query, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 1) // "b"
	assert.Equal(t, kv.EncodeKey("b"), results[0].PK.RowKey)
}

func TestQuery_GreaterThan(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		StartRowKey:  kv.EncodeKey("c"),
		Operator:     kv.GreaterThan,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 4) // "d", "e", "f", "g"
	assert.Equal(t, kv.EncodeKey("d"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("e"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("f"), results[2].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("g"), results[3].PK.RowKey)
}

func TestQuery_GreaterThanEqual(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		StartRowKey:  kv.EncodeKey("c"),
		Operator:     kv.GreaterThanOrEqual,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 5) //"c" "d", "e", "f", "g"
	assert.Equal(t, kv.EncodeKey("c"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("d"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("e"), results[2].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("f"), results[3].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("g"), results[4].PK.RowKey)
}

func TestQuery_LessThan(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		EndRowKey:    kv.EncodeKey("d"),
		Operator:     kv.LessThan,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 3) // "a", "b", "c"
	assert.Equal(t, kv.EncodeKey("a"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("b"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("c"), results[2].PK.RowKey)
}

func TestQuery_LessThanOrEqual(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		EndRowKey:    kv.EncodeKey("d"),
		Operator:     kv.LessThanOrEqual,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 4) // "a", "b", "c", "d"
	assert.Equal(t, kv.EncodeKey("a"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("b"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("c"), results[2].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("d"), results[3].PK.RowKey)
}

func TestQuery_Between(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		StartRowKey:  kv.EncodeKey("b"),
		EndRowKey:    kv.EncodeKey("d"),
		Operator:     kv.Between,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 3) // "b", "c", "d"
	assert.Equal(t, kv.EncodeKey("b"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("c"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("d"), results[2].PK.RowKey)
}

func TestQuery_PartitionScan(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		Operator:     kv.Scan,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 7) // "a", "b", "c", "d", "e", "f", "g"
	assert.Equal(t, kv.EncodeKey("a"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("b"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("c"), results[2].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("d"), results[3].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("e"), results[4].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("f"), results[5].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("g"), results[6].PK.RowKey)
}

func TestQuery_PartitionScanWithLimit(t *testing.T) {
	// Arrange
	db := setup(t)

	args := kv.QueryArgs{
		PartitionKey: partitionKey,
		Operator:     kv.Scan,
		Limit:        3,
	}

	// Act
	results, err := db.Query(args, kv.Ascending)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, results, 3) // "a", "b", "c"
	assert.Equal(t, kv.EncodeKey("a"), results[0].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("b"), results[1].PK.RowKey)
	assert.Equal(t, kv.EncodeKey("c"), results[2].PK.RowKey)
}
