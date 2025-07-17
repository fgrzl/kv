package kv_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/azure"
	"github.com/fgrzl/kv/pebble"
	"github.com/fgrzl/kv/redis"
	"github.com/fgrzl/lexkey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	providers = []string{"azure", "pebble", "redis"}

	partitionKey = lexkey.Encode("test")

	sampleData = []*kv.Item{
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("a")), Value: []byte("A")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("b")), Value: []byte("B")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("c")), Value: []byte("C")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("d")), Value: []byte("D")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("e")), Value: []byte("E")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("f")), Value: []byte("F")},
		{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("g")), Value: []byte("G")},
	}
)

// Setup function initializes a test database and ensures cleanup after the test.
func setup(t *testing.T, provider string) kv.KV {
	var store kv.KV
	var err error

	switch provider {

	case "azure":
		// Default Azurite configuration for local testing
		accountName := "devstoreaccount1"
		accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
		endpoint := "http://127.0.0.1:10002/devstoreaccount1"

		credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			panic(err)
		}

		store, err = azure.NewAzureStore(
			azure.WithPrefix("test"),
			azure.WithTable(uuid.NewString()),
			azure.WithEndpoint(endpoint),
			azure.WithSharedKey(credential),
		)
		require.NoError(t, err)

	case "pebble":
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, fmt.Sprintf("db_%v.pebble", uuid.NewString()))
		store, err = pebble.NewPebbleStore(dbPath, pebble.WithTableCacheShards(1))
		require.NoError(t, err)

	case "redis":
		store, err = redis.NewRedisStore(
			redis.WithAddress("localhost:6379"),
			redis.WithDatabase(0),
		)
		require.NoError(t, err)
		store.(*redis.Store).Clear()
	}

	// Cleanup after test
	t.Cleanup(func() {
		store.Close()
	})

	// Seed initial data
	var batch []*kv.BatchItem
	for _, item := range sampleData {
		batch = append(batch, &kv.BatchItem{Op: kv.Put, PK: item.PK, Value: item.Value})
	}
	err = store.Batch(t.Context(), batch)
	require.NoError(t, err)

	return store
}

func TestPut(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			item := &kv.Item{PK: lexkey.NewPrimaryKey(lexkey.Encode("put-test"), lexkey.Encode(1)), Value: []byte("hello world")}

			// Act
			err := db.Put(t.Context(), item)

			// Assert
			assert.NoError(t, err)

			result, errGet := db.Get(t.Context(), item.PK)
			assert.NoError(t, errGet)
			assert.NotNil(t, result)
			assert.Equal(t, item.PK, result.PK)
			assert.Equal(t, item.Value, result.Value)
		})
	}
}

func TestRemove(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			item := sampleData[0]
			result, err := db.Get(t.Context(), item.PK)
			require.NoError(t, err)
			require.NotNil(t, result)

			// Act
			err = db.Remove(t.Context(), item.PK)

			// Assert
			assert.NoError(t, err)

			result, err = db.Get(t.Context(), item.PK)
			assert.NoError(t, err)
			assert.Nil(t, result)
		})
	}
}

func TestQueryExactMatch(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			query := kv.QueryArgs{
				PartitionKey: partitionKey,
				StartRowKey:  lexkey.Encode("b"),
				Operator:     kv.Equal,
			}

			// Act
			results, err := db.Query(t.Context(), query, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 1) // "b"
			assert.Equal(t, lexkey.Encode("b"), results[0].PK.RowKey)
		})
	}
}

func TestQueryGreaterThan(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				StartRowKey:  lexkey.Encode("c"),
				Operator:     kv.GreaterThan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 4) // "d", "e", "f", "g"
			assert.Equal(t, lexkey.Encode("d"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("e"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("f"), results[2].PK.RowKey)
			assert.Equal(t, lexkey.Encode("g"), results[3].PK.RowKey)
		})
	}
}

func TestQueryGreaterThanEqual(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				StartRowKey:  lexkey.Encode("c"),
				Operator:     kv.GreaterThanOrEqual,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 5) // "c", "d", "e", "f", "g"
			assert.Equal(t, lexkey.Encode("c"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("d"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("e"), results[2].PK.RowKey)
			assert.Equal(t, lexkey.Encode("f"), results[3].PK.RowKey)
			assert.Equal(t, lexkey.Encode("g"), results[4].PK.RowKey)
		})
	}
}

func TestQueryLessThan(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				EndRowKey:    lexkey.Encode("d"),
				Operator:     kv.LessThan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 3) // "a", "b", "c"
			assert.Equal(t, lexkey.Encode("a"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("b"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("c"), results[2].PK.RowKey)
		})
	}
}

func TestQueryLessThanOrEqual(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				EndRowKey:    lexkey.Encode("d"),
				Operator:     kv.LessThanOrEqual,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 4) // "a", "b", "c", "d"
			assert.Equal(t, lexkey.Encode("a"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("b"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("c"), results[2].PK.RowKey)
			assert.Equal(t, lexkey.Encode("d"), results[3].PK.RowKey)
		})
	}
}

func TestQueryBetween(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				StartRowKey:  lexkey.Encode("b"),
				EndRowKey:    lexkey.Encode("d"),
				Operator:     kv.Between,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 3) // "b", "c", "d"
			assert.Equal(t, lexkey.Encode("b"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("c"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("d"), results[2].PK.RowKey)
		})
	}
}

func TestQueryPartitionScan(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				Operator:     kv.Scan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 7) // "a", "b", "c", "d", "e", "f", "g"
			assert.Equal(t, lexkey.Encode("a"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("b"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("c"), results[2].PK.RowKey)
			assert.Equal(t, lexkey.Encode("d"), results[3].PK.RowKey)
			assert.Equal(t, lexkey.Encode("e"), results[4].PK.RowKey)
			assert.Equal(t, lexkey.Encode("f"), results[5].PK.RowKey)
			assert.Equal(t, lexkey.Encode("g"), results[6].PK.RowKey)
		})
	}
}

func TestQueryPartitionScanWithLimit(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				Operator:     kv.Scan,
				Limit:        3,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 3) // order is not guaranteed
		})
	}
}
