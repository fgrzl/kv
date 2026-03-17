package kv_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/azkit/credentials"
	"github.com/fgrzl/enumerators"
	kv "github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/storage/azure"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/fgrzl/kv/pkg/storage/redis"
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

		credential, err := credentials.NewSharedKeyCredential(accountName, accountKey)
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
			redis.WithAddress("127.0.0.1:6379"),
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

// benchmarkSetup initializes a database for benchmarks.
func benchmarkSetup(b *testing.B, provider string) kv.KV {
	var store kv.KV
	var err error

	switch provider {

	case "pebble":
		tempDir := b.TempDir()
		dbPath := filepath.Join(tempDir, fmt.Sprintf("bench_%v.pebble", uuid.NewString()))
		store, err = pebble.NewPebbleStore(dbPath, pebble.WithTableCacheShards(1))
		if err != nil {
			b.Fatal(err)
		}

	default:
		b.Skipf("Benchmark not supported for provider: %s", provider)
	}

	// Cleanup after benchmark
	b.Cleanup(func() {
		store.Close()
	})

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
			assert.Len(t, results, 3)
		})
	}
}

func TestQueryPartitionScanDescending(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				Operator:     kv.Scan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Descending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 7) // "g", "f", "e", "d", "c", "b", "a"
			assert.Equal(t, lexkey.Encode("g"), results[0].PK.RowKey)
			assert.Equal(t, lexkey.Encode("f"), results[1].PK.RowKey)
			assert.Equal(t, lexkey.Encode("e"), results[2].PK.RowKey)
			assert.Equal(t, lexkey.Encode("d"), results[3].PK.RowKey)
			assert.Equal(t, lexkey.Encode("c"), results[4].PK.RowKey)
			assert.Equal(t, lexkey.Encode("b"), results[5].PK.RowKey)
			assert.Equal(t, lexkey.Encode("a"), results[6].PK.RowKey)
		})
	}
}

func TestQueryPartitionScanEmptyPartition(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			emptyPartitionKey := lexkey.Encode("empty-partition")
			args := kv.QueryArgs{
				PartitionKey: emptyPartitionKey,
				Operator:     kv.Scan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 0)
		})
	}
}

func TestQueryPartitionScanIsolation(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)

			// Add data to a different partition
			otherPartitionKey := lexkey.Encode("other-partition")
			otherItems := []*kv.Item{
				{PK: lexkey.NewPrimaryKey(otherPartitionKey, lexkey.Encode("x")), Value: []byte("X")},
				{PK: lexkey.NewPrimaryKey(otherPartitionKey, lexkey.Encode("y")), Value: []byte("Y")},
			}
			for _, item := range otherItems {
				err := db.Put(t.Context(), item)
				require.NoError(t, err)
			}

			// Scan the original partition
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				Operator:     kv.Scan,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Ascending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 7) // Only items from the original partition

			// Verify all results are from the correct partition
			for _, item := range results {
				assert.Equal(t, partitionKey, item.PK.PartitionKey)
			}

			// Verify other partition items are not included
			foundX := false
			foundY := false
			for _, item := range results {
				if string(item.PK.RowKey) == "x" {
					foundX = true
				}
				if string(item.PK.RowKey) == "y" {
					foundY = true
				}
			}
			assert.False(t, foundX, "should not find item from other partition")
			assert.False(t, foundY, "should not find item from other partition")
		})
	}
}

func TestQueryPartitionScanWithLimitAndSort(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				Operator:     kv.Scan,
				Limit:        2,
			}

			// Act
			results, err := db.Query(t.Context(), args, kv.Descending)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 2)
			// Verify items are from the correct partition and sorted descending
			for _, item := range results {
				assert.Equal(t, partitionKey, item.PK.PartitionKey)
			}
			// Verify descending order (first item's row key >= second item's row key)
			if len(results) == 2 {
				assert.GreaterOrEqual(t, string(results[0].PK.RowKey), string(results[1].PK.RowKey),
					"results should be sorted in descending order")
			}
		})
	}
}

func TestEnumeratePartitionScan(t *testing.T) {
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
			var results []*kv.Item
			enumerator := db.Enumerate(t.Context(), args)
			err := enumerators.ForEach(enumerator, func(item *kv.Item) error {
				results = append(results, item)
				return nil
			})

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 3)
			// Redis SCAN doesn't guarantee order, so just verify we got 3 items from the partition
			for _, item := range results {
				assert.Equal(t, partitionKey, item.PK.PartitionKey)
			}
		})
	}
}

func TestEnumeratePartitionScanEmpty(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			emptyPartitionKey := lexkey.Encode("nonexistent")
			args := kv.QueryArgs{
				PartitionKey: emptyPartitionKey,
				Operator:     kv.Scan,
			}

			// Act
			var results []*kv.Item
			enumerator := db.Enumerate(t.Context(), args)
			err := enumerators.ForEach(enumerator, func(item *kv.Item) error {
				results = append(results, item)
				return nil
			})

			// Assert
			assert.NoError(t, err)
			assert.Len(t, results, 0)
		})
	}
}

func TestShouldGetItem(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			pk := sampleData[0].PK

			// Act
			item, err := db.Get(t.Context(), pk)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, item)
			assert.Equal(t, sampleData[0].Value, item.Value)
		})
	}
}

func TestShouldGetBatchItems(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			keys := []lexkey.PrimaryKey{sampleData[0].PK, sampleData[1].PK}

			// Act
			results, err := db.GetBatch(t.Context(), keys...)

			// Assert
			assert.NoError(t, err)
			require.Len(t, results, 2)
			assert.True(t, results[0].Found, "first key should be found")
			assert.True(t, results[1].Found, "second key should be found")
			require.NotNil(t, results[0].Item)
			require.NotNil(t, results[1].Item)
			assert.Equal(t, sampleData[0].Value, results[0].Item.Value)
			assert.Equal(t, sampleData[1].Value, results[1].Item.Value)
		})
	}
}

func TestGetBatchMixedFoundAndMissingKeys(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			db := setup(t, provider)
			// Request keys: first two exist (from sampleData), third does not exist
			existing0 := sampleData[0].PK
			existing1 := sampleData[1].PK
			missingPK := lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("nonexistent"))
			keys := []lexkey.PrimaryKey{existing0, missingPK, existing1}

			results, err := db.GetBatch(t.Context(), keys...)
			require.NoError(t, err)
			require.Len(t, results, 3, "result length must match request length")

			// Order must match request order
			assert.True(t, results[0].Found, "first key exists")
			require.NotNil(t, results[0].Item)
			assert.Equal(t, sampleData[0].Value, results[0].Item.Value)

			assert.False(t, results[1].Found, "second key is missing")
			assert.Nil(t, results[1].Item)

			assert.True(t, results[2].Found, "third key exists")
			require.NotNil(t, results[2].Item)
			assert.Equal(t, sampleData[1].Value, results[2].Item.Value)
		})
	}
}

func TestShouldInsertItem(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			newItem := &kv.Item{PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("new")), Value: []byte("new value")}

			// Act
			err := db.Insert(t.Context(), newItem)

			// Assert
			assert.NoError(t, err)
			// Verify it was inserted
			retrieved, err := db.Get(t.Context(), newItem.PK)
			assert.NoError(t, err)
			assert.Equal(t, newItem.Value, retrieved.Value)
		})
	}
}

func TestShouldFailInsertWhenItemExists(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			existingPK := sampleData[0].PK

			// Act
			err := db.Insert(t.Context(), &kv.Item{PK: existingPK, Value: []byte("duplicate")})

			// Assert
			assert.Error(t, err)
		})
	}
}

func TestShouldRemoveBatch(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			keys := []lexkey.PrimaryKey{sampleData[0].PK, sampleData[1].PK}

			// Act
			err := db.RemoveBatch(t.Context(), keys...)

			// Assert
			assert.NoError(t, err)
			// Verify removed
			for _, key := range keys {
				item, err := db.Get(t.Context(), key)
				assert.NoError(t, err)
				assert.Nil(t, item)
			}
		})
	}
}

func TestShouldRemoveRange(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			rangeKey := lexkey.NewRangeKey(partitionKey, lexkey.Encode("a"), lexkey.Encode("d"))

			// Act
			err := db.RemoveRange(t.Context(), rangeKey)

			// Assert
			assert.NoError(t, err)
			// Verify range removed (a, b, c should be gone)
			for _, item := range sampleData {
				if string(item.PK.RowKey) >= "a" && string(item.PK.RowKey) < "d" {
					retrieved, err := db.Get(t.Context(), item.PK)
					assert.NoError(t, err)
					assert.Nil(t, retrieved, "item %s should be removed", string(item.PK.RowKey))
				}
			}
		})
	}
}

func TestShouldEnumerateQueryResults(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			args := kv.QueryArgs{
				PartitionKey: partitionKey,
				StartRowKey:  lexkey.Encode("a"),
				EndRowKey:    lexkey.Encode("c"),
				Operator:     kv.Between,
			}

			// Act
			var results []*kv.Item
			enumerator := db.Enumerate(t.Context(), args)
			err := enumerators.ForEach(enumerator, func(item *kv.Item) error {
				results = append(results, item)
				return nil
			})

			// Assert
			assert.NoError(t, err)
			assert.True(t, len(results) >= 2) // a, b, possibly c
		})
	}
}

func TestShouldBatchPutItems(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			batch := []*kv.BatchItem{
				{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("batch1")), Value: []byte("val1")},
				{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("batch2")), Value: []byte("val2")},
			}

			// Act
			err := db.Batch(t.Context(), batch)

			// Assert
			assert.NoError(t, err)
			// Verify
			item1, err := db.Get(t.Context(), batch[0].PK)
			assert.NoError(t, err)
			assert.Equal(t, []byte("val1"), item1.Value)
		})
	}
}

func TestShouldBatchChunks(t *testing.T) {
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			// Arrange
			db := setup(t, provider)
			batch := []*kv.BatchItem{
				{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("chunk1")), Value: []byte("val1")},
				{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode("chunk2")), Value: []byte("val2")},
			}
			enumerator := enumerators.Slice(batch)

			// Act
			err := db.BatchChunks(t.Context(), enumerator, 1)

			// Assert
			assert.NoError(t, err)
			// Verify
			item1, err := db.Get(t.Context(), batch[0].PK)
			assert.NoError(t, err)
			assert.Equal(t, []byte("val1"), item1.Value)
		})
	}
}

// Benchmarks

// BenchmarkPut measures Put performance across providers.
func BenchmarkPut(b *testing.B) {
	for _, provider := range providers {
		b.Run(provider, func(b *testing.B) {
			db := benchmarkSetup(b, provider)
			defer db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				item := &kv.Item{
					PK:    lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("bench-%d", i))),
					Value: []byte("benchmark value"),
				}
				if err := db.Put(context.Background(), item); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGet measures Get performance across providers.
func BenchmarkGet(b *testing.B) {
	for _, provider := range providers {
		b.Run(provider, func(b *testing.B) {
			db := benchmarkSetup(b, provider)
			defer db.Close()

			// Pre-populate
			for i := 0; i < 100; i++ {
				item := &kv.Item{
					PK:    lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("bench-%d", i))),
					Value: []byte("benchmark value"),
				}
				if err := db.Put(context.Background(), item); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pk := lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("bench-%d", i%100)))
				_, err := db.Get(context.Background(), pk)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBatch measures batch performance across providers.
func BenchmarkBatch(b *testing.B) {
	for _, provider := range providers {
		b.Run(provider, func(b *testing.B) {
			db := benchmarkSetup(b, provider)
			defer db.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := []*kv.BatchItem{
					{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("batch-%d-1", i))), Value: []byte("val1")},
					{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("batch-%d-2", i))), Value: []byte("val2")},
					{Op: kv.Put, PK: lexkey.NewPrimaryKey(partitionKey, lexkey.Encode(fmt.Sprintf("batch-%d-3", i))), Value: []byte("val3")},
				}
				if err := db.Batch(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
