package azure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/valuecodec"
	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
)

type mockTableClient struct {
	data map[string][]byte
}

func newMockTableClient() *mockTableClient {
	return &mockTableClient{
		data: make(map[string][]byte),
	}
}

func (m *mockTableClient) CreateTable(ctx context.Context) error {
	return nil
}

func (m *mockTableClient) GetEntity(ctx context.Context, partitionKey, rowKey string) ([]byte, error) {
	key := partitionKey + ":" + rowKey
	value, ok := m.data[key]
	if !ok {
		return nil, &client.AzureError{StatusCode: 404, Code: "ResourceNotFound", Message: "entity not found"}
	}
	return value, nil
}

func (m *mockTableClient) UpsertEntity(ctx context.Context, entity []byte, mode string) error {
	var e map[string]interface{}
	if err := json.Unmarshal(entity, &e); err != nil {
		return err
	}
	key := e["PartitionKey"].(string) + ":" + e["RowKey"].(string)
	m.data[key] = entity
	return nil
}

func (m *mockTableClient) AddEntity(ctx context.Context, entity []byte) error {
	var e map[string]interface{}
	if err := json.Unmarshal(entity, &e); err != nil {
		return err
	}
	key := e["PartitionKey"].(string) + ":" + e["RowKey"].(string)
	if _, exists := m.data[key]; exists {
		return &client.AzureError{StatusCode: 409, Code: "EntityAlreadyExists", Message: "conflict"}
	}
	m.data[key] = entity
	return nil
}

func (m *mockTableClient) DeleteEntity(ctx context.Context, partitionKey, rowKey string) error {
	key := partitionKey + ":" + rowKey
	delete(m.data, key)
	return nil
}

func (m *mockTableClient) NewListEntitiesPager(filter, selectCols string, top int32) *client.ListEntitiesPager {
	panic("NewListEntitiesPager not implemented in mock")
}

func (m *mockTableClient) SubmitBatch(ctx context.Context, ops []client.BatchOp) error {
	panic("SubmitBatch not implemented in mock")
}

type mockBatchTableClient struct {
	*mockTableClient
	submitBatchSizes []int
}

func newMockBatchTableClient() *mockBatchTableClient {
	return &mockBatchTableClient{mockTableClient: newMockTableClient()}
}

func (m *mockBatchTableClient) SubmitBatch(ctx context.Context, ops []client.BatchOp) error {
	m.submitBatchSizes = append(m.submitBatchSizes, len(ops))
	return nil
}

func makeBatchItems(partition string, count int) []*kv.BatchItem {
	items := make([]*kv.BatchItem, 0, count)
	for i := 0; i < count; i++ {
		items = append(items, &kv.BatchItem{
			Op: kv.Put,
			PK: lexkey.NewPrimaryKey(
				lexkey.Encode(partition),
				lexkey.Encode(fmt.Sprintf("row-%03d", i)),
			),
			Value: []byte("value"),
		})
	}
	return items
}

func TestShouldGetItemFromAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	entity := Entity{
		PartitionKey: pk.PartitionKey,
		RowKey:       pk.RowKey,
		Value:        value,
	}
	entityJSON, _ := json.Marshal(entity)
	mockClient.data[pk.PartitionKey.ToHexString()+":"+pk.RowKey.ToHexString()] = entityJSON

	item, err := store.Get(context.Background(), pk)

	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, pk, item.PK)
	assert.Equal(t, value, item.Value)
}

func TestShouldReturnNilWhenGettingNonExistentItemFromAzure(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))

	item, err := store.Get(context.Background(), pk)

	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestShouldPutItemInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	err = store.Put(context.Background(), item)

	assert.NoError(t, err)

	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldInsertItemInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	err = store.Insert(context.Background(), item)

	assert.NoError(t, err)

	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldFailInsertWhenItemExistsInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	err = store.Insert(context.Background(), item)
	assert.NoError(t, err)

	err = store.Insert(context.Background(), item)

	assert.Error(t, err)
	assert.Equal(t, kv.ErrAlreadyExists, err)
}

func TestShouldRemoveItemFromAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	err = store.Put(context.Background(), item)
	assert.NoError(t, err)

	err = store.Remove(context.Background(), pk)

	assert.NoError(t, err)

	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.Nil(t, retrieved)
}

func TestGetBatchReturnsAllDuplicateKeys(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	entity := Entity{
		PartitionKey: pk.PartitionKey,
		RowKey:       pk.RowKey,
		Value:        value,
	}
	entityJSON, _ := json.Marshal(entity)
	mockClient.data[pk.PartitionKey.ToHexString()+":"+pk.RowKey.ToHexString()] = entityJSON

	results, err := store.GetBatch(context.Background(), pk, pk)

	assert.NoError(t, err)
	assert.Len(t, results, 2)
	for _, result := range results {
		assert.True(t, result.Found)
		assert.NotNil(t, result.Item)
		assert.Equal(t, pk, result.Item.PK)
		assert.Equal(t, value, result.Item.Value)
	}
}

func TestGroupBatchSlotsByStoredRowKeyKeepsDuplicates(t *testing.T) {
	pkA := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("a"))
	pkB := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("b"))
	storedA, storedB := pkToStore(pkA), pkToStore(pkB)
	partHexA, rkHexA := storedA.PartitionKey.ToHexString(), storedA.RowKey.ToHexString()
	rkHexB := storedB.RowKey.ToHexString()

	uniqueRKHexes, slotsByRK := groupBatchSlotsByStoredRowKey([]getBatchSlot{
		{idx: 0, pk: pkA, partHex: partHexA, rkHex: rkHexA},
		{idx: 1, pk: pkA, partHex: partHexA, rkHex: rkHexA},
		{idx: 2, pk: pkB, partHex: partHexA, rkHex: rkHexB},
	})

	assert.Len(t, uniqueRKHexes, 2)
	assert.Len(t, slotsByRK[rkHexA], 2)
	assert.Len(t, slotsByRK[rkHexB], 1)
}

func TestSplitBatchRowKeysForQueryRespectsFilterLength(t *testing.T) {
	partHex := strings.Repeat("p", 32)
	rowKeys := []string{
		strings.Repeat("a", 360),
		strings.Repeat("b", 360),
		strings.Repeat("c", 360),
	}

	queryChunks, pointGets := splitBatchRowKeysForQuery(partHex, rowKeys)

	assert.Empty(t, pointGets)
	assert.NotEmpty(t, queryChunks)
	totalKeys := 0
	for _, chunk := range queryChunks {
		totalKeys += len(chunk)
		assert.LessOrEqual(t, len(chunk), getBatchMaxRowKeysPerQuery)
		assert.LessOrEqual(t, len(buildPartitionRowKeysFilter(partHex, chunk)), getBatchMaxFilterLength)
	}
	assert.Equal(t, len(rowKeys), totalKeys)
}

func TestSplitBatchRowKeysForQueryFallsBackToPointGet(t *testing.T) {
	partHex := strings.Repeat("p", 32)
	veryLongRowKey := strings.Repeat("x", getBatchMaxFilterLength)

	queryChunks, pointGets := splitBatchRowKeysForQuery(partHex, []string{veryLongRowKey})

	assert.Empty(t, queryChunks)
	assert.Equal(t, []string{veryLongRowKey}, pointGets)
}

func TestBatchShouldChunkSinglePartitionIntoValidBatchSizes(t *testing.T) {
	mockClient := newMockBatchTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	items := makeBatchItems("partition-a", 250)

	err = store.Batch(context.Background(), items)

	assert.NoError(t, err)
	assert.Equal(t, []int{100, 100, 50}, mockClient.submitBatchSizes)
}

func TestBatchShouldChunkEachPartitionIndependently(t *testing.T) {
	mockClient := newMockBatchTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	items := append(makeBatchItems("partition-a", 120), makeBatchItems("partition-b", 130)...)

	err = store.Batch(context.Background(), items)

	assert.NoError(t, err)
	sizes := append([]int(nil), mockClient.submitBatchSizes...)
	sort.Ints(sizes)
	assert.Equal(t, []int{20, 30, 100, 100}, sizes)
}

func TestShouldReadLegacyRawValueWhenCompressionEnabledInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient, WithValueCompression(testValueCompressionConfig()))
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("legacy"))
	value := []byte("legacy raw value")
	storedPK := pkToStore(pk)
	entityJSON, err := json.Marshal(Entity{PartitionKey: storedPK.PartitionKey, RowKey: storedPK.RowKey, Value: value})
	assert.NoError(t, err)
	mockClient.data[storedPK.PartitionKey.ToHexString()+":"+storedPK.RowKey.ToHexString()] = entityJSON

	item, err := store.Get(context.Background(), pk)

	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, value, item.Value)
}

func TestShouldCompressValuesWhenConfiguredInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient, WithValueCompression(testValueCompressionConfig()))
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("compressed"))
	value := bytes.Repeat([]byte("compressible-value-"), 64)
	item := &kv.Item{PK: pk, Value: value}

	err = store.Put(context.Background(), item)
	assert.NoError(t, err)

	assert.Len(t, mockClient.data, 1)
	for _, raw := range mockClient.data {
		var entity Entity
		err = json.Unmarshal(raw, &entity)
		assert.NoError(t, err)
		assert.True(t, valuecodec.IsCompressed(entity.Value))
	}

	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldCompressLargeValuesByDefaultInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("default-compressed"))
	value := bytes.Repeat([]byte("compressible-value-"), 64)

	err = store.Put(context.Background(), &kv.Item{PK: pk, Value: value})
	assert.NoError(t, err)

	assert.Len(t, mockClient.data, 1)
	for _, raw := range mockClient.data {
		var entity Entity
		err = json.Unmarshal(raw, &entity)
		assert.NoError(t, err)
		assert.True(t, valuecodec.IsCompressed(entity.Value))
	}

	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldKeepValuesRawWhenCompressionDisabledInAzureStore(t *testing.T) {
	mockClient := newMockTableClient()
	store, err := NewAzureStoreWithClient(mockClient, WithoutValueCompression())
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("disabled"))
	value := bytes.Repeat([]byte("compressible-value-"), 64)

	err = store.Put(context.Background(), &kv.Item{PK: pk, Value: value})
	assert.NoError(t, err)

	assert.Len(t, mockClient.data, 1)
	for _, raw := range mockClient.data {
		var entity Entity
		err = json.Unmarshal(raw, &entity)
		assert.NoError(t, err)
		assert.False(t, valuecodec.IsCompressed(entity.Value))
		assert.Equal(t, value, entity.Value)
	}
}

func testValueCompressionConfig() valuecodec.Config {
	config := valuecodec.DefaultConfig()
	config.MinInputSize = 1
	config.MinSavingsBytes = 1
	config.MaxEncodedRatio = 0.99
	return config
}
