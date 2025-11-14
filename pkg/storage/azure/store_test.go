package azure

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/stretchr/testify/assert"
)

// mockAzureClient is a mock implementation of AzureTableClient for testing.
type mockAzureClient struct {
	data map[string][]byte
}

func newMockAzureClient() *mockAzureClient {
	return &mockAzureClient{
		data: make(map[string][]byte),
	}
}

func (m *mockAzureClient) CreateTable(ctx context.Context, options *aztables.CreateTableOptions) (interface{}, error) {
	return nil, nil
}

// mockResponseError simulates azcore.ResponseError
type mockResponseError struct {
	statusCode int
}

func (e *mockResponseError) Error() string {
	return "mock error"
}

func (e *mockResponseError) StatusCode() int {
	return e.statusCode
}

func (m *mockAzureClient) GetEntity(ctx context.Context, partitionKey, rowKey string, options *aztables.GetEntityOptions) (aztables.GetEntityResponse, error) {
	key := partitionKey + ":" + rowKey
	value, ok := m.data[key]
	if !ok {
		return aztables.GetEntityResponse{}, &mockResponseError{statusCode: 404}
	}
	return aztables.GetEntityResponse{Value: value}, nil
}

func (m *mockAzureClient) UpsertEntity(ctx context.Context, entity []byte, options *aztables.UpsertEntityOptions) (interface{}, error) {
	// Parse entity to get key
	var e map[string]interface{}
	json.Unmarshal(entity, &e)
	key := e["PartitionKey"].(string) + ":" + e["RowKey"].(string)
	m.data[key] = entity
	return nil, nil
}

func (m *mockAzureClient) AddEntity(ctx context.Context, entity []byte, options *aztables.AddEntityOptions) (interface{}, error) {
	// Parse entity to get key
	var e map[string]interface{}
	json.Unmarshal(entity, &e)
	key := e["PartitionKey"].(string) + ":" + e["RowKey"].(string)
	if _, exists := m.data[key]; exists {
		return nil, &mockResponseError{statusCode: 409} // Conflict
	}
	m.data[key] = entity
	return nil, nil
}

func (m *mockAzureClient) DeleteEntity(ctx context.Context, partitionKey, rowKey string, options *aztables.DeleteEntityOptions) (interface{}, error) {
	key := partitionKey + ":" + rowKey
	delete(m.data, key)
	return nil, nil
}

func (m *mockAzureClient) NewListEntitiesPager(options *aztables.ListEntitiesOptions) interface{} {
	// For unit tests, we don't use enumeration, so panic if called.
	panic("NewListEntitiesPager not implemented in mock")
}

func (m *mockAzureClient) SubmitTransaction(ctx context.Context, operations []aztables.TransactionAction, options *aztables.SubmitTransactionOptions) (interface{}, error) {
	// For unit tests, we don't use batch operations, so panic if called.
	panic("SubmitTransaction not implemented in mock")
}

func TestShouldGetItemFromAzureStore(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
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

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, pk, item.PK)
	assert.Equal(t, value, item.Value)
}

func TestShouldReturnNilWhenGettingNonExistentItemFromAzure(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
	store, err := NewAzureStoreWithClient(mockClient)
	assert.NoError(t, err)

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestShouldPutItemInAzureStore(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
	store, err := NewAzureStoreWithClient(mockClient)
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

func TestShouldInsertItemInAzureStore(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
	store, err := NewAzureStoreWithClient(mockClient)
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

func TestShouldFailInsertWhenItemExistsInAzureStore(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
	store, err := NewAzureStoreWithClient(mockClient)
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
	assert.Equal(t, kv.ErrAlreadyExists, err)
}

func TestShouldRemoveItemFromAzureStore(t *testing.T) {
	// Arrange
	mockClient := newMockAzureClient()
	store, err := NewAzureStoreWithClient(mockClient)
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
