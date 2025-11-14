package redis

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// mockRedisClient is a mock implementation of RedisClient for testing.
type mockRedisClient struct {
	data map[string][]byte
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		data: make(map[string][]byte),
	}
}

func (m *mockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("PONG")
	return cmd
}

func (m *mockRedisClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	m.data = make(map[string][]byte)
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if val, ok := m.data[key]; ok {
		cmd.SetVal(string(val))
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (m *mockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if v, ok := value.([]byte); ok {
		m.data[key] = v
	} else {
		m.data[key] = []byte(value.(string))
	}
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	if _, exists := m.data[key]; exists {
		cmd.SetVal(false)
	} else {
		if v, ok := value.([]byte); ok {
			m.data[key] = v
		} else {
			m.data[key] = []byte(value.(string))
		}
		cmd.SetVal(true)
	}
	return cmd
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	count := 0
	for _, key := range keys {
		if _, ok := m.data[key]; ok {
			delete(m.data, key)
			count++
		}
	}
	cmd.SetVal(int64(count))
	return cmd
}

func (m *mockRedisClient) Pipeline() redis.Pipeliner {
	// For unit tests, we don't use pipeline, so panic if called.
	panic("Pipeline not implemented in mock")
}

func (m *mockRedisClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	// For unit tests, we don't use scan, so panic if called.
	panic("Scan not implemented in mock")
}

func (m *mockRedisClient) Close() error {
	return nil
}

func TestShouldGetItemFromRedisStore(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	key := pk.Encode().ToHexString()
	value := []byte("test value")
	mockClient.data[key] = value

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, pk, item.PK)
	assert.Equal(t, value, item.Value)
}

func TestShouldReturnNilWhenGettingNonExistentItemFromRedis(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))

	// Act
	item, err := store.Get(context.Background(), pk)

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestShouldPutItemInRedisStore(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Act
	err := store.Put(context.Background(), item)

	// Assert
	assert.NoError(t, err)

	// Verify it was stored
	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldInsertItemInRedisStore(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Act
	err := store.Insert(context.Background(), item)

	// Assert
	assert.NoError(t, err)

	// Verify it was stored
	retrieved, err := store.Get(context.Background(), pk)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, value, retrieved.Value)
}

func TestShouldFailInsertWhenItemExistsInRedisStore(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Insert first time
	err := store.Insert(context.Background(), item)
	assert.NoError(t, err)

	// Act - Insert again
	err = store.Insert(context.Background(), item)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, kv.ErrAlreadyExists, err)
}

func TestShouldRemoveItemFromRedisStore(t *testing.T) {
	// Arrange
	mockClient := newMockRedisClient()
	store := NewRedisStoreWithClient(mockClient, "")

	pk := lexkey.NewPrimaryKey(lexkey.Encode("partition"), lexkey.Encode("row"))
	value := []byte("test value")
	item := &kv.Item{PK: pk, Value: value}

	// Put item first
	err := store.Put(context.Background(), item)
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
