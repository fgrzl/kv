package redis

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/redis/go-redis/v9"
)

var _ kv.KV = (*Store)(nil)

// Store implements the kv.KV interface using Redis.
type Store struct {
	client *redis.Client
	prefix string // Optional prefix for all keys
	mu     sync.Mutex
}

// RedisOptions holds configuration options for the Redis provider.
type RedisOptions struct {
	Addr     string // e.g., "localhost:6379"
	Password string // Optional
	DB       int    // Redis database number (default 0)
	Prefix   string // Optional key prefix
}

// NewRedisStore initializes a new Redis provider.
func NewRedisStore(options *RedisOptions) (kv.KV, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     options.Addr,
		Password: options.Password,
		DB:       options.DB,
	})

	// Ping to verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Store{
		client: client,
		prefix: options.Prefix,
	}, nil
}

func (r *Store) Clear() {
	ctx := context.Background()
	err := r.client.FlushDB(ctx).Err()
	if err != nil {
		panic(err)
	}
}

// Get retrieves an item by its PrimaryKey.
func (r *Store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {

	val, err := r.client.Get(ctx, pk.Encode().ToHexString()).Bytes()
	if err == redis.Nil {
		return nil, nil // Key not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get item: %w", err)
	}
	return &kv.Item{PK: pk, Value: val}, nil
}

// GetBatch retrieves multiple items concurrently.
func (r *Store) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]*kv.Item, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, pk := range keys {
		cmds[i] = pipe.Get(ctx, pk.Encode().ToHexString())
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get batch: %w", err)
	}

	results := make([]*kv.Item, 0, len(keys))
	for i, cmd := range cmds {
		val, err := cmd.Bytes()
		if err == redis.Nil {
			continue // Skip missing keys
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get item %d: %w", i, err)
		}

		item := &kv.Item{PK: keys[i], Value: val}

		results = append(results, item)
	}
	return results, nil
}

// Put inserts or updates an item.
func (r *Store) Put(ctx context.Context, item *kv.Item) error {

	err := r.client.Set(ctx, item.PK.Encode().ToHexString(), item.Value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}
	return nil
}

// Remove deletes an item by its PrimaryKey.
func (r *Store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {

	err := r.client.Del(ctx, pk.Encode().ToHexString()).Err()
	if err != nil {
		return fmt.Errorf("failed to remove item: %w", err)
	}
	return nil
}

// RemoveBatch deletes multiple items concurrently.
func (r *Store) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	if len(keys) == 0 {
		return nil
	}

	redisKeys := make([]string, len(keys))
	for i, pk := range keys {
		redisKeys[i] = pk.Encode().ToHexString()
	}
	err := r.client.Del(ctx, redisKeys...).Err()
	if err != nil {
		return fmt.Errorf("failed to remove batch: %w", err)
	}
	return nil
}

// RemoveRange removes items within a range (simulated with SCAN).
func (r *Store) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {

	match := rangeKey.PartitionKey.ToHexString() + "*"

	iter := r.client.Scan(ctx, 0, match, 0).Iterator()
	var keys []string
	for iter.Next(ctx) {

		key := iter.Val()

		var encodedKey lexkey.LexKey
		err := encodedKey.FromHexString(key)
		if err != nil {
			return err
		}
		pk := lexkey.NewPrimaryKey(
			encodedKey[:len(rangeKey.PartitionKey)],
			encodedKey[len(rangeKey.PartitionKey)+1:],
		)

		if bytes.Equal(pk.PartitionKey, rangeKey.PartitionKey) {
			continue
		}

		if bytes.Compare(pk.RowKey, rangeKey.StartRowKey) >= 0 && bytes.Compare(pk.RowKey, rangeKey.EndRowKey) <= 0 {
			keys = append(keys, key)
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan range: %w", err)
	}

	if len(keys) > 0 {
		err := r.client.Del(ctx, keys...).Err()
		if err != nil {
			return fmt.Errorf("failed to remove range: %w", err)
		}
	}
	return nil
}

// Query retrieves items matching the query args with specified sorting.
func (r *Store) Query(ctx context.Context, args kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {
	enumerator := r.Enumerate(ctx, args)
	items, err := enumerators.ToSlice(enumerator)
	if err != nil {
		return nil, err
	}

	kv.SortItems(items, sort)
	return items, nil
}

// Enumerate returns an Enumerator for items matching the query args.
func (r *Store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {

	// Shortcut for Equal operator
	if args.Operator == kv.Equal {
		pk := lexkey.PrimaryKey{PartitionKey: args.PartitionKey, RowKey: args.StartRowKey}
		item, err := r.Get(ctx, pk)
		if err != nil {
			return enumerators.Error[*kv.Item](err)
		}
		if item == nil {
			return enumerators.Empty[*kv.Item]()
		}
		return enumerators.Slice([]*kv.Item{item})
	}

	rangeKey := lexkey.RangeKey{
		PartitionKey: args.PartitionKey,
		StartRowKey:  args.StartRowKey,
		EndRowKey:    args.EndRowKey,
	}
	satifies := getOperatorFunctions(args.Operator)

	match := rangeKey.PartitionKey.ToHexString() + "*"
	iter := r.client.Scan(ctx, 0, match, 0).Iterator()

	enumerator := Enumerator(context.Background(), iter)

	var totalCount int
	items := enumerators.FilterMap(
		enumerator,
		func(key string) (*kv.Item, bool, error) {
			if args.Limit > 0 && totalCount >= args.Limit {
				return nil, false, nil
			}

			var encodedKey lexkey.LexKey
			err := encodedKey.FromHexString(key)
			if err != nil {
				return nil, false, err
			}

			pk := lexkey.NewPrimaryKey(
				encodedKey[:len(rangeKey.PartitionKey)],
				encodedKey[len(rangeKey.PartitionKey)+1:],
			)

			if !satifies(pk, rangeKey) {
				return nil, false, nil
			}

			item, err := r.Get(ctx, pk)
			if err != nil {
				return nil, false, err
			}

			totalCount++
			return item, true, nil
		})

	return items
}

// Batch performs a batch of operations (not atomic in Redis).
func (r *Store) Batch(ctx context.Context, items []*kv.BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	for i, item := range items {
		key := item.PK.Encode().ToHexString()
		switch item.Op {
		case kv.Put:
			pipe.Set(ctx, key, item.Value, 0)
		case kv.Delete:
			pipe.Del(ctx, key)
		default:
			return fmt.Errorf("unsupported batch operation %v at index %d", item.Op, i)
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch: %w", err)
	}
	return nil
}

// BatchChunks processes batched items in chunks.
func (r *Store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	defer items.Dispose()
	chunks := enumerators.ChunkByCount(items, chunkSize)
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return fmt.Errorf("failed to retrieve chunk: %w", err)
		}
		batch := make([]*kv.BatchItem, 0, chunkSize)
		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				return fmt.Errorf("failed to retrieve item in chunk: %w", err)
			}
			batch = append(batch, item)
		}
		if err := r.Batch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// Close releases resources.
func (r *Store) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		if err != nil {
			return fmt.Errorf("failed to close Redis client: %w", err)
		}
	}
	return nil
}

func getOperatorFunctions(operator kv.QueryOperator) func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {

	switch operator {
	case kv.GreaterThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rk.StartRowKey) > 0
		}

	case kv.GreaterThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0
		}

	case kv.LessThan:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rk.EndRowKey) < 0
		}

	case kv.LessThanOrEqual:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
		}

	case kv.Between:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.Compare(pk.RowKey, rk.StartRowKey) >= 0 &&
				bytes.Compare(pk.RowKey, rk.EndRowKey) <= 0
		}

	case kv.StartsWith:
		return func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
			return bytes.HasPrefix(pk.RowKey, rk.StartRowKey)
		}

	default:
		return func(_ lexkey.PrimaryKey, _ lexkey.RangeKey) bool { return true }
	}
}
