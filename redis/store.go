package redis

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"github.com/redis/go-redis/v9"
)

var _ kv.KV = (*Store)(nil)

type Store struct {
	client *redis.Client
	prefix string
	mu     sync.Mutex
}

func NewRedisStore(options ...Option) (kv.KV, error) {
	cfg := applyOptions(options...)

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		slog.ErrorContext(ctx, "redis ping failed", "err", err)
		return nil, err
	}

	slog.DebugContext(ctx, "redis client initialized", "addr", cfg.Addr, "db", cfg.DB)
	return &Store{client: client, prefix: cfg.Prefix}, nil
}

func (r *Store) Clear() {
	_ = r.client.FlushDB(context.Background()).Err()
}

func (r *Store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	key := pk.Encode().ToHexString()
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		slog.ErrorContext(ctx, "redis get failed", "key", key, "err", err)
		return nil, err
	}
	return &kv.Item{PK: pk, Value: val}, nil
}

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
		slog.ErrorContext(ctx, "redis pipeline get failed", "err", err)
		return nil, err
	}

	results := make([]*kv.Item, 0, len(keys))
	for i, cmd := range cmds {
		val, err := cmd.Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			slog.ErrorContext(ctx, "redis get error", "index", i, "err", err)
			return nil, err
		}
		results = append(results, &kv.Item{PK: keys[i], Value: val})
	}
	return results, nil
}

func (r *Store) Insert(ctx context.Context, item *kv.Item) error {
	key := item.PK.Encode().ToHexString()
	ok, err := r.client.SetNX(ctx, key, item.Value, 0).Result()
	if err != nil {
		slog.ErrorContext(ctx, "redis insert failed", "key", key, "err", err)
		return err
	}
	if !ok {
		return kv.ErrAlreadyExists
	}
	return nil
}

func (r *Store) Put(ctx context.Context, item *kv.Item) error {
	key := item.PK.Encode().ToHexString()
	err := r.client.Set(ctx, key, item.Value, 0).Err()
	if err != nil {
		slog.ErrorContext(ctx, "redis put failed", "key", key, "err", err)
		return err
	}
	return nil
}

func (r *Store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	key := pk.Encode().ToHexString()
	err := r.client.Del(ctx, key).Err()
	if err != nil {
		slog.ErrorContext(ctx, "redis delete failed", "key", key, "err", err)
		return err
	}
	return nil
}

func (r *Store) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	if len(keys) == 0 {
		return nil
	}
	strKeys := make([]string, len(keys))
	for i, pk := range keys {
		strKeys[i] = pk.Encode().ToHexString()
	}
	if err := r.client.Del(ctx, strKeys...).Err(); err != nil {
		slog.ErrorContext(ctx, "redis batch delete failed", "count", len(keys), "err", err)
		return err
	}
	return nil
}

func (r *Store) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {
	iter := r.client.Scan(ctx, 0, rangeKey.PartitionKey.ToHexString()+"*", 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		key := iter.Val()
		var encoded lexkey.LexKey
		if err := encoded.FromHexString(key); err != nil {
			slog.WarnContext(ctx, "invalid lexkey during scan", "key", key, "err", err)
			continue
		}
		pk := lexkey.NewPrimaryKey(encoded[:len(rangeKey.PartitionKey)], encoded[len(rangeKey.PartitionKey)+1:])
		if bytes.Equal(pk.PartitionKey, rangeKey.PartitionKey) {
			continue
		}
		if bytes.Compare(pk.RowKey, rangeKey.StartRowKey) >= 0 && bytes.Compare(pk.RowKey, rangeKey.EndRowKey) <= 0 {
			keys = append(keys, key)
		}
	}
	if err := iter.Err(); err != nil {
		slog.ErrorContext(ctx, "redis scan failed", "err", err)
		return err
	}
	if len(keys) > 0 {
		if err := r.client.Del(ctx, keys...).Err(); err != nil {
			slog.ErrorContext(ctx, "redis delete range failed", "count", len(keys), "err", err)
			return err
		}
	}
	return nil
}

func (r *Store) Query(ctx context.Context, args kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {
	items, err := enumerators.ToSlice(r.Enumerate(ctx, args))
	if err != nil {
		return nil, err
	}
	kv.SortItems(items, sort)
	return items, nil
}

func (r *Store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
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

	rk := lexkey.RangeKey{
		PartitionKey: args.PartitionKey,
		StartRowKey:  args.StartRowKey,
		EndRowKey:    args.EndRowKey,
	}
	match := rk.PartitionKey.ToHexString() + "*"
	filter := getOperatorFunctions(args.Operator)
	iter := r.client.Scan(ctx, 0, match, 0).Iterator()

	rawEnum := RedisEnumerator(ctx, iter)
	var count int

	return enumerators.FilterMap(rawEnum, func(key string) (*kv.Item, bool, error) {
		if args.Limit > 0 && count >= args.Limit {
			return nil, false, nil
		}
		var encoded lexkey.LexKey
		if err := encoded.FromHexString(key); err != nil {
			slog.WarnContext(ctx, "invalid lexkey", "key", key, "err", err)
			return nil, false, kv.ErrInvalidLexKey
		}
		pk := lexkey.NewPrimaryKey(encoded[:len(rk.PartitionKey)], encoded[len(rk.PartitionKey)+1:])
		if !filter(pk, rk) {
			return nil, false, nil
		}
		item, err := r.Get(ctx, pk)
		if err != nil {
			slog.WarnContext(ctx, "enumerate get failed", "key", key, "err", err)
			return nil, false, err
		}
		count++
		return item, true, nil
	})
}

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
			slog.WarnContext(ctx, "invalid batch op", "index", i, "op", item.Op)
			return kv.ErrInvalidBatchOperation
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "batch exec failed", "err", err)
		return fmt.Errorf("%w: %v", kv.ErrBackendExecution, err)
	}
	return nil
}

func (r *Store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	return enumerators.ForEach(
		enumerators.ChunkByCount(items, chunkSize),
		func(chunk enumerators.Enumerator[*kv.BatchItem]) error {
			var batch []*kv.BatchItem
			if err := enumerators.ForEach(chunk, func(item *kv.BatchItem) error {
				batch = append(batch, item)
				return nil
			}); err != nil {
				return err
			}
			if len(batch) > 0 {
				return r.Batch(ctx, batch)
			}
			return nil
		},
	)
}

func (r *Store) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client == nil {
		return nil
	}
	err := r.client.Close()
	r.client = nil
	return err
}

func getOperatorFunctions(op kv.QueryOperator) func(pk lexkey.PrimaryKey, rk lexkey.RangeKey) bool {
	switch op {
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
