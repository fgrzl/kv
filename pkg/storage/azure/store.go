package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
	kv "github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// AzureTableClient defines the interface for Azure Table operations used in unit tests.
type AzureTableClient interface {
	CreateTable(ctx context.Context, options *aztables.CreateTableOptions) (interface{}, error)
	GetEntity(ctx context.Context, partitionKey, rowKey string, options *aztables.GetEntityOptions) (aztables.GetEntityResponse, error)
	UpsertEntity(ctx context.Context, entity []byte, options *aztables.UpsertEntityOptions) (interface{}, error)
	AddEntity(ctx context.Context, entity []byte, options *aztables.AddEntityOptions) (interface{}, error)
	DeleteEntity(ctx context.Context, partitionKey, rowKey string, options *aztables.DeleteEntityOptions) (interface{}, error)
	NewListEntitiesPager(options *aztables.ListEntitiesOptions) interface{}
	SubmitTransaction(ctx context.Context, operations []aztables.TransactionAction, options *aztables.SubmitTransactionOptions) (interface{}, error)
}

// realAzureClient wraps *aztables.Client to implement AzureTableClient.
type realAzureClient struct {
	*aztables.Client
}

func (r *realAzureClient) CreateTable(ctx context.Context, options *aztables.CreateTableOptions) (interface{}, error) {
	return r.Client.CreateTable(ctx, options)
}

func (r *realAzureClient) UpsertEntity(ctx context.Context, entity []byte, options *aztables.UpsertEntityOptions) (interface{}, error) {
	return r.Client.UpsertEntity(ctx, entity, options)
}

func (r *realAzureClient) AddEntity(ctx context.Context, entity []byte, options *aztables.AddEntityOptions) (interface{}, error) {
	return r.Client.AddEntity(ctx, entity, options)
}

func (r *realAzureClient) DeleteEntity(ctx context.Context, partitionKey, rowKey string, options *aztables.DeleteEntityOptions) (interface{}, error) {
	return r.Client.DeleteEntity(ctx, partitionKey, rowKey, options)
}

func (r *realAzureClient) NewListEntitiesPager(options *aztables.ListEntitiesOptions) interface{} {
	return r.Client.NewListEntitiesPager(options)
}

func (r *realAzureClient) SubmitTransaction(ctx context.Context, operations []aztables.TransactionAction, options *aztables.SubmitTransactionOptions) (interface{}, error) {
	return r.Client.SubmitTransaction(ctx, operations, options)
}

type store struct {
	options *TableProviderOptions
	client  AzureTableClient
}

var _ kv.KV = (*store)(nil)

type Entity struct {
	PartitionKey lexkey.LexKey `json:"PartitionKey"`
	RowKey       lexkey.LexKey `json:"RowKey"`
	Value        []byte        `json:"Value"`
}

func NewAzureStore(opts ...StoreOption) (kv.KV, error) {
	options := &TableProviderOptions{}
	for _, opt := range opts {
		opt(options)
	}
	client, err := getClient(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create table client: %w", err)
	}
	s := &store{options: options, client: &realAzureClient{Client: client}}
	if err := s.createTableIfNotExists(context.Background()); err != nil {
		return nil, err
	}
	slog.InfoContext(context.Background(), "Azure table store initialized", "table", options.Table)
	return s, nil
}

// NewAzureStoreWithClient creates a new Azure-backed kv.KV store with a provided client.
// This is primarily for testing purposes.
func NewAzureStoreWithClient(client AzureTableClient, opts ...StoreOption) (kv.KV, error) {
	options := &TableProviderOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return &store{options: options, client: client}, nil
}

func (s *store) createTableIfNotExists(ctx context.Context) error {
	_, err := s.client.CreateTable(ctx, nil)
	var respErr *azcore.ResponseError
	if err == nil || (errors.As(err, &respErr) && respErr.ErrorCode == string(aztables.TableAlreadyExists)) {
		return nil
	}
	slog.ErrorContext(ctx, "failed to create table", "err", err)
	return fmt.Errorf("failed to create table: %w", err)
}

// responseError interface for error checking
type responseError interface {
	StatusCode() int
}

func (s *store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	resp, err := s.client.GetEntity(ctx, pk.PartitionKey.ToHexString(), pk.RowKey.ToHexString(), nil)
	if err != nil {
		var azErr *azcore.ResponseError
		if errors.As(err, &azErr) && azErr.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		var respErr responseError
		if errors.As(err, &respErr) && respErr.StatusCode() == http.StatusNotFound {
			return nil, nil
		}
		slog.ErrorContext(ctx, "failed to get entity", "pk", pk, "err", err)
		return nil, fmt.Errorf("get entity failed: %w", err)
	}
	var entity Entity
	if err := json.Unmarshal(resp.Value, &entity); err != nil {
		slog.ErrorContext(ctx, "failed to decode entity", "err", err)
		return nil, fmt.Errorf("decode failed: %w", err)
	}
	return &kv.Item{PK: pk, Value: entity.Value}, nil
}

func (s *store) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]*kv.Item, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	items := make([]*kv.Item, 0, len(keys))

	for _, key := range keys {
		wg.Add(1)
		go func(pk lexkey.PrimaryKey) {
			defer wg.Done()
			item, err := s.Get(ctx, pk)
			if err != nil {
				slog.WarnContext(ctx, "get batch item failed", "pk", pk, "err", err)
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if item != nil {
				mu.Lock()
				items = append(items, item)
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()
	return items, firstErr
}

func (s *store) Insert(ctx context.Context, item *kv.Item) error {
	entityJSON, err := json.Marshal(Entity{PartitionKey: item.PK.PartitionKey, RowKey: item.PK.RowKey, Value: item.Value})
	if err != nil {
		return err
	}
	_, err = s.client.AddEntity(ctx, entityJSON, nil)
	var azErr *azcore.ResponseError
	if errors.As(err, &azErr) && azErr.StatusCode == http.StatusConflict {
		return kv.ErrAlreadyExists
	}
	var respErr responseError
	if errors.As(err, &respErr) && respErr.StatusCode() == http.StatusConflict {
		return kv.ErrAlreadyExists
	}
	if err != nil {
		slog.ErrorContext(ctx, "insert failed", "pk", item.PK, "err", err)
	}
	return err
}

func (s *store) Put(ctx context.Context, item *kv.Item) error {
	entityJSON, err := json.Marshal(Entity{PartitionKey: item.PK.PartitionKey, RowKey: item.PK.RowKey, Value: item.Value})
	if err != nil {
		return err
	}
	_, err = s.client.UpsertEntity(ctx, entityJSON, &aztables.UpsertEntityOptions{UpdateMode: aztables.UpdateModeReplace})
	if err != nil {
		slog.ErrorContext(ctx, "put failed", "pk", item.PK, "err", err)
	}
	return err
}

func (s *store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	_, err := s.client.DeleteEntity(ctx, pk.PartitionKey.ToHexString(), pk.RowKey.ToHexString(), nil)
	var azErr *azcore.ResponseError
	if errors.As(err, &azErr) && azErr.StatusCode == http.StatusNotFound {
		return nil
	}
	var respErr responseError
	if errors.As(err, &respErr) && respErr.StatusCode() == http.StatusNotFound {
		return nil
	}
	if err != nil {
		slog.ErrorContext(ctx, "delete failed", "pk", pk, "err", err)
	}
	return err
}

func (s *store) RemoveBatch(ctx context.Context, keys ...lexkey.PrimaryKey) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, key := range keys {
		wg.Add(1)
		go func(pk lexkey.PrimaryKey) {
			defer wg.Done()
			if err := s.Remove(ctx, pk); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()
	return firstErr
}

func (s *store) RemoveRange(ctx context.Context, rangeKey lexkey.RangeKey) error {
	args := kv.QueryArgs{
		PartitionKey: rangeKey.PartitionKey,
		StartRowKey:  rangeKey.StartRowKey,
		EndRowKey:    rangeKey.EndRowKey,
		Operator:     kv.Between,
	}
	enum := s.Enumerate(ctx, args)
	defer enum.Dispose()

	batch := make([]*kv.BatchItem, 0, 100)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.Batch(ctx, batch); err != nil {
			slog.ErrorContext(ctx, "range batch delete failed", "err", err)
			return err
		}
		batch = batch[:0]
		return nil
	}

	err := enumerators.ForEach(enum, func(item *kv.Item) error {
		batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: item.PK})
		if len(batch) == 100 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func (s *store) Query(ctx context.Context, args kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {
	items, err := enumerators.ToSlice(s.Enumerate(ctx, args))
	if err != nil {
		return nil, err
	}
	if sort == kv.Descending {
		kv.ReverseItems(items)
	}
	return items, nil
}

func (s *store) Enumerate(ctx context.Context, args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	if args.Operator == kv.Equal {
		item, err := s.Get(ctx, lexkey.PrimaryKey{PartitionKey: args.PartitionKey, RowKey: args.StartRowKey})
		if err != nil {
			return enumerators.Error[*kv.Item](err)
		}
		if item == nil {
			return enumerators.Empty[*kv.Item]()
		}
		return enumerators.Slice([]*kv.Item{item})
	}
	filter, err := buildFilter(args)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}
	pager := s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top:    normalizeLimit(args.Limit),
		Filter: filter,
	}).(*runtime.Pager[aztables.ListEntitiesResponse])
	return AzureEnumerator(ctx, pager, args.Limit)
}

func (s *store) Batch(ctx context.Context, items []*kv.BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > 100 {
		return kv.ErrInvalidBatchOperation
	}

	// Group items by partition key
	batchesByPartition := make(map[string][]*kv.BatchItem)
	for _, item := range items {
		key := string(item.PK.PartitionKey)
		batchesByPartition[key] = append(batchesByPartition[key], item)
	}

	// Process each partition batch
	for _, batch := range batchesByPartition {
		if err := s.batchSinglePartition(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (s *store) batchSinglePartition(ctx context.Context, items []*kv.BatchItem) error {
	var ops []aztables.TransactionAction
	for _, item := range items {
		var typ aztables.TransactionType
		switch item.Op {
		case kv.Put:
			typ = aztables.TransactionTypeInsertReplace
		case kv.Delete:
			typ = aztables.TransactionTypeDelete
		default:
			return kv.ErrInvalidBatchOperation
		}
		raw, err := json.Marshal(Entity{
			PartitionKey: item.PK.PartitionKey,
			RowKey:       item.PK.RowKey,
			Value:        item.Value,
		})
		if err != nil {
			return fmt.Errorf("marshal failed: %w", err)
		}
		ops = append(ops, aztables.TransactionAction{ActionType: typ, Entity: raw})
	}

	_, err := s.client.SubmitTransaction(ctx, ops, nil)
	if err != nil && err.Error() == "unexpected EOF" {
		return nil // Azurite quirk
	}
	return err
}

func (s *store) BatchChunks(ctx context.Context, items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
	return enumerators.ForEach(
		enumerators.ChunkByCount(items, chunkSize),
		func(chunk enumerators.Enumerator[*kv.BatchItem]) error {
			var batch []*kv.BatchItem
			err := enumerators.ForEach(chunk, func(item *kv.BatchItem) error {
				batch = append(batch, item)
				return nil
			})
			if err != nil {
				return err
			}
			if len(batch) > 0 {
				return s.Batch(ctx, batch)
			}
			return nil
		},
	)
}

func (s *store) Close() error {
	return nil
}

func normalizeLimit(limit int) *int32 {
	val := int32(100)
	if limit > 0 && limit <= 100 {
		val = int32(limit)
	}
	return &val
}

func buildFilter(args kv.QueryArgs) (*string, error) {
	pk := args.PartitionKey.ToHexString()
	switch args.Operator {
	case kv.Scan:
		return nil, nil
	case kv.Equal:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", pk, args.StartRowKey.ToHexString())), nil
	case kv.GreaterThan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s'", pk, args.StartRowKey.ToHexString())), nil
	case kv.GreaterThanOrEqual:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", pk, args.StartRowKey.ToHexString())), nil
	case kv.LessThan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey lt '%s'", pk, args.EndRowKey.ToHexString())), nil
	case kv.LessThanOrEqual:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", pk, args.EndRowKey.ToHexString())), nil
	case kv.Between:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s' and RowKey le '%s'",
			pk, args.StartRowKey.ToHexString(), args.EndRowKey.ToHexString())), nil
	case kv.StartsWith:
		return nil, fmt.Errorf("StartsWith is not natively supported")
	default:
		return nil, fmt.Errorf("unsupported operator: %v", args.Operator)
	}
}

func ptr[T any](v T) *T {
	return &v
}
