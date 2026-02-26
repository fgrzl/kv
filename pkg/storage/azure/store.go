package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	client "github.com/fgrzl/azkit/tables"
	"github.com/fgrzl/enumerators"
	kv "github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// TableClient defines the operations used by the store against Azure Table Storage.
// *client.HTTPTableClient implements this interface.
type TableClient interface {
	CreateTable(ctx context.Context) error
	GetEntity(ctx context.Context, pk, rk string) ([]byte, error)
	AddEntity(ctx context.Context, data []byte) error
	UpsertEntity(ctx context.Context, data []byte, mode string) error
	DeleteEntity(ctx context.Context, pk, rk string) error
	NewListEntitiesPager(filter, selectCols string, top int32) *client.ListEntitiesPager
	SubmitBatch(ctx context.Context, ops []client.BatchOp) error
}

type store struct {
	options *TableProviderOptions
	client  TableClient
}

var _ kv.KV = (*store)(nil)

// Entity is the Azure Table Storage row shape used by the store.
// PartitionKey and RowKey are lexicographically encoded; Value is opaque.
type Entity struct {
	PartitionKey lexkey.LexKey `json:"PartitionKey"`
	RowKey       lexkey.LexKey `json:"RowKey"`
	Value        []byte        `json:"Value"`
}

// NewAzureStore creates a kv.KV backed by Azure Table Storage using the given options.
// It requires WithEndpoint and either WithSharedKey or WithManagedIdentity.
// The table is created if it does not exist.
func NewAzureStore(opts ...StoreOption) (kv.KV, error) {
	options := &TableProviderOptions{}
	for _, opt := range opts {
		opt(options)
	}
	client, err := getClient(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create table client: %w", err)
	}
	s := &store{options: options, client: client}
	if err := s.createTableIfNotExists(context.Background()); err != nil {
		return nil, err
	}
	slog.InfoContext(context.Background(), "Azure table store initialized", "table", options.Table)
	return s, nil
}

// NewAzureStoreWithClient creates a new Azure-backed kv.KV store with a provided client.
// This is primarily for testing purposes.
func NewAzureStoreWithClient(client TableClient, opts ...StoreOption) (kv.KV, error) {
	options := &TableProviderOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return &store{options: options, client: client}, nil
}

func (s *store) createTableIfNotExists(ctx context.Context) error {
	return s.client.CreateTable(ctx)
}

func (s *store) Get(ctx context.Context, pk lexkey.PrimaryKey) (*kv.Item, error) {
	resp, err := s.client.GetEntity(ctx, pk.PartitionKey.ToHexString(), pk.RowKey.ToHexString())
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		slog.ErrorContext(ctx, "failed to get entity", "pk", pk, "err", err)
		return nil, fmt.Errorf("get entity failed: %w", err)
	}
	var entity Entity
	if err := json.Unmarshal(resp, &entity); err != nil {
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
	err = s.client.AddEntity(ctx, entityJSON)
	if err != nil {
		var azErr *client.AzureError
		if errors.As(err, &azErr) && azErr.StatusCode == 409 {
			return kv.ErrAlreadyExists
		}
		slog.ErrorContext(ctx, "insert failed", "pk", item.PK, "err", err)
		return err
	}
	return nil
}

func (s *store) Put(ctx context.Context, item *kv.Item) error {
	entityJSON, err := json.Marshal(Entity{PartitionKey: item.PK.PartitionKey, RowKey: item.PK.RowKey, Value: item.Value})
	if err != nil {
		return err
	}
	err = s.client.UpsertEntity(ctx, entityJSON, "Replace")
	if err != nil {
		slog.ErrorContext(ctx, "put failed", "pk", item.PK, "err", err)
	}
	return err
}

func (s *store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	err := s.client.DeleteEntity(ctx, pk.PartitionKey.ToHexString(), pk.RowKey.ToHexString())
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
	top := normalizeLimit(args.Limit)
	pager := s.client.NewListEntitiesPager(derefString(filter), "", top)
	return AzureEnumerator(ctx, pager, args.Limit)
}

func (s *store) Batch(ctx context.Context, items []*kv.BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) > 100 {
		return kv.ErrInvalidBatchOperation
	}

	batchesByPartition := make(map[string][]*kv.BatchItem)
	for _, item := range items {
		key := item.PK.PartitionKey.ToHexString()
		batchesByPartition[key] = append(batchesByPartition[key], item)
	}

	for _, batch := range batchesByPartition {
		if err := s.batchSinglePartition(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (s *store) batchSinglePartition(ctx context.Context, items []*kv.BatchItem) error {
	var ops []client.BatchOp
	for _, item := range items {
		switch item.Op {
		case kv.Put:
			raw, err := json.Marshal(Entity{
				PartitionKey: item.PK.PartitionKey,
				RowKey:       item.PK.RowKey,
				Value:        item.Value,
			})
			if err != nil {
				return fmt.Errorf("marshal failed: %w", err)
			}
			ops = append(ops, client.BatchOp{Type: client.BatchInsertReplace, Entity: raw})
		case kv.Delete:
			ops = append(ops, client.BatchOp{
				Type:         client.BatchDelete,
				PartitionKey: item.PK.PartitionKey.ToHexString(),
				RowKey:       item.PK.RowKey.ToHexString(),
			})
		default:
			return kv.ErrInvalidBatchOperation
		}
	}

	err := s.client.SubmitBatch(ctx, ops)
	if err != nil && errors.Is(err, io.ErrUnexpectedEOF) {
		return nil // Azurite quirk: sometimes returns unexpected EOF on batch success
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

func normalizeLimit(limit int) int32 {
	val := int32(100)
	if limit > 0 && limit <= 100 {
		val = int32(limit)
	}
	return val
}

func isNotFound(err error) bool {
	var azErr *client.AzureError
	if errors.As(err, &azErr) && azErr.StatusCode == 404 {
		return true
	}
	return strings.Contains(err.Error(), "ResourceNotFound")
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// escapeODataString escapes a value for use inside an OData string literal (single-quoted).
// Single quotes are doubled per OData spec.
func escapeODataString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func buildFilter(args kv.QueryArgs) (*string, error) {
	pk := escapeODataString(args.PartitionKey.ToHexString())
	switch args.Operator {
	case kv.Scan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
	case kv.Equal:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", pk, escapeODataString(args.StartRowKey.ToHexString()))), nil
	case kv.GreaterThan:
		startKey := escapeODataString(args.StartRowKey.ToHexString())
		if args.StartRowKey.ToHexString() == "" {
			return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
		}
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s'", pk, startKey)), nil
	case kv.GreaterThanOrEqual:
		startKey := escapeODataString(args.StartRowKey.ToHexString())
		if args.StartRowKey.ToHexString() == "" {
			return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
		}
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", pk, startKey)), nil
	case kv.LessThan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey lt '%s'", pk, escapeODataString(args.EndRowKey.ToHexString()))), nil
	case kv.LessThanOrEqual:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", pk, escapeODataString(args.EndRowKey.ToHexString()))), nil
	case kv.Between:
		startKey := escapeODataString(args.StartRowKey.ToHexString())
		endKey := escapeODataString(args.EndRowKey.ToHexString())
		if args.StartRowKey.ToHexString() == "" {
			return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", pk, endKey)), nil
		}
		if args.EndRowKey.ToHexString() == "" {
			return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", pk, startKey)), nil
		}
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s' and RowKey le '%s'", pk, startKey, endKey)), nil
	case kv.StartsWith:
		return nil, fmt.Errorf("StartsWith is not natively supported")
	default:
		return nil, fmt.Errorf("unsupported operator: %v", args.Operator)
	}
}

func ptr[T any](v T) *T {
	return &v
}
