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
	slog.DebugContext(context.Background(), "Azure table store initialized", "table", options.Table)
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
	storedPK := pkToStore(pk)
	resp, err := s.client.GetEntity(ctx, storedPK.PartitionKey.ToHexString(), storedPK.RowKey.ToHexString())
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		slog.ErrorContext(ctx, "failed to get entity", "table", s.options.Table, "pk", pk, "err", err)
		return nil, fmt.Errorf("get entity failed: %w", err)
	}
	var entity Entity
	if err := json.Unmarshal(resp, &entity); err != nil {
		slog.ErrorContext(ctx, "failed to decode entity", "table", s.options.Table, "pk", pk, "err", err)
		return nil, fmt.Errorf("decode failed: %w", err)
	}
	return &kv.Item{PK: pk, Value: entity.Value}, nil
}

// getBatchMaxConcurrency limits concurrent partition requests to avoid throttling.
const getBatchMaxConcurrency = 20

// getBatchMaxRowKeysPerQuery caps predicates per OData filter.
const getBatchMaxRowKeysPerQuery = 100

// getBatchMaxFilterLength keeps filters below Table Storage URL/query limits (~32KB max; 2–3KB safe for all environments including Azurite).
const getBatchMaxFilterLength = 2048

// getBatchSlot holds request index, key, and precomputed hex for GetBatch (avoids repeated pkToStore/ToHexString).
type getBatchSlot struct {
	idx     int
	pk      lexkey.PrimaryKey
	partHex string
	rkHex   string
}

func fillSlotsNotFound(slots []getBatchSlot, results []kv.BatchGetResult) {
	for _, sl := range slots {
		results[sl.idx] = kv.BatchGetResult{Item: nil, Found: false}
	}
}

func assignFirstErr(mu *sync.Mutex, dst *error, err error) {
	mu.Lock()
	if *dst == nil {
		*dst = err
	}
	mu.Unlock()
}

func (s *store) GetBatch(ctx context.Context, keys ...lexkey.PrimaryKey) ([]kv.BatchGetResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	results := make([]kv.BatchGetResult, len(keys))
	byPartition := make(map[string][]getBatchSlot)
	for i, pk := range keys {
		stored := pkToStore(pk)
		partHex := stored.PartitionKey.ToHexString()
		rkHex := stored.RowKey.ToHexString()
		byPartition[partHex] = append(byPartition[partHex], getBatchSlot{idx: i, pk: pk, partHex: partHex, rkHex: rkHex})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, getBatchMaxConcurrency)

	for partHex, slots := range byPartition {
		partHex, slots := partHex, slots
		if ctx.Err() != nil {
			fillSlotsNotFound(slots, results)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.getBatchPartition(ctx, partHex, slots, results); err != nil {
				assignFirstErr(&mu, &firstErr, err)
				fillSlotsNotFound(slots, results)
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return results, firstErr
	}
	if ctx.Err() != nil {
		return results, ctx.Err()
	}
	return results, nil
}

// getBatchPartition fetches all keys in one partition: one GetEntity for a single key, OData filter query for multiple.
func (s *store) getBatchPartition(ctx context.Context, partHex string, slots []getBatchSlot, results []kv.BatchGetResult) error {
	if len(slots) == 0 || ctx.Err() != nil {
		return ctx.Err()
	}
	uniqueRKHexes, slotsByRK := groupBatchSlotsByStoredRowKey(slots)
	if len(uniqueRKHexes) == 1 {
		return s.getBatchPoint(ctx, partHex, uniqueRKHexes[0], slotsByRK[uniqueRKHexes[0]], results)
	}

	queryChunks, pointGetRKHexes := splitBatchRowKeysForQuery(partHex, uniqueRKHexes)
	var partWg sync.WaitGroup
	var partMu sync.Mutex
	var partErr error

	for _, rkHex := range pointGetRKHexes {
		rkHex := rkHex
		partWg.Add(1)
		go func() {
			defer partWg.Done()
			if ctx.Err() != nil {
				return
			}
			if err := s.getBatchPoint(ctx, partHex, rkHex, slotsByRK[rkHex], results); err != nil {
				assignFirstErr(&partMu, &partErr, err)
			}
		}()
	}
	for _, chunk := range queryChunks {
		chunk := chunk
		partWg.Add(1)
		go func() {
			defer partWg.Done()
			if ctx.Err() != nil {
				return
			}
			filter := buildPartitionRowKeysFilter(partHex, chunk)
			pager := s.client.NewListEntitiesPager(filter, "PartitionKey,RowKey,Value", int32(len(chunk)))
			defer pager.Close()
			fetchedValues := make(map[string][]byte, len(chunk))
			for !pager.IsDone() {
				entities, err := pager.FetchPage(ctx)
				if err != nil {
					assignFirstErr(&partMu, &partErr, fmt.Errorf("fetch page: %w", err))
					return
				}
				for _, ent := range entities {
					fetchedValues[ent.RowKey] = ent.Value
				}
			}
			for _, rkHex := range chunk {
				slotsForRK := slotsByRK[rkHex]
				if value, ok := fetchedValues[rkHex]; ok {
					setBatchResults(slotsForRK, value, true, results)
				} else {
					setBatchResults(slotsForRK, nil, false, results)
				}
			}
		}()
	}
	partWg.Wait()
	return partErr
}

func (s *store) getBatchPoint(ctx context.Context, partHex, rkHex string, slots []getBatchSlot, results []kv.BatchGetResult) error {
	resp, err := s.client.GetEntity(ctx, partHex, rkHex)
	if err != nil {
		if isNotFound(err) {
			setBatchResults(slots, nil, false, results)
			return nil
		}
		return err
	}
	var entity Entity
	if err := json.Unmarshal(resp, &entity); err != nil {
		slog.ErrorContext(ctx, "getBatchPoint decode failed", "table", s.options.Table, "partition_key", partHex, "row_key", rkHex, "err", err)
		return fmt.Errorf("decode entity: %w", err)
	}
	setBatchResults(slots, entity.Value, true, results)
	return nil
}

func groupBatchSlotsByStoredRowKey(slots []getBatchSlot) ([]string, map[string][]getBatchSlot) {
	uniqueRKHexes := make([]string, 0, len(slots))
	slotsByRK := make(map[string][]getBatchSlot, len(slots))
	for _, sl := range slots {
		rkHex := sl.rkHex
		if _, exists := slotsByRK[rkHex]; !exists {
			uniqueRKHexes = append(uniqueRKHexes, rkHex)
		}
		slotsByRK[rkHex] = append(slotsByRK[rkHex], sl)
	}
	return uniqueRKHexes, slotsByRK
}

// splitBatchRowKeysForQuery splits row keys into query chunks (OData filter) and keys that must use point GetEntity (filter too long).
func splitBatchRowKeysForQuery(partHex string, rowKeyHexes []string) ([][]string, []string) {
	var queryChunks [][]string
	var pointGetRKHexes []string
	chunk := make([]string, 0, min(len(rowKeyHexes), getBatchMaxRowKeysPerQuery))

	pkEscaped := escapeODataString(partHex)
	baseLen := len("PartitionKey eq '") + len(pkEscaped) + len("' and (") + len(")")
	partitionOnlyLen := baseLen - len(" and ()")
	const rkClauseLen = len("RowKey eq '") + len("'")
	const orLen = len(" or ")

	rkEscapedLens := make([]int, len(rowKeyHexes))
	for i, rk := range rowKeyHexes {
		rkEscapedLens[i] = len(escapeODataString(rk))
	}

	// OData filter length: PartitionKey eq 'pk' and (RowKey eq 'r1' or RowKey eq 'r2' or ...)
	calcFilterLen := func(rkLens []int) int {
		if len(rkLens) == 0 {
			return partitionOnlyLen
		}
		n := len(rkLens)
		total := baseLen
		for _, l := range rkLens {
			total += rkClauseLen + l
		}
		if n > 1 {
			total += (n - 1) * orLen
		}
		return total
	}

	flushChunk := func() {
		if len(chunk) == 0 {
			return
		}
		queryChunks = append(queryChunks, append([]string(nil), chunk...))
		chunk = chunk[:0]
	}

	var chunkLens []int
	for i, rkHex := range rowKeyHexes {
		candidateLens := append(chunkLens, rkEscapedLens[i])
		candidateCount := len(chunk) + 1
		if candidateCount <= getBatchMaxRowKeysPerQuery && calcFilterLen(candidateLens) <= getBatchMaxFilterLength {
			chunk = append(chunk, rkHex)
			chunkLens = candidateLens
			continue
		}
		flushChunk()
		chunkLens = chunkLens[:0]
		singleLen := calcFilterLen([]int{rkEscapedLens[i]})
		if singleLen > getBatchMaxFilterLength {
			pointGetRKHexes = append(pointGetRKHexes, rkHex)
			continue
		}
		chunk = append(chunk, rkHex)
		chunkLens = append(chunkLens, rkEscapedLens[i])
	}
	flushChunk()
	return queryChunks, pointGetRKHexes
}

func setBatchResults(slots []getBatchSlot, value []byte, found bool, results []kv.BatchGetResult) {
	if !found {
		for _, sl := range slots {
			results[sl.idx] = kv.BatchGetResult{Item: nil, Found: false}
		}
		return
	}
	if len(slots) == 1 {
		results[slots[0].idx] = kv.BatchGetResult{
			Item:  &kv.Item{PK: slots[0].pk, Value: value},
			Found: true,
		}
		return
	}
	for _, sl := range slots {
		valueCopy := append([]byte(nil), value...)
		results[sl.idx] = kv.BatchGetResult{
			Item:  &kv.Item{PK: sl.pk, Value: valueCopy},
			Found: true,
		}
	}
}

// buildPartitionRowKeysFilter returns an OData filter for partition + row key set.
// Format: PartitionKey eq 'pk' and (RowKey eq 'r1' or RowKey eq 'r2' or ...).
func buildPartitionRowKeysFilter(partitionKeyHex string, rowKeyHexes []string) string {
	pk := escapeODataString(partitionKeyHex)
	if len(rowKeyHexes) == 0 {
		return fmt.Sprintf("PartitionKey eq '%s'", pk)
	}
	if len(rowKeyHexes) == 1 {
		return fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", pk, escapeODataString(rowKeyHexes[0]))
	}
	parts := make([]string, len(rowKeyHexes))
	for i, rk := range rowKeyHexes {
		parts[i] = "RowKey eq '" + escapeODataString(rk) + "'"
	}
	return fmt.Sprintf("PartitionKey eq '%s' and (%s)", pk, strings.Join(parts, " or "))
}

func (s *store) Insert(ctx context.Context, item *kv.Item) error {
	storedPK := pkToStore(item.PK)
	entityJSON, err := json.Marshal(Entity{PartitionKey: storedPK.PartitionKey, RowKey: storedPK.RowKey, Value: item.Value})
	if err != nil {
		return err
	}
	err = s.client.AddEntity(ctx, entityJSON)
	if err != nil {
		var azErr *client.AzureError
		if errors.As(err, &azErr) && azErr.StatusCode == 409 {
			return kv.ErrAlreadyExists
		}
		slog.ErrorContext(ctx, "insert failed", "table", s.options.Table, "pk", item.PK, "err", err)
		return err
	}
	return nil
}

func (s *store) Put(ctx context.Context, item *kv.Item) error {
	storedPK := pkToStore(item.PK)
	entityJSON, err := json.Marshal(Entity{PartitionKey: storedPK.PartitionKey, RowKey: storedPK.RowKey, Value: item.Value})
	if err != nil {
		return err
	}
	err = s.client.UpsertEntity(ctx, entityJSON, "Replace")
	if err != nil {
		slog.ErrorContext(ctx, "put failed", "table", s.options.Table, "pk", item.PK, "err", err)
	}
	return err
}

func (s *store) Remove(ctx context.Context, pk lexkey.PrimaryKey) error {
	storedPK := pkToStore(pk)
	err := s.client.DeleteEntity(ctx, storedPK.PartitionKey.ToHexString(), storedPK.RowKey.ToHexString())
	if err != nil {
		slog.ErrorContext(ctx, "delete failed", "table", s.options.Table, "pk", pk, "err", err)
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
			slog.ErrorContext(ctx, "range batch delete failed", "table", s.options.Table, "partition_key", rangeKey.PartitionKey.ToHexString(), "batch_size", len(batch), "err", err)
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
		storedPK := pkToStore(item.PK)
		switch item.Op {
		case kv.Put:
			raw, err := json.Marshal(Entity{
				PartitionKey: storedPK.PartitionKey,
				RowKey:       storedPK.RowKey,
				Value:        item.Value,
			})
			if err != nil {
				return fmt.Errorf("marshal failed: %w", err)
			}
			ops = append(ops, client.BatchOp{Type: client.BatchInsertReplace, Entity: raw})
		case kv.Delete:
			ops = append(ops, client.BatchOp{
				Type:         client.BatchDelete,
				PartitionKey: storedPK.PartitionKey.ToHexString(),
				RowKey:       storedPK.RowKey.ToHexString(),
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
	startRK := rowKeyToStore(args.StartRowKey)
	endRK := rowKeyToStore(args.EndRowKey)
	switch args.Operator {
	case kv.Scan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
	case kv.Equal:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", pk, escapeODataString(startRK.ToHexString()))), nil
	case kv.GreaterThan:
		startKey := escapeODataString(startRK.ToHexString())
		if len(args.StartRowKey) == 0 {
			return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
		}
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s'", pk, startKey)), nil
	case kv.GreaterThanOrEqual:
		startKey := escapeODataString(startRK.ToHexString())
		if len(args.StartRowKey) == 0 {
			return ptr(fmt.Sprintf("PartitionKey eq '%s'", pk)), nil
		}
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", pk, startKey)), nil
	case kv.LessThan:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey lt '%s'", pk, escapeODataString(endRK.ToHexString()))), nil
	case kv.LessThanOrEqual:
		return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", pk, escapeODataString(endRK.ToHexString()))), nil
	case kv.Between:
		startKey := escapeODataString(startRK.ToHexString())
		endKey := escapeODataString(endRK.ToHexString())
		if len(args.StartRowKey) == 0 {
			return ptr(fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", pk, endKey)), nil
		}
		if len(args.EndRowKey) == 0 {
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
