package azure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
)

var _ kv.KV = (*store)(nil)

func NewSharedKeyCredential(accountName, accountKey string) (*aztables.SharedKeyCredential, error) {
	return aztables.NewSharedKeyCredential(accountName, accountKey)
}

// NewAzureStore initializes a new AzureTableProvider.
func NewAzureStore(options *TableProviderOptions) (kv.KV, error) {

	// Create a new table client
	client, err := getClient(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create table client: %w", err)
	}

	store := &store{
		options: options,
		client:  client,
	}

	return store, store.createTableIfNotExists()
}

func (s *store) createTableIfNotExists() error {
	_, err := s.client.CreateTable(context.Background(), nil)
	if err == nil {
		return nil // Table created successfully
	}

	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) && responseErr.ErrorCode == string(aztables.TableAlreadyExists) {
		return nil // Table already exists, no further action needed
	}

	return fmt.Errorf("failed to create table: %w", err)
}

type Entity struct {
	PartitionKey kv.EncodedKey `json:"PartitionKey"`
	RowKey       kv.EncodedKey `json:"RowKey"`
	Value        []byte        `json:"Value"`
}

// TableProviderOptions holds configuration options for Azure Storage Tables.
type TableProviderOptions struct {
	Prefix                    string
	Table                     string
	Endpoint                  string
	UseDefaultAzureCredential bool
	SharedKeyCredential       *aztables.SharedKeyCredential
}

type store struct {
	options  *TableProviderOptions
	client   *aztables.Client
	disposed sync.Once
}

// Get retrieves an item from Azure Table Storage using the primary key.
// Returns nil and no error if the item is not found.
func (s *store) Get(pk kv.PrimaryKey) (*kv.Item, error) {
	partition, row := pk.ToHexStrings()
	ctx := context.Background()

	// Attempt to retrieve entity from Azure Table Storage
	resp, err := s.client.GetEntity(ctx, partition, row, nil)
	if err != nil {
		// Handle not found case
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get entity: %w", err)
	}

	// Parse response into entity
	var entity Entity
	if err := json.Unmarshal(resp.Value, &entity); err != nil {
		return nil, fmt.Errorf("failed to decode entity: %w", err)
	}

	// Convert to kv.Item
	return &kv.Item{
		PK: kv.PrimaryKey{
			PartitionKey: entity.PartitionKey,
			RowKey:       entity.RowKey,
		},
		Value: entity.Value,
	}, nil
}

// GetBatch retrieves multiple items concurrently from Azure Table Storage.
func (s *store) GetBatch(keys ...kv.PrimaryKey) ([]*kv.Item, error) {
	if len(keys) == 0 {
		return []*kv.Item{}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	items := make([]*kv.Item, 0, len(keys))
	var firstErr error

	for _, key := range keys {
		wg.Add(1)

		go func(key kv.PrimaryKey) {
			defer wg.Done()

			item, err := s.Get(key)
			if err != nil {
				// Capture the first error encountered
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to retrieve item for key %v: %w", key, err)
				}
				mu.Unlock()
				return
			}

			// Add the item to the results slice if found
			if item != nil {
				mu.Lock()
				items = append(items, item)
				mu.Unlock()
			}
		}(key)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return items, nil
}

// Put inserts or replaces an item in the Azure Table Storage.
func (s *store) Put(item *kv.Item) error {

	entity := &Entity{
		PartitionKey: item.PK.PartitionKey,
		RowKey:       item.PK.RowKey,
		Value:        item.Value,
	}

	// Marshal the EDMEntity to JSON
	entityJSON, err := json.Marshal(entity)
	if err != nil {
		return fmt.Errorf("failed to marshal entity to JSON: %w", err)
	}

	// Use the UpsertEntity method to insert or replace the entity
	_, err = s.client.UpsertEntity(context.Background(), entityJSON, &aztables.UpsertEntityOptions{UpdateMode: aztables.UpdateModeReplace})
	if err != nil {
		return fmt.Errorf("failed to upsert entity: %w", err)
	}

	return nil
}

// Remove deletes an item from Azure Table Storage using the primary key.
// Returns nil if the item is not found.
func (s *store) Remove(pk kv.PrimaryKey) error {
	partition, row := pk.ToHexStrings()
	_, err := s.client.DeleteEntity(context.Background(), partition, row, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil // Not found is not an error
		}
		return fmt.Errorf("failed to delete entity (PartitionKey: %s, RowKey: %s): %w", partition, row, err)
	}
	return nil
}

// RemoveBatch implements kv.KV.
func (s *store) RemoveBatch(keys ...kv.PrimaryKey) error {
	if len(keys) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, key := range keys {
		wg.Add(1)

		go func(key kv.PrimaryKey) {
			defer wg.Done()

			err := s.Remove(key)
			if err != nil {
				// Capture the first error encountered
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to retrieve item for key %v: %w", key, err)
				}
				mu.Unlock()
				return
			}
		}(key)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	return nil
}

// RemoveRange implements kv.KV.
func (s *store) RemoveRange(rangeKey kv.RangeKey) error {
	panic("implement me")
}

// Query implements kv.KV.
func (s *store) Query(queryArgs kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {

	// table storage is always sorted by PartitionKey and RowKey in ascending order

	enumerator := s.Enumerate(queryArgs)
	slice, err := enumerators.ToSlice(enumerator)
	if err != nil {
		return nil, err
	}

	if sort == kv.Descending {
		// only reverse if we want descending order
		kv.ReverseItems(slice)
	}

	return slice, nil
}

// Enumerate returns an Enumerator that yields items matching the query arguments.
// It stops after yielding args.Limit items, or when all matching items are retrieved or an error occurs.
func (s *store) Enumerate(args kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	ctx := context.Background()

	// Shortcut for Equal operator
	if args.Operator == kv.Equal {
		pk := kv.PrimaryKey{PartitionKey: args.PartitionKey, RowKey: args.StartRowKey}
		item, err := s.Get(pk)
		if err != nil {
			return enumerators.Error[*kv.Item](err)
		}
		if item == nil {
			return enumerators.Empty[*kv.Item]()
		}
		return enumerators.Slice([]*kv.Item{item})
	}

	limit := normalizeLimit(args.Limit)
	filter, err := buildFilter(args)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}

	pager := s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Top:    limit, // Per-page limit
		Filter: filter,
	})

	var queue *kv.Queue[*kv.Item]
	var totalCount int // Track total items returned

	return enumerators.Generate(func() (*kv.Item, bool, error) {
		for {
			// Stop if we've reached the total limit
			if args.Limit > 0 && totalCount >= args.Limit {
				return nil, false, nil
			}

			// Check for cancellation (even though ctx is Background, future-proofing)
			if ctx.Err() != nil {
				return nil, false, ctx.Err()
			}

			// Return items from queue if available
			if queue != nil && !queue.IsEmpty() {
				item, _ := queue.Dequeue()
				totalCount++
				return item, true, nil
			}

			// Check if more pages are available
			if !pager.More() {
				return nil, false, nil
			}

			// Fetch next page
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("failed to fetch page: %w", err)
			}

			// Skip empty responses
			if len(resp.Entities) == 0 {
				continue
			}

			// Initialize or reset queue
			if queue == nil {
				// Adjust queue size based on remaining limit
				remaining := args.Limit - totalCount
				if args.Limit > 0 && remaining < len(resp.Entities) {
					queue = kv.NewQueue[*kv.Item](remaining)
				} else {
					queue = kv.NewQueue[*kv.Item](len(resp.Entities))
				}
			} else {
				queue.Reset()
			}

			// Enqueue items up to the limit
			for _, entityBytes := range resp.Entities {
				if args.Limit > 0 && totalCount >= args.Limit {
					break // Stop enqueueing if limit is reached
				}
				var entity Entity
				if err := json.Unmarshal(entityBytes, &entity); err != nil {
					return nil, false, fmt.Errorf("failed to decode entity: %w", err)
				}
				queue.Enqueue(&kv.Item{
					PK: kv.PrimaryKey{
						PartitionKey: entity.PartitionKey,
						RowKey:       entity.RowKey,
					},
					Value: entity.Value,
				})
			}
		}
	})
}

// Batch performs a transactional batch of operations on Azure Table Storage.
// All items must share the same PartitionKey, and the batch size must not exceed 100.
func (s *store) Batch(items []*kv.BatchItem) error {
	ctx := context.Background()
	if len(items) == 0 {
		return nil
	}

	if len(items) > 100 {
		return fmt.Errorf("batch size exceeds Azure Table Storage limit of 100 operations")
	}

	// Check that all items share the same PartitionKey
	if len(items) > 1 {
		pk := items[0].PK.PartitionKey
		for i, item := range items[1:] {
			if !bytes.Equal(item.PK.PartitionKey, pk) {
				return fmt.Errorf("all items in batch must share the same PartitionKey; mismatch at index %d", i+1)
			}
		}
	}

	operations := make([]aztables.TransactionAction, 0, len(items))
	for i, item := range items {
		var entity Entity
		var actionType aztables.TransactionType

		switch item.Op {
		case kv.Delete:
			entity = Entity{
				PartitionKey: item.PK.PartitionKey,
				RowKey:       item.PK.RowKey,
			}
			actionType = aztables.TransactionTypeDelete
		case kv.Put:
			entity = Entity{
				PartitionKey: item.PK.PartitionKey,
				RowKey:       item.PK.RowKey,
				Value:        item.Value,
			}
			actionType = aztables.TransactionTypeInsertReplace
		default:
			return fmt.Errorf("unsupported batch operation %v at index %d", item.Op, i)
		}

		entityJSON, err := json.Marshal(entity)
		if err != nil {
			return fmt.Errorf("failed to marshal entity at index %d: %w", i, err)
		}

		operations = append(operations, aztables.TransactionAction{
			ActionType: actionType,
			Entity:     entityJSON,
		})
	}

	// Submit the transaction
	_, err := s.client.SubmitTransaction(ctx, operations, nil)
	if err != nil {
		// Handle EOF with 202 Accepted as a special case with azurite
		if err.Error() == "unexpected EOF" {
			return nil
		}
		return fmt.Errorf("batch transaction failed: %w", err)
	}
	return nil
}

func (s *store) BatchChunks(items enumerators.Enumerator[*kv.BatchItem], chunkSize int) error {
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
				// Close batch immediately on error
				return fmt.Errorf("failed to retrieve item in chunk: %w", err)
			}
			batch = append(batch, item)
		}

		// Commit the batch and close it immediately after
		if err := s.Batch(batch); err != nil {
			return err
		}
	}

	return nil
}

// Close releases resources (not much to do here for Azure Table Storage).
func (s *store) Close() error {
	var closeErr error
	s.disposed.Do(func() {
		// No explicit close operation required for Azure Table client
	})
	return closeErr
}

// getClient retrieves or creates a new Azure Table client.
func getClient(options *TableProviderOptions) (*aztables.Client, error) {
	prefixedTableName := fmt.Sprintf("%s-%s", options.Prefix, options.Table)
	prefixedTableName = sanitizeTableName(prefixedTableName)

	url := fmt.Sprintf("%s/%s", options.Endpoint, prefixedTableName)

	var clientOptions = aztables.ClientOptions{}
	var client *aztables.Client
	if options.UseDefaultAzureCredential {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		client, _ = aztables.NewClient(url, cred, &clientOptions)
	} else if options.SharedKeyCredential != nil {

		if options.SharedKeyCredential.AccountName() == "devstoreaccount1" {
			clientOptions.InsecureAllowCredentialWithHTTP = true
		}

		client, _ = aztables.NewClientWithSharedKey(url, options.SharedKeyCredential, &clientOptions)
	} else {
		return nil, fmt.Errorf("please provide a valid Azure credential")
	}
	return client, nil
}

func sanitizeTableName(name string) string {
	if len(name) == 0 {
		return "T00" // Default name if empty
	}

	var sanitized []byte

	// Ensure the first character is a letter
	firstChar := name[0]
	if isLetter(firstChar) {
		sanitized = append(sanitized, firstChar)
	} else {
		sanitized = append(sanitized, 'T') // Prefix with 'T' if not a letter
	}

	// Only keep alphanumeric characters
	for i := 1; i < len(name); i++ {
		char := name[i]
		if isAlphanumeric(char) {
			sanitized = append(sanitized, char)
		}
	}

	// Ensure the name is at least 3 characters long
	for len(sanitized) < 3 {
		sanitized = append(sanitized, '0') // Pad with zeros if too short
	}

	// Truncate if it's longer than 63 characters
	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
	}

	return string(sanitized)
}

func isLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isAlphanumeric(c byte) bool {
	return isLetter(c) || (c >= '0' && c <= '9')
}

func normalizeLimit(limit int) *int32 {
	normalized := int32(100)
	if limit > 0 && limit <= 100 {
		normalized = int32(limit)
	}
	return &normalized
}

func buildFilter(args kv.QueryArgs) (*string, error) {

	var filter string

	switch args.Operator {
	case kv.Scan:
		return nil, nil // No filter applied

	case kv.Equal:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", args.PartitionKey.ToHexString(), args.StartRowKey.ToHexString())

	case kv.GreaterThan:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s'", args.PartitionKey.ToHexString(), args.StartRowKey.ToHexString())

	case kv.GreaterThanOrEqual:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", args.PartitionKey.ToHexString(), args.StartRowKey.ToHexString())

	case kv.LessThan:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey lt '%s'", args.PartitionKey.ToHexString(), args.EndRowKey.ToHexString())

	case kv.LessThanOrEqual:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", args.PartitionKey.ToHexString(), args.EndRowKey.ToHexString())

	case kv.Between:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s' and RowKey le '%s'", args.PartitionKey.ToHexString(), args.StartRowKey.ToHexString(), args.EndRowKey.ToHexString())

	case kv.StartsWith:
		return nil, fmt.Errorf("StartsWith operator is not supported natively; consider client-side filtering")

	default:
		return nil, fmt.Errorf("unsupported query operator: %v", args.Operator)
	}

	return &filter, nil
}
