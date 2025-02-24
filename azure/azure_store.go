package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
)

func NewSharedKeyCredential(accountName, accountKey string) (*aztables.SharedKeyCredential, error) {
	return aztables.NewSharedKeyCredential(accountName, accountKey)
}

// NewTableProvider initializes a new AzureTableProvider.
func NewTableProvider(options *TableProviderOptions) (kv.KV, error) {

	tableName := toKebabCase(options.Prefix + "-" + options.Table)

	// Create a default Azure credential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	// Create a new service client
	serviceClient, err := aztables.NewServiceClient(options.Endpoint, cred, nil)
	if err != nil {
		return nil, err
	}

	// Create a new table client
	tableClient := serviceClient.NewClient(tableName)

	store := &store{
		options: options,
		client:  tableClient,
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
func (s *store) Get(primaryKey kv.PrimaryKey) (*kv.Item, error) {
	// Convert PartitionKey and RowKey to UTF-8 strings
	partitionKey := string(primaryKey.PartitionKey)
	rowKey := string(primaryKey.RowKey)

	// Retrieve the entity from Azure Table Storage
	resp, err := s.client.GetEntity(context.Background(), partitionKey, rowKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve entity: %w", err)
	}

	// Unmarshal the response into an Entity struct
	var entity Entity
	if err := json.Unmarshal(resp.Value, &entity); err != nil {
		return nil, fmt.Errorf("failed to unmarshal entity from response: %w", err)
	}

	// Convert Entity back into kv.Item
	item := &kv.Item{
		PK: kv.PrimaryKey{
			PartitionKey: entity.PartitionKey,
			RowKey:       entity.RowKey,
		},
		Value: entity.Value,
	}

	return item, nil
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
	_, err = s.client.UpsertEntity(context.Background(), entityJSON, nil)
	if err != nil {
		return fmt.Errorf("failed to upsert entity: %w", err)
	}

	return nil
}

func (s *store) Remove(primaryKey kv.PrimaryKey) error {
	// Convert PartitionKey and RowKey to UTF-8 strings
	partitionKey := string(primaryKey.PartitionKey)
	rowKey := string(primaryKey.RowKey)

	// Call the DeleteEntity method
	_, err := s.client.DeleteEntity(context.Background(), partitionKey, rowKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete entity (PartitionKey: %s, RowKey: %s): %w", partitionKey, rowKey, err)
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

}

// Query implements kv.KV.
func (s *store) Query(queryArgs kv.QueryArgs, sort kv.SortDirection) ([]*kv.Item, error) {
	enumerator := s.Enumerate(queryArgs)
	slice, err := enumerators.ToSlice(enumerator)
	if err != nil {
		return nil, err
	}

	// Determine the order in which items were enumerated.
	// For LessThan and LessThanOrEqual queries, we iterate in descending order;
	// otherwise, we iterate in ascending order.
	isAscendingEnumerated := true
	switch queryArgs.Operator {
	case kv.LessThan, kv.LessThanOrEqual:
		isAscendingEnumerated = false
	default:
		isAscendingEnumerated = true
	}

	// If the enumerated order doesn't match the requested sort order, simply reverse the slice.
	switch sort {
	case kv.Ascending:
		if !isAscendingEnumerated {
			kv.ReverseItems(slice)
		}
	case kv.Descending:
		if isAscendingEnumerated {
			kv.ReverseItems(slice)
		}
	}

	return slice, nil
}

// Enumerate implements kv.KV.
func (s *store) Enumerate(queryArgs kv.QueryArgs) enumerators.Enumerator[*kv.Item] {
	limit := normalizeLimit(queryArgs.Limit)

	filter, err := buildFilter(queryArgs)
	if err != nil {
		return enumerators.Error[*kv.Item](err)
	}

	// Set up ListEntitiesOptions with continuation token
	options := &aztables.ListEntitiesOptions{
		Top:    limit,
		Filter: filter,
	}
	// Create pager for listing entities
	pager := s.client.NewListEntitiesPager(options)

	// Initialize the queue
	var queue *Queue[*kv.Item]
	generator := enumerators.Generate[*kv.Item](func() (*kv.Item, bool, error) {
		for {
			if queue != nil {
				if item, ok := queue.Dequeue(); ok {
					return item, true, nil
				}
			}

			// Retrieve next page
			if pager.More() {
				resp, err := pager.NextPage(context.Background())
				if err != nil {
					return nil, false, fmt.Errorf("failed to retrieve entities: %w", err)
				}

				// Initialize queue with capacity based on response size
				if queue == nil {
					queue = NewQueue[*kv.Item](len(resp.Entities))
				} else {
					queue.Reset()
				}

				// Convert response entities to kv.Item and enqueue them
				for _, entity := range resp.Entities {
					e := &Entity{}
					if err := json.Unmarshal(entity, e); err != nil {
						return nil, false, fmt.Errorf("failed to unmarshal entity: %w", err)
					}

					item := &kv.Item{
						PK: kv.PrimaryKey{
							PartitionKey: e.PartitionKey,
							RowKey:       e.RowKey,
						},
						Value: e.Value,
					}
					queue.Enqueue(item)
				}
			}

			// No more data to process
			return nil, false, nil
		}
	})

	return generator
}

// Batch implements kv.KV.
func (s *store) Batch(items []*kv.BatchItem) error {

	if len(items) == 0 {
		return nil // No actions to process
	}

	// Create a slice of TransactionOperations
	var operations []aztables.TransactionAction

	for _, item := range items {
		var op aztables.TransactionAction

		// Define operation based on action type
		switch item.Op {
		case kv.Delete:
			entity := &Entity{
				PartitionKey: item.PK.PartitionKey,
				RowKey:       item.PK.RowKey,
			}
			entityJSON, err := json.Marshal(entity)
			if err != nil {
				return fmt.Errorf("failed to marshal entity to JSON: %w", err)
			}
			op = aztables.TransactionAction{
				ActionType: aztables.TransactionTypeDelete,
				Entity:     entityJSON,
			}
		case kv.Put:
			entity := &Entity{
				PartitionKey: item.PK.PartitionKey,
				RowKey:       item.PK.RowKey,
				Value:        item.Value,
			}
			entityJSON, err := json.Marshal(entity)
			if err != nil {
				return fmt.Errorf("failed to marshal entity to JSON: %w", err)
			}
			op = aztables.TransactionAction{
				ActionType: aztables.TransactionTypeInsertReplace,
				Entity:     entityJSON,
			}
		}

		operations = append(operations, op)
	}

	// Submit the transaction to Azure Table Storage
	_, err := s.client.SubmitTransaction(context.Background(), operations, nil)
	if err != nil {
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

// Optimized Queue with preallocation and minimal allocations
type Queue[T any] struct {
	items []T
	head  int
}

// NewQueue initializes a queue with a preallocated capacity.
func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{items: make([]T, 0, capacity)}
}

// Enqueue adds an item efficiently.
func (q *Queue[T]) Enqueue(item T) {
	q.items = append(q.items, item)
}

// Dequeue without causing unnecessary allocations.
func (q *Queue[T]) Dequeue() (T, bool) {
	var zero T
	if q.IsEmpty() {
		return zero, false
	}
	item := q.items[q.head]
	q.head++

	// Only shrink if 75% of items have been dequeued
	if q.head > len(q.items)*3/4 {
		copy(q.items, q.items[q.head:]) // Avoids unnecessary allocations
		q.items = q.items[:len(q.items)-q.head]
		q.head = 0
	}

	return item, true
}

// IsEmpty checks if the queue has no items left.
func (q *Queue[T]) IsEmpty() bool {
	return q.head >= len(q.items)
}

// Length returns the number of remaining items.
func (q *Queue[T]) Length() int {
	return len(q.items) - q.head
}

// Reset clears the queue and reuses the underlying capacity.
func (q *Queue[T]) Reset() {
	q.items = q.items[:0] // Reset slice length to 0
	q.head = 0            // Reset head index
}

// ToKebabCase converts a string to kebab-case.
func toKebabCase(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", "-"))
}

func normalizeLimit(limit int) *int32 {
	normalized := int32(100)
	if limit > 0 && limit <= 100 {
		normalized = int32(limit)
	}
	return &normalized
}

func buildFilter(queryArgs kv.QueryArgs) (*string, error) {
	partitionKey := string(queryArgs.PartitionKey)
	startRowKey := string(queryArgs.StartRowKey)
	endRowKey := string(queryArgs.EndRowKey)

	var filter string

	switch queryArgs.Operator {
	case kv.Scan:
		return nil, nil // No filter applied

	case kv.Equal:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", partitionKey, startRowKey)

	case kv.GreaterThan:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey gt '%s'", partitionKey, startRowKey)

	case kv.GreaterThanOrEqual:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s'", partitionKey, startRowKey)

	case kv.LessThan:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey lt '%s'", partitionKey, endRowKey)

	case kv.LessThanOrEqual:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey le '%s'", partitionKey, endRowKey)

	case kv.Between:
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey ge '%s' and RowKey le '%s'", partitionKey, startRowKey, endRowKey)

	case kv.StartsWith:
		return nil, fmt.Errorf("StartsWith operator is not supported natively; consider client-side filtering")

	default:
		return nil, fmt.Errorf("unsupported query operator: %v", queryArgs.Operator)
	}

	return &filter, nil
}
