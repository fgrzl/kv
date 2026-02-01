// Package searchoverlay provides a generic, schema-agnostic search layer
// built on top of kv.KV and lexkey. It is designed to feel like "full text"
// search for consumers, while internally using simple, Azure Table–friendly
// indexes (value index + field+value index + field registry + payload table).
//
// # Design Philosophy
//
// This is NOT Elasticsearch. It is a KV-native search overlay optimized for:
//   - Projector-driven event sourcing (replay-safe, idempotent)
//   - Bounded write amplification (incremental updates via token registry)
//   - No cross-entity joins (IDs-only discovery, hydration is separate)
//   - Backend portability (Pebble, Redis, Azure Tables, etc.)
//
// # Architecture
//
// The overlay maintains five logical tables (all KV partitions):
//
// 1. Entity Payload: search:{name}:entity → entity payloads
//   - Opaque bytes returned with search hits
//   - Optional: can use IDs-only mode and hydrate from source
//
// 2. Token Registry: search:{name}:tokens:{entity_id} → {field: [tokens]}
//   - Tracks which tokens are indexed for each entity
//   - Enables O(changes) updates instead of O(entity-size)
//   - Critical for avoiding stale posting leaks
//
// 3. Value Index: search:{name}:value:{first_letter} → postings
//   - Key: (token, entity_id, field)
//   - Supports cross-field search
//
// 4. Field Index: search:{name}:field:{field}:{first_letter} → postings
//   - Key: (token, entity_id)
//   - Supports field-scoped search
//
// 5. Field Registry: search:{name}:fields → field names
//   - Tracks discovered fields for UI/UX
//   - Eventually consistent
//
// # Update Semantics
//
// Index() performs diff-based incremental updates:
//   - Load existing token registry (1 read per entity)
//   - Compute added/removed tokens via set diff
//   - Delete stale postings (prevents unbounded growth)
//   - Add new postings
//   - Update token registry atomically with postings
//
// Delete() uses token registry for complete cleanup:
//   - Removes all postings (value + field indexes)
//   - Deletes payload
//   - Deletes token registry
//   - No long-term index rot
//
// # Search Semantics
//
// Single-token exact match only:
//   - Search("golang admin") → matches token "golang" only
//   - Multi-token AND/OR is intentionally not supported
//   - Avoids joins, maintains KV-friendly semantics
//   - Complex queries: call Search() multiple times and intersect at app layer
//
// Search returns IDs + matched fields + payloads:
//   - Payload hydration is optional (can use IDs-only mode)
//   - Matched fields show where token was found
//   - Results are unordered (no scoring/ranking)
//
// # Replay Safety
//
// The overlay is designed for projector-driven indexing:
//   - Deterministic tokenization (case-insensitive, alphanumeric)
//   - Idempotent writes (re-indexing same entity is safe)
//   - Bounded batch operations per partition
//   - Token registry enables clean re-indexing without leaks
//
// Full index rebuild = replay all Index() calls from event log.
//
// # Future Extensions
//
// Possible without breaking changes:
//   - IDs-only search mode (skip payload hydration)
//   - Prefix/fuzzy matching (currently exact token only)
//   - Optional scoring/ranking (TF-IDF, BM25)
//   - Cursored pagination over large result sets
//   - Field-level tokenization strategies
//
// Intentionally NOT planned:
//   - Multi-token joins (use app-layer intersection)
//   - Cross-entity aggregations
//   - Graph traversal
//   - ACID transactions across entities
package searchoverlay

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"unicode"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/searchoverlay")

// Attribute is a generic field/value pair on an entity.
type Attribute struct {
	Field string
	Value string
}

// SearchEntity is the unit of indexing.
// ID must be unique within the overlay's namespace.
// Attributes are the fields you want to make searchable.
// Payload is an opaque blob returned to callers on search hits (e.g. JSON).
type SearchEntity struct {
	ID         string
	Attributes []Attribute
	Payload    []byte
}

// Query describes a search request.
// Text is the keyword users typed.
// Fields optionally scopes the search to specific fields.
// Limit bounds the number of hits returned (0 or negative = no explicit limit).
type Query struct {
	Text   string
	Fields []string
	Limit  int
}

// SearchHit is a single search result.
type SearchHit struct {
	ID            string
	MatchedFields []string
	Payload       []byte
}

// SearchOverlay is the public interface for the overlay.
type SearchOverlay interface {
	Index(ctx context.Context, e SearchEntity) error
	BatchIndex(ctx context.Context, entities []SearchEntity) error
	Delete(ctx context.Context, id string) error
	Search(ctx context.Context, q Query) ([]SearchHit, error)
	ListFields(ctx context.Context) ([]string, error)
	Close() error
}

// overlay is the default implementation of SearchOverlay.
type overlay struct {
	store kv.KV
	name  string // logical index/namespace name to allow multiple overlays on the same KV
	log   *slog.Logger
}

// New creates a new SearchOverlay instance.
func New(store kv.KV, name string, logger *slog.Logger) SearchOverlay {
	if logger == nil {
		logger = slog.Default()
	}
	return &overlay{
		store: store,
		name:  name,
		log:   logger,
	}
}

// internal posting stored in index tables.
type posting struct {
	EntityID string `json:"id"`
	Field    string `json:"field"`
}

// internal field registry entry.
type fieldEntry struct {
	Field string `json:"field"`
}

// internal token registry: tracks which tokens were indexed for an entity.
// Key is field name, value is list of tokens.
type tokenRegistry map[string][]string

// Index indexes or updates an entity's searchable attributes and payload.
// It performs a diff-based update to avoid leaking stale postings.
func (o *overlay) Index(ctx context.Context, e SearchEntity) error {
	ctx, span := tracer.Start(ctx, "searchoverlay.Index",
		trace.WithAttributes(
			attribute.String("index", o.name),
			attribute.String("entity_id", e.ID),
			attribute.Int("attr_count", len(e.Attributes)),
		),
	)
	defer span.End()

	if e.ID == "" {
		err := errors.New("searchoverlay: entity ID cannot be empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Load existing token registry to compute diff
	oldReg, err := o.loadTokenRegistry(ctx, e.ID)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: failed to load token registry",
			"index", o.name,
			"entity_id", e.ID,
			"err", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Normalize attributes into tokens per field.
	newReg := make(tokenRegistry) // field -> []tokens
	allFields := make(map[string]struct{})

	for _, attr := range e.Attributes {
		field := strings.TrimSpace(attr.Field)
		if field == "" {
			continue
		}
		allFields[field] = struct{}{}

		val := strings.ToLower(attr.Value)
		tokenSet := make(map[string]struct{})
		for _, tok := range tokenize(val) {
			if tok != "" {
				tokenSet[tok] = struct{}{}
			}
		}

		// Convert set to sorted slice for deterministic registry
		tokens := make([]string, 0, len(tokenSet))
		for tok := range tokenSet {
			tokens = append(tokens, tok)
		}
		if len(tokens) > 0 {
			sort.Strings(tokens)
			newReg[field] = tokens
		}
	}

	// Compute diff: what changed?
	addedTokens, removedTokens := tokenDiff(oldReg, newReg)

	// Build batches grouped by partition key.
	batchesByPartition := make(map[string][]*kv.BatchItem)

	// 1) Delete stale postings for removed tokens
	for field, tokens := range removedTokens {
		for _, tok := range tokens {
			firstLetter := partitionKeyForToken(tok)

			// Remove from value index
			valuePartKey := o.valuePartition(firstLetter)
			valuePK := lexkey.NewPrimaryKey(valuePartKey, lexkey.Encode(tok, e.ID, field))
			batchesByPartition[string(valuePartKey)] = append(batchesByPartition[string(valuePartKey)], &kv.BatchItem{
				Op: kv.Delete,
				PK: valuePK,
			})

			// Remove from field+value index
			fieldPartKey := o.fieldPartition(field, firstLetter)
			fieldPK := lexkey.NewPrimaryKey(fieldPartKey, lexkey.Encode(tok, e.ID))
			batchesByPartition[string(fieldPartKey)] = append(batchesByPartition[string(fieldPartKey)], &kv.BatchItem{
				Op: kv.Delete,
				PK: fieldPK,
			})
		}
	}

	// 2) Add new postings for added tokens
	for field, tokens := range addedTokens {
		for _, tok := range tokens {
			firstLetter := partitionKeyForToken(tok)

			// value index: search across all fields
			valPost := posting{
				EntityID: e.ID,
				Field:    field,
			}
			valBytes, err := json.Marshal(valPost)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			valuePartKey := o.valuePartition(firstLetter)
			valuePK := lexkey.NewPrimaryKey(valuePartKey, lexkey.Encode(tok, e.ID, field))
			batchesByPartition[string(valuePartKey)] = append(batchesByPartition[string(valuePartKey)], &kv.BatchItem{
				Op:    kv.Put,
				PK:    valuePK,
				Value: valBytes,
			})

			// field+value index: search within specific field
			fieldPost := posting{
				EntityID: e.ID,
				Field:    field,
			}
			fieldBytes, err := json.Marshal(fieldPost)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			fieldPartKey := o.fieldPartition(field, firstLetter)
			fieldPK := lexkey.NewPrimaryKey(fieldPartKey, lexkey.Encode(tok, e.ID))
			batchesByPartition[string(fieldPartKey)] = append(batchesByPartition[string(fieldPartKey)], &kv.BatchItem{
				Op:    kv.Put,
				PK:    fieldPK,
				Value: fieldBytes,
			})
		}
	}

	// 3) Update entity payload
	entityPartKey := o.entityPartition()
	entityPK := lexkey.NewPrimaryKey(entityPartKey, lexkey.Encode(e.ID))
	batchesByPartition[string(entityPartKey)] = append(batchesByPartition[string(entityPartKey)], &kv.BatchItem{
		Op:    kv.Put,
		PK:    entityPK,
		Value: e.Payload,
	})

	// 4) Update field registry entries
	fieldsPartKey := o.fieldsPartition()
	for field := range allFields {
		entry := fieldEntry{Field: field}
		raw, err := json.Marshal(entry)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		pk := lexkey.NewPrimaryKey(fieldsPartKey, lexkey.Encode(field))
		batchesByPartition[string(fieldsPartKey)] = append(batchesByPartition[string(fieldsPartKey)], &kv.BatchItem{
			Op:    kv.Put,
			PK:    pk,
			Value: raw,
		})
	}

	// 5) Update token registry
	if err := o.saveTokenRegistry(ctx, e.ID, newReg, batchesByPartition); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Execute each batch grouped by partition key
	for _, batch := range batchesByPartition {
		if err := o.store.Batch(ctx, batch); err != nil {
			o.log.ErrorContext(ctx, "searchoverlay: index batch failed",
				"index", o.name,
				"entity_id", e.ID,
				"err", err,
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	o.log.DebugContext(ctx, "searchoverlay: entity indexed",
		"index", o.name,
		"entity_id", e.ID,
		"field_count", len(allFields),
		"added_tokens", countTokens(addedTokens),
		"removed_tokens", countTokens(removedTokens),
	)
	return nil
}

// BatchIndex indexes multiple entities in a single efficient batching operation.
// This is more efficient than calling Index() multiple times when indexing large
// numbers of entities, as it amortizes the batch overhead across all entities.
// It performs diff-based updates to avoid leaking stale postings.
//
// Atomicity: BatchIndex is best-effort atomic per partition, not globally atomic.
// If a batch write fails for one partition, other partitions may have already been
// written. This is acceptable for a derived index that can be rebuilt from source.
func (o *overlay) BatchIndex(ctx context.Context, entities []SearchEntity) error {
	ctx, span := tracer.Start(ctx, "searchoverlay.BatchIndex",
		trace.WithAttributes(
			attribute.String("index", o.name),
			attribute.Int("entity_count", len(entities)),
		),
	)
	defer span.End()

	if len(entities) == 0 {
		return nil
	}

	// Load all existing token registries in batch
	entityIDs := make([]string, len(entities))
	for i, e := range entities {
		if e.ID == "" {
			err := errors.New("searchoverlay: entity ID cannot be empty")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		entityIDs[i] = e.ID
	}

	tokenPKs := make([]lexkey.PrimaryKey, len(entityIDs))
	tokenPartKey := o.tokenPartition()
	for i, id := range entityIDs {
		tokenPKs[i] = lexkey.NewPrimaryKey(tokenPartKey, lexkey.Encode(id))
	}

	tokenItems, err := o.store.GetBatch(ctx, tokenPKs...)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: batch load token registries failed",
			"index", o.name,
			"err", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Build map of existing registries
	oldRegistries := make(map[string]tokenRegistry)
	for i, item := range tokenItems {
		if item == nil {
			continue
		}
		var reg tokenRegistry
		if err := json.Unmarshal(item.Value, &reg); err != nil {
			continue
		}
		// Use entity ID from our request list
		oldRegistries[entityIDs[i]] = reg
	}

	// Accumulate all batch items grouped by partition key.
	batchesByPartition := make(map[string][]*kv.BatchItem)
	allFieldsAcrossEntities := make(map[string]struct{})

	for _, e := range entities {
		// Get old registry for this entity
		oldReg, exists := oldRegistries[e.ID]
		if !exists {
			oldReg = make(tokenRegistry)
		}

		// Normalize attributes into tokens per field for this entity.
		newReg := make(tokenRegistry) // field -> []tokens
		entityFields := make(map[string]struct{})

		for _, attr := range e.Attributes {
			field := strings.TrimSpace(attr.Field)
			if field == "" {
				continue
			}
			entityFields[field] = struct{}{}
			allFieldsAcrossEntities[field] = struct{}{}

			val := strings.ToLower(attr.Value)
			tokenSet := make(map[string]struct{})
			for _, tok := range tokenize(val) {
				if tok != "" {
					tokenSet[tok] = struct{}{}
				}
			}

			// Convert set to sorted slice for deterministic registry
			tokens := make([]string, 0, len(tokenSet))
			for tok := range tokenSet {
				tokens = append(tokens, tok)
			}
			if len(tokens) > 0 {
				sort.Strings(tokens)
				newReg[field] = tokens
			}
		}

		// Compute diff: what changed?
		addedTokens, removedTokens := tokenDiff(oldReg, newReg)

		// Delete stale postings for removed tokens
		for field, tokens := range removedTokens {
			for _, tok := range tokens {
				firstLetter := partitionKeyForToken(tok)

				// Remove from value index
				valuePartKey := o.valuePartition(firstLetter)
				valuePK := lexkey.NewPrimaryKey(valuePartKey, lexkey.Encode(tok, e.ID, field))
				batchesByPartition[string(valuePartKey)] = append(batchesByPartition[string(valuePartKey)], &kv.BatchItem{
					Op: kv.Delete,
					PK: valuePK,
				})

				// Remove from field+value index
				fieldPartKey := o.fieldPartition(field, firstLetter)
				fieldPK := lexkey.NewPrimaryKey(fieldPartKey, lexkey.Encode(tok, e.ID))
				batchesByPartition[string(fieldPartKey)] = append(batchesByPartition[string(fieldPartKey)], &kv.BatchItem{
					Op: kv.Delete,
					PK: fieldPK,
				})
			}
		}

		// Add new postings for added tokens
		for field, tokens := range addedTokens {
			for _, tok := range tokens {
				firstLetter := partitionKeyForToken(tok)

				// value index: search across all fields
				valPost := posting{
					EntityID: e.ID,
					Field:    field,
				}
				valBytes, err := json.Marshal(valPost)
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					return err
				}

				valuePartKey := o.valuePartition(firstLetter)
				valuePK := lexkey.NewPrimaryKey(valuePartKey, lexkey.Encode(tok, e.ID, field))
				batchesByPartition[string(valuePartKey)] = append(batchesByPartition[string(valuePartKey)], &kv.BatchItem{
					Op:    kv.Put,
					PK:    valuePK,
					Value: valBytes,
				})

				// field+value index: search within specific field
				fieldPost := posting{
					EntityID: e.ID,
					Field:    field,
				}
				fieldBytes, err := json.Marshal(fieldPost)
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					return err
				}

				fieldPartKey := o.fieldPartition(field, firstLetter)
				fieldPK := lexkey.NewPrimaryKey(fieldPartKey, lexkey.Encode(tok, e.ID))
				batchesByPartition[string(fieldPartKey)] = append(batchesByPartition[string(fieldPartKey)], &kv.BatchItem{
					Op:    kv.Put,
					PK:    fieldPK,
					Value: fieldBytes,
				})
			}
		}

		// Add entity payload
		entityPartKey := o.entityPartition()
		entityPK := lexkey.NewPrimaryKey(entityPartKey, lexkey.Encode(e.ID))
		batchesByPartition[string(entityPartKey)] = append(batchesByPartition[string(entityPartKey)], &kv.BatchItem{
			Op:    kv.Put,
			PK:    entityPK,
			Value: e.Payload,
		})

		// Add field registry entries for this entity
		fieldsPartKey := o.fieldsPartition()
		for field := range entityFields {
			entry := fieldEntry{Field: field}
			raw, err := json.Marshal(entry)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}
			pk := lexkey.NewPrimaryKey(fieldsPartKey, lexkey.Encode(field))
			batchesByPartition[string(fieldsPartKey)] = append(batchesByPartition[string(fieldsPartKey)], &kv.BatchItem{
				Op:    kv.Put,
				PK:    pk,
				Value: raw,
			})
		}

		// Update token registry
		if err := o.saveTokenRegistry(ctx, e.ID, newReg, batchesByPartition); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	// Execute each batch grouped by partition key
	for _, batch := range batchesByPartition {
		if err := o.store.Batch(ctx, batch); err != nil {
			o.log.ErrorContext(ctx, "searchoverlay: batch index failed",
				"index", o.name,
				"entity_count", len(entities),
				"err", err,
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	o.log.DebugContext(ctx, "searchoverlay: batch indexed",
		"index", o.name,
		"entity_count", len(entities),
		"field_count", len(allFieldsAcrossEntities),
	)
	return nil
}

// Delete removes an entity's payload and all associated index entries.
// This performs a complete cleanup using the token registry to ensure
// no stale postings remain in the index.
func (o *overlay) Delete(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "searchoverlay.Delete",
		trace.WithAttributes(
			attribute.String("index", o.name),
			attribute.String("entity_id", id),
		),
	)
	defer span.End()

	if id == "" {
		err := errors.New("searchoverlay: entity ID cannot be empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Load token registry to know what to delete
	oldReg, err := o.loadTokenRegistry(ctx, id)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: failed to load token registry for delete",
			"index", o.name,
			"entity_id", id,
			"err", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	batchesByPartition := make(map[string][]*kv.BatchItem)

	// Delete all postings for this entity
	for field, tokens := range oldReg {
		for _, tok := range tokens {
			firstLetter := partitionKeyForToken(tok)

			// Remove from value index
			valuePartKey := o.valuePartition(firstLetter)
			valuePK := lexkey.NewPrimaryKey(valuePartKey, lexkey.Encode(tok, id, field))
			batchesByPartition[string(valuePartKey)] = append(batchesByPartition[string(valuePartKey)], &kv.BatchItem{
				Op: kv.Delete,
				PK: valuePK,
			})

			// Remove from field+value index
			fieldPartKey := o.fieldPartition(field, firstLetter)
			fieldPK := lexkey.NewPrimaryKey(fieldPartKey, lexkey.Encode(tok, id))
			batchesByPartition[string(fieldPartKey)] = append(batchesByPartition[string(fieldPartKey)], &kv.BatchItem{
				Op: kv.Delete,
				PK: fieldPK,
			})
		}
	}

	// Delete entity payload
	entityPartKey := o.entityPartition()
	entityPK := lexkey.NewPrimaryKey(entityPartKey, lexkey.Encode(id))
	batchesByPartition[string(entityPartKey)] = append(batchesByPartition[string(entityPartKey)], &kv.BatchItem{
		Op: kv.Delete,
		PK: entityPK,
	})

	// Delete token registry
	o.deleteTokenRegistry(ctx, id, batchesByPartition)

	// Execute each batch grouped by partition key
	for _, batch := range batchesByPartition {
		if err := o.store.Batch(ctx, batch); err != nil {
			o.log.ErrorContext(ctx, "searchoverlay: delete batch failed",
				"index", o.name,
				"entity_id", id,
				"err", err,
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	o.log.DebugContext(ctx, "searchoverlay: entity fully deleted",
		"index", o.name,
		"entity_id", id,
		"postings_deleted", countTokens(oldReg),
	)
	return nil
}

// Search performs a keyword search over all fields or a subset of fields.
func (o *overlay) Search(ctx context.Context, q Query) ([]SearchHit, error) {
	ctx, span := tracer.Start(ctx, "searchoverlay.Search",
		trace.WithAttributes(
			attribute.String("index", o.name),
			attribute.String("text", q.Text),
			attribute.Int("field_count", len(q.Fields)),
			attribute.Int("limit", q.Limit),
		),
	)
	defer span.End()

	text := strings.ToLower(strings.TrimSpace(q.Text))
	if text == "" {
		return nil, nil
	}

	toks := tokenize(text)
	if len(toks) == 0 {
		return nil, nil
	}

	// Single-token search: We use the first token only.
	// Multi-token AND/OR queries are intentionally not implemented to avoid
	// join operations and maintain KV-friendly semantics. For complex queries,
	// use multiple Search calls and intersect/union results at the application layer.
	token := toks[0]
	firstLetter := partitionKeyForToken(token)

	limit := q.Limit
	if limit <= 0 {
		limit = 0 // let backend decide; we will still guard on hydration
	}

	// Collect postings from either value index or field+value index.
	postingsByEntity := make(map[string]map[string]struct{}) // entityID -> matched field set

	if len(q.Fields) == 0 {
		if err := o.searchValueIndex(ctx, token, firstLetter, limit, postingsByEntity); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	} else {
		// Normalize field list (dedupe, lower/trim)
		fieldSet := make(map[string]struct{})
		for _, f := range q.Fields {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			fieldSet[f] = struct{}{}
		}
		if len(fieldSet) == 0 {
			return nil, nil
		}
		if err := o.searchFieldIndexes(ctx, token, firstLetter, limit, fieldSet, postingsByEntity); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}

	if len(postingsByEntity) == 0 {
		return nil, nil
	}

	// Hydrate payloads in batch.
	entityIDs := make([]string, 0, len(postingsByEntity))
	for id := range postingsByEntity {
		entityIDs = append(entityIDs, id)
	}

	hits, err := o.hydrateHits(ctx, entityIDs, postingsByEntity)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Apply final limit if needed.
	if q.Limit > 0 && len(hits) > q.Limit {
		hits = hits[:q.Limit]
	}

	o.log.DebugContext(ctx, "searchoverlay: search completed",
		"index", o.name,
		"text", q.Text,
		"result_count", len(hits),
	)
	return hits, nil
}

// ListFields returns the set of known fields discovered via indexing.
func (o *overlay) ListFields(ctx context.Context) ([]string, error) {
	ctx, span := tracer.Start(ctx, "searchoverlay.ListFields",
		trace.WithAttributes(
			attribute.String("index", o.name),
		),
	)
	defer span.End()

	args := kv.QueryArgs{
		PartitionKey: o.fieldsPartition(),
		StartRowKey:  lexkey.Empty,
		EndRowKey:    lexkey.Empty,
		Operator:     kv.Scan,
	}

	items, err := o.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: list fields query failed",
			"index", o.name,
			"err", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	fieldSet := make(map[string]struct{})
	for _, it := range items {
		var fe fieldEntry
		if err := json.Unmarshal(it.Value, &fe); err != nil {
			o.log.WarnContext(ctx, "searchoverlay: skipping malformed field entry",
				"index", o.name,
				"err", err,
			)
			continue
		}
		if fe.Field != "" {
			fieldSet[fe.Field] = struct{}{}
		}
	}

	out := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		out = append(out, f)
	}

	o.log.DebugContext(ctx, "searchoverlay: list fields completed",
		"index", o.name,
		"field_count", len(out),
	)
	return out, nil
}

// Close closes the underlying KV store, if needed.
func (o *overlay) Close() error {
	return o.store.Close()
}

// --- internal helpers -------------------------------------------------------

func (o *overlay) entityPartition() lexkey.LexKey {
	return lexkey.Encode("search", o.name, "entity")
}

func (o *overlay) fieldsPartition() lexkey.LexKey {
	return lexkey.Encode("search", o.name, "fields")
}

func (o *overlay) tokenPartition() lexkey.LexKey {
	return lexkey.Encode("search", o.name, "tokens")
}

func (o *overlay) valuePartition(firstLetter string) lexkey.LexKey {
	return lexkey.Encode("search", o.name, "value", firstLetter)
}

func (o *overlay) fieldPartition(field, firstLetter string) lexkey.LexKey {
	return lexkey.Encode("search", o.name, "field", field, firstLetter)
}

// partitionKeyForToken picks a single-character bucket for a token,
// keeping partitions small and ATS-friendly.
func partitionKeyForToken(token string) string {
	if token == "" {
		return "_"
	}
	for _, r := range token {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return strings.ToLower(string(r))
		}
	}
	return "_"
}

// tokenize splits a string into alphanumeric tokens, lowercased.
func tokenize(s string) []string {
	var tokens []string
	var current []rune

	flush := func() {
		if len(current) == 0 {
			return
		}
		tokens = append(tokens, strings.ToLower(string(current)))
		current = current[:0]
	}

	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current = append(current, r)
		} else {
			flush()
		}
	}
	flush()

	return tokens
}

func (o *overlay) searchValueIndex(
	ctx context.Context,
	token string,
	firstLetter string,
	limit int,
	postingsByEntity map[string]map[string]struct{},
) error {
	args := kv.QueryArgs{
		PartitionKey: o.valuePartition(firstLetter),
		StartRowKey:  lexkey.Encode(token),
		// Use Encode(token+"\xFF") to match only keys starting with exact token
		// This ensures we don't match "python" when searching for "programming"
		EndRowKey: lexkey.Encode(token + string(rune(0xFF))),
		Operator:  kv.Between,
		Limit:     limit,
	}

	items, err := o.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: value index query failed",
			"index", o.name,
			"token", token,
			"err", err,
		)
		return err
	}

	for _, it := range items {
		var p posting
		if err := json.Unmarshal(it.Value, &p); err != nil {
			o.log.WarnContext(ctx, "searchoverlay: skipping malformed value posting",
				"index", o.name,
				"err", err,
			)
			continue
		}
		if p.EntityID == "" {
			continue
		}
		if postingsByEntity[p.EntityID] == nil {
			postingsByEntity[p.EntityID] = make(map[string]struct{})
		}
		if p.Field != "" {
			postingsByEntity[p.EntityID][p.Field] = struct{}{}
		}
	}

	return nil
}

func (o *overlay) searchFieldIndexes(
	ctx context.Context,
	token string,
	firstLetter string,
	limit int,
	fields map[string]struct{},
	postingsByEntity map[string]map[string]struct{},
) error {
	for field := range fields {
		args := kv.QueryArgs{
			PartitionKey: o.fieldPartition(field, firstLetter),
			StartRowKey:  lexkey.Encode(token),
			// Use Encode(token+"\xFF") to match only keys starting with exact token
			EndRowKey: lexkey.Encode(token + string(rune(0xFF))),
			Operator:  kv.Between,
			Limit:     limit,
		}

		items, err := o.store.Query(ctx, args, kv.Ascending)
		if err != nil {
			o.log.ErrorContext(ctx, "searchoverlay: field index query failed",
				"index", o.name,
				"field", field,
				"token", token,
				"err", err,
			)
			return err
		}

		for _, it := range items {
			var p posting
			if err := json.Unmarshal(it.Value, &p); err != nil {
				o.log.WarnContext(ctx, "searchoverlay: skipping malformed field posting",
					"index", o.name,
					"field", field,
					"err", err,
				)
				continue
			}
			if p.EntityID == "" {
				continue
			}
			if postingsByEntity[p.EntityID] == nil {
				postingsByEntity[p.EntityID] = make(map[string]struct{})
			}
			if p.Field != "" {
				postingsByEntity[p.EntityID][p.Field] = struct{}{}
			}
		}
	}
	return nil
}

func (o *overlay) hydrateHits(
	ctx context.Context,
	entityIDs []string,
	postingsByEntity map[string]map[string]struct{},
) ([]SearchHit, error) {
	// Build primary keys for batch get.
	pks := make([]lexkey.PrimaryKey, 0, len(entityIDs))
	for _, id := range entityIDs {
		pk := lexkey.NewPrimaryKey(
			o.entityPartition(),
			lexkey.Encode(id),
		)
		pks = append(pks, pk)
	}

	items, err := o.store.GetBatch(ctx, pks...)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: hydrate GetBatch failed",
			"index", o.name,
			"err", err,
		)
		return nil, err
	}

	// Build a map from encoded RowKey to payload.
	rowKeyToPayload := make(map[string][]byte, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		rowKeyToPayload[string(it.PK.RowKey)] = it.Value
	}

	hits := make([]SearchHit, 0, len(entityIDs))
	for _, id := range entityIDs {
		payload, ok := rowKeyToPayload[string(lexkey.Encode(id))]
		if !ok {
			// Entity payload has been deleted or missing; skip this hit.
			continue
		}
		fieldsSet := postingsByEntity[id]
		fields := make([]string, 0, len(fieldsSet))
		for f := range fieldsSet {
			fields = append(fields, f)
		}
		hits = append(hits, SearchHit{
			ID:            id,
			MatchedFields: fields,
			Payload:       payload,
		})
	}

	return hits, nil
}

// loadTokenRegistry retrieves the stored token registry for an entity.
func (o *overlay) loadTokenRegistry(ctx context.Context, entityID string) (tokenRegistry, error) {
	pk := lexkey.NewPrimaryKey(
		o.tokenPartition(),
		lexkey.Encode(entityID),
	)

	item, err := o.store.Get(ctx, pk)
	if err != nil {
		return nil, err
	}
	if item == nil {
		// Not found, return empty registry
		return make(tokenRegistry), nil
	}

	var reg tokenRegistry
	if err := json.Unmarshal(item.Value, &reg); err != nil {
		return nil, err
	}
	return reg, nil
}

// saveTokenRegistry stores the token registry for an entity.
func (o *overlay) saveTokenRegistry(ctx context.Context, entityID string, reg tokenRegistry, batch map[string][]*kv.BatchItem) error {
	raw, err := json.Marshal(reg)
	if err != nil {
		return err
	}

	tokenPartKey := o.tokenPartition()
	pk := lexkey.NewPrimaryKey(tokenPartKey, lexkey.Encode(entityID))
	batch[string(tokenPartKey)] = append(batch[string(tokenPartKey)], &kv.BatchItem{
		Op:    kv.Put,
		PK:    pk,
		Value: raw,
	})
	return nil
}

// deleteTokenRegistry removes the token registry for an entity.
func (o *overlay) deleteTokenRegistry(ctx context.Context, entityID string, batch map[string][]*kv.BatchItem) {
	tokenPartKey := o.tokenPartition()
	pk := lexkey.NewPrimaryKey(tokenPartKey, lexkey.Encode(entityID))
	batch[string(tokenPartKey)] = append(batch[string(tokenPartKey)], &kv.BatchItem{
		Op: kv.Delete,
		PK: pk,
	})
}

// tokenDiff computes added and removed tokens between old and new registries.
func tokenDiff(oldReg, newReg tokenRegistry) (added, removed tokenRegistry) {
	added = make(tokenRegistry)
	removed = make(tokenRegistry)

	// Find removed tokens: in old but not in new
	for field, oldTokens := range oldReg {
		newTokens := newReg[field]
		newSet := make(map[string]struct{})
		for _, t := range newTokens {
			newSet[t] = struct{}{}
		}

		for _, tok := range oldTokens {
			if _, exists := newSet[tok]; !exists {
				removed[field] = append(removed[field], tok)
			}
		}
	}

	// Find added tokens: in new but not in old
	for field, newTokens := range newReg {
		oldTokens := oldReg[field]
		oldSet := make(map[string]struct{})
		for _, t := range oldTokens {
			oldSet[t] = struct{}{}
		}

		for _, tok := range newTokens {
			if _, exists := oldSet[tok]; !exists {
				added[field] = append(added[field], tok)
			}
		}
	}

	return added, removed
}

// countTokens returns the total number of tokens across all fields in a registry.
func countTokens(reg tokenRegistry) int {
	count := 0
	for _, tokens := range reg {
		count += len(tokens)
	}
	return count
}
