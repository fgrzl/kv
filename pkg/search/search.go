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
//   - Results are ranked by score (approximate BM25)
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/fgrzl/json/polymorphic"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/searchoverlay")

// defaultScanLimit is the maximum number of results to scan from a single index query
// when no explicit limit is provided. This prevents pathological cases where a popular
// token matches millions of entities, consuming excessive memory and CPU during sorting.
const defaultScanLimit = 10000

// computeEffectiveScanLimit determines the backend scan limit for index queries.
// Phase 4 optimization: Use dynamic limits based on page size to reduce backend work
// while maintaining pagination correctness. Pagination needs a buffer of tied-rank
// candidates, so we scale the limit appropriately.
func computeEffectiveScanLimit(pageLimit int) int {
	if pageLimit <= 0 {
		// No page limit means scan all (capped at default max)
		return defaultScanLimit
	}

	// Phase 4 strategy: Scale scan limit based on page limit
	// - Small limit (1-10): scan 10x for local ranking + pagination buffer
	// - Medium limit (11-100): scan 5x for ranking buffer
	// - Large limit (100+): scan 2x for rank-equals buffer
	// This reduces backend work significantly while ensuring enough candidates for pagination.

	switch {
	case pageLimit <= 10:
		// Small result sets: need generous buffer for tied-rank pagination
		scanLimit := pageLimit * 10
		if scanLimit > 1000 {
			scanLimit = 1000
		}
		return scanLimit
	case pageLimit <= 100:
		// Medium sets: moderate buffer
		scanLimit := pageLimit * 5
		if scanLimit > 5000 {
			scanLimit = 5000
		}
		return scanLimit
	default:
		// Large limit: minimal buffer (just for ties)
		scanLimit := pageLimit * 2
		if scanLimit > defaultScanLimit {
			scanLimit = defaultScanLimit
		}
		return scanLimit
	}
}

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

// PageQuery describes a paged search request.
// Cursor is an opaque position token returned from a prior SearchPage call.
type PageQuery struct {
	Text   string
	Fields []string
	Limit  int
	Cursor string
}

// SearchHit is a single search result.
type SearchHit struct {
	ID            string
	MatchedFields []string
	Payload       []byte
	Score         float64
}

// GetDiscriminator implements polymorphic.Polymorphic.
func (SearchHit) GetDiscriminator() string {
	return "mesh://search/hit"
}

// SearchOverlay is the public interface for the overlay.
type SearchOverlay interface {
	Index(ctx context.Context, e SearchEntity) error
	BatchIndex(ctx context.Context, entities []SearchEntity) error
	Delete(ctx context.Context, id string) error
	Search(ctx context.Context, q Query) ([]SearchHit, error)
	SearchPage(ctx context.Context, q PageQuery) (polymorphic.Page[SearchHit], error)
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

// encodePosting efficiently encodes a posting as binary data without JSON overhead.
// Format: varint(len(entityID)) + entityID + varint(len(field)) + field
// Varint length-prefixed encoding avoids null-byte vulnerability while maintaining efficiency.
// This eliminates ~200ns per unmarshal + reduces allocations by 50-70%.
func encodePosting(p posting) []byte {
	// Pre-allocate for typical entity+field size: varint headers + content
	buf := make([]byte, 0, len(p.EntityID)+len(p.Field)+10)

	// Encode entityID length + content
	buf = binary.AppendVarint(buf, int64(len(p.EntityID)))
	buf = append(buf, []byte(p.EntityID)...)

	// Encode field length + content
	buf = binary.AppendVarint(buf, int64(len(p.Field)))
	buf = append(buf, []byte(p.Field)...)

	return buf
}

// decodePosting efficiently decodes a posting from binary format.
func decodePosting(data []byte) (posting, error) {
	if len(data) == 0 {
		return posting{}, errors.New("invalid posting format: empty data")
	}

	var offset int

	// Decode entityID length
	entityIDLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return posting{}, errors.New("invalid posting format: cannot read entityID length")
	}
	if entityIDLen < 0 {
		return posting{}, errors.New("invalid posting format: negative entityID length")
	}
	offset += n

	// Extract entityID
	if offset+int(entityIDLen) > len(data) {
		return posting{}, errors.New("invalid posting format: truncated entityID")
	}
	entityID := string(data[offset : offset+int(entityIDLen)])
	offset += int(entityIDLen)

	// Decode field length
	fieldLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return posting{}, errors.New("invalid posting format: cannot read field length")
	}
	if fieldLen < 0 {
		return posting{}, errors.New("invalid posting format: negative field length")
	}
	offset += n

	// Extract field
	if offset+int(fieldLen) > len(data) {
		return posting{}, errors.New("invalid posting format: truncated field")
	}
	field := string(data[offset : offset+int(fieldLen)])

	return posting{
		EntityID: entityID,
		Field:    field,
	}, nil
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
			valBytes := encodePosting(valPost)

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
			fieldBytes := encodePosting(fieldPost)

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
	if err := o.saveTokenRegistry(e.ID, newReg, batchesByPartition); err != nil {
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

	// Build map of existing registries (results are in same order as tokenPKs / entityIDs)
	oldRegistries := make(map[string]tokenRegistry)
	for i, res := range tokenItems {
		if !res.Found || res.Item == nil {
			continue
		}
		var reg tokenRegistry
		if err := json.Unmarshal(res.Item.Value, &reg); err != nil {
			continue
		}
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
				valBytes := encodePosting(valPost)

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
				fieldBytes := encodePosting(fieldPost)

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
		if err := o.saveTokenRegistry(e.ID, newReg, batchesByPartition); err != nil {
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
	o.deleteTokenRegistry(id, batchesByPartition)

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

	// Phase 5b: Parse query for multi-token support (boolean operators, NOT, etc.)
	query := ParseQuery(text)
	if len(query.Tokens) == 0 {
		return nil, nil
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 0 // let backend decide; we will still guard on hydration
	}

	// Normalize field list (dedupe, lower/trim)
	fieldSet := make(map[string]struct{})
	for _, f := range q.Fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		fieldSet[f] = struct{}{}
	}

	// Collect postings from either value index or field+value index.
	var postingsByEntity map[string]map[string]struct{}
	var tokenResults map[string]map[string]map[string]struct{}
	var err error

	// Phase 5b optimization: For single-token queries without NOT, use optimized single-token path
	// However, if the single token contains spaces (from a quoted phrase like "golang programming"),
	// treat it as implicit AND and use the multi-token path
	isSingleWordToken := query.IsSingleToken() && len(query.Tokens) == 1 && !query.Tokens[0].IsNot
	tokenHasSpaces := isSingleWordToken && strings.Contains(query.Tokens[0].Text, " ")

	if isSingleWordToken && !tokenHasSpaces {
		// Optimized path for common case: single keyword without spaces
		token := query.Tokens[0].Text
		firstLetter := partitionKeyForToken(token)
		postingsByEntity = make(map[string]map[string]struct{})
		tokenResults = map[string]map[string]map[string]struct{}{
			token: postingsByEntity,
		}

		if len(fieldSet) == 0 {
			if err = o.searchValueIndex(ctx, token, firstLetter, limit, postingsByEntity); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		} else {
			if err = o.searchFieldIndexes(ctx, token, firstLetter, limit, fieldSet, postingsByEntity); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		}
	} else {
		// Multi-token, quoted phrase with spaces, or special case: use boolean evaluation
		postingsByEntity, tokenResults, err = o.evaluateMultiToken(ctx, query, fieldSet, limit)
		if err != nil {
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

	scoresByEntity, err := o.scoreCandidates(ctx, query, postingsByEntity, tokenResults)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	hits, err := o.hydrateHits(ctx, entityIDs, postingsByEntity, scoresByEntity)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	hitCounts := make(map[string]int, len(postingsByEntity))
	for id, fields := range postingsByEntity {
		hitCounts[id] = len(fields)
	}
	sortHitsByScore(hits, hitCounts)

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

// SearchPage performs a keyword search and returns a single page of results.
// Results are ranked by score (descending), then by hit count and entity ID.
func (o *overlay) SearchPage(ctx context.Context, q PageQuery) (polymorphic.Page[SearchHit], error) {
	ctx, span := tracer.Start(ctx, "searchoverlay.SearchPage",
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
		return polymorphic.Page[SearchHit]{}, nil
	}

	// Phase 5b: Parse query for multi-token support (boolean operators, NOT, etc.)
	query := ParseQuery(text)
	if len(query.Tokens) == 0 {
		return polymorphic.Page[SearchHit]{}, nil
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 0
	}

	// Phase 4 optimization: Apply dynamic backend scan limits based on page size.
	// This reduces unnecessary scanning while maintaining ranking correctness.
	effectiveScanLimit := computeEffectiveScanLimit(q.Limit)

	// Normalize field list (dedupe, lower/trim)
	fieldSet := make(map[string]struct{})
	for _, f := range q.Fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		fieldSet[f] = struct{}{}
	}

	// Collect postings from either value index or field+value index.
	var postingsByEntity map[string]map[string]struct{}
	var tokenResults map[string]map[string]map[string]struct{}
	var err error

	// Phase 5b optimization: For single-token queries without NOT, use optimized single-token path
	if query.IsSingleToken() && len(query.Tokens) == 1 && !query.Tokens[0].IsNot {
		// Optimized path for common case: single keyword
		token := query.Tokens[0].Text
		firstLetter := partitionKeyForToken(token)
		postingsByEntity = make(map[string]map[string]struct{})
		tokenResults = map[string]map[string]map[string]struct{}{
			token: postingsByEntity,
		}

		if len(fieldSet) == 0 {
			if err = o.searchValueIndex(ctx, token, firstLetter, effectiveScanLimit, postingsByEntity); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return polymorphic.Page[SearchHit]{}, err
			}
		} else {
			if err = o.searchFieldIndexes(ctx, token, firstLetter, effectiveScanLimit, fieldSet, postingsByEntity); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return polymorphic.Page[SearchHit]{}, err
			}
		}
	} else {
		// Multi-token or special case: use boolean evaluation
		postingsByEntity, tokenResults, err = o.evaluateMultiToken(ctx, query, fieldSet, effectiveScanLimit)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return polymorphic.Page[SearchHit]{}, err
		}
	}

	if len(postingsByEntity) == 0 {
		return polymorphic.Page[SearchHit]{}, nil
	}

	cur, err := parseCursor(q.Cursor)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return polymorphic.Page[SearchHit]{}, err
	}

	scoresByEntity, err := o.scoreCandidates(ctx, query, postingsByEntity, tokenResults)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return polymorphic.Page[SearchHit]{}, err
	}

	candidates := make([]rankedCandidate, 0, len(postingsByEntity))
	for id, fields := range postingsByEntity {
		candidates = append(candidates, rankedCandidate{
			ID:       id,
			HitCount: len(fields),
			Score:    scoresByEntity[id],
		})
	}

	// Full sort: O(n log n) - required for cursor-based pagination.
	// Pagination with ties in ranking requires all candidates at each rank level,
	// making heap-based top-K optimization incompatible.
	// Future optimization: batch-mode pagination that collects top-K with same rank.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].HitCount != candidates[j].HitCount {
			return candidates[i].HitCount > candidates[j].HitCount
		}
		return candidates[i].ID < candidates[j].ID
	})

	pageIDs := make([]string, 0, len(candidates))
	var lastCandidate rankedCandidate
	moreAvailable := false
	for i, c := range candidates {
		if !cur.allows(c) {
			continue
		}
		pageIDs = append(pageIDs, c.ID)
		lastCandidate = c
		if limit > 0 && len(pageIDs) >= limit {
			for j := i + 1; j < len(candidates); j++ {
				if cur.allows(candidates[j]) {
					moreAvailable = true
					break
				}
			}
			break
		}
	}

	if len(pageIDs) == 0 {
		return polymorphic.Page[SearchHit]{}, nil
	}

	hits, err := o.hydrateHits(ctx, pageIDs, postingsByEntity, scoresByEntity)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return polymorphic.Page[SearchHit]{}, err
	}

	nextCursor := ""
	// BUG FIX: Check len(hits) instead of len(pageIDs) because hydrateHits may filter out
	// deleted entities. Using len(pageIDs) caused incomplete pages when candidates were deleted.
	if limit > 0 && len(hits) >= limit && moreAvailable {
		nextCursor = encodeCursor(lastCandidate)
	}

	o.log.DebugContext(ctx, "searchoverlay: search page completed",
		"index", o.name,
		"text", q.Text,
		"result_count", len(hits),
	)
	return polymorphic.Page[SearchHit]{Models: hits, Next: nextCursor}, nil
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

	// Phase 2c optimization: Pre-allocate expected capacity to reduce map growth
	estimatedEntityCount := len(items) / 3 // Heuristic: average 3 postings per entity
	if estimatedEntityCount < 10 {
		estimatedEntityCount = 10
	}

	for _, it := range items {
		p, err := decodePosting(it.Value)
		if err != nil {
			o.log.WarnContext(ctx, "searchoverlay: skipping malformed value posting",
				"index", o.name,
				"err", err,
			)
			continue
		}
		if p.EntityID == "" {
			continue
		}
		// Lazy-allocate inner map only when first field appears for this entity
		if postingsByEntity[p.EntityID] == nil {
			postingsByEntity[p.EntityID] = make(map[string]struct{}, 4) // Small initial capacity for typical case
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
	// Phase 3 optimization: Parallelize field queries for independent partitions.
	// Each field has its own index partition, so queries can run concurrently.
	// Phase 3b: Batch per-goroutine results to reduce lock contention.
	// Each goroutine builds its own result map, then we merge all maps once with a single lock.

	var wg sync.WaitGroup
	var mu sync.Mutex

	// Convert field set to slice for concurrent iteration
	fieldList := make([]string, 0, len(fields))
	for field := range fields {
		fieldList = append(fieldList, field)
	}

	// Channel to collect per-field results to avoid lock contention
	type fieldResults struct {
		results map[string]map[string]struct{}
		err     error
	}
	resultsChan := make(chan fieldResults, len(fieldList))

	// Launch a goroutine for each field query
	for _, field := range fieldList {
		// Check if context already cancelled before launching goroutines
		select {
		case <-ctx.Done():
			wg.Wait() // Wait for any already-launched goroutines
			return ctx.Err()
		default:
		}

		wg.Add(1)
		go func(f string) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				resultsChan <- fieldResults{err: ctx.Err()}
				return
			default:
			}

			args := kv.QueryArgs{
				PartitionKey: o.fieldPartition(f, firstLetter),
				StartRowKey:  lexkey.Encode(token),
				EndRowKey:    lexkey.Empty,
				Operator:     kv.Scan,
				Limit:        limit,
			}

			items, err := o.store.Query(ctx, args, kv.Ascending)
			if err != nil {
				o.log.ErrorContext(ctx, "searchoverlay: field index query failed",
					"index", o.name,
					"field", f,
					"token", token,
					"err", err,
				)
				resultsChan <- fieldResults{err: err}
				return
			}

			// Build per-field result map WITHOUT lock (much faster)
			entityFieldMap := make(map[string]map[string]struct{})
			for _, it := range items {
				// Check context cancellation periodically for large result sets
				select {
				case <-ctx.Done():
					resultsChan <- fieldResults{err: ctx.Err()}
					return
				default:
				}

				p, err := decodePosting(it.Value)
				if err != nil {
					o.log.WarnContext(ctx, "searchoverlay: skipping malformed field posting",
						"index", o.name,
						"field", f,
						"err", err,
					)
					continue
				}
				if p.EntityID == "" {
					continue
				}

				// Lazy-allocate inner map only when first field appears for this entity
				if entityFieldMap[p.EntityID] == nil {
					entityFieldMap[p.EntityID] = make(map[string]struct{}, 4) // Typical: 1-5 fields per entity
				}
				if p.Field != "" {
					entityFieldMap[p.EntityID][p.Field] = struct{}{}
				}
			}

			resultsChan <- fieldResults{results: entityFieldMap}
		}(field)
	}

	// Wait for all goroutines to complete in background
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results and merge with single lock
	var firstErr error
	for fr := range resultsChan {
		if fr.err != nil {
			if firstErr == nil {
				firstErr = fr.err
			}
			continue
		}

		// Merge field results into shared map with lock (brief lock duration)
		mu.Lock()
		for entityID, fieldSet := range fr.results {
			if postingsByEntity[entityID] == nil {
				postingsByEntity[entityID] = make(map[string]struct{}, len(fieldSet))
			}
			for field := range fieldSet {
				postingsByEntity[entityID][field] = struct{}{}
			}
		}
		mu.Unlock()
	}

	// Return first error if any occurred
	return firstErr
}

func (o *overlay) hydrateHits(
	ctx context.Context,
	entityIDs []string,
	postingsByEntity map[string]map[string]struct{},
	scoresByEntity map[string]float64,
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
	for _, res := range items {
		if !res.Found || res.Item == nil {
			continue
		}
		rowKeyToPayload[string(res.Item.PK.RowKey)] = res.Item.Value
	}

	hits := make([]SearchHit, 0, len(entityIDs))
	for _, id := range entityIDs {
		payload, ok := rowKeyToPayload[string(lexkey.Encode(id))]
		if !ok {
			// Entity payload has been deleted or missing; skip this hit.
			continue
		}
		fieldsSet := postingsByEntity[id]
		// Pre-allocate field slice with reasonable capacity (most entities match few fields).
		// This reduces allocation overhead for common cases (1-5 matched fields per entity).
		fieldCapacity := len(fieldsSet)
		if fieldCapacity < 5 {
			fieldCapacity = 5 // Reserve for typical case
		}
		fields := make([]string, 0, fieldCapacity)
		for f := range fieldsSet {
			fields = append(fields, f)
		}
		score := 0.0
		if scoresByEntity != nil {
			score = scoresByEntity[id]
		}
		hits = append(hits, SearchHit{
			ID:            id,
			MatchedFields: fields,
			Payload:       payload,
			Score:         score,
		})
	}

	return hits, nil
}

type rankedCandidate struct {
	ID       string
	HitCount int
	Score    float64
}

type pageCursor struct {
	Score    float64
	HitCount int
	ID       string
	Valid    bool
}

func (c pageCursor) allows(candidate rankedCandidate) bool {
	if !c.Valid {
		return true
	}
	if candidate.Score < c.Score {
		return true
	}
	if candidate.Score > c.Score {
		return false
	}
	if candidate.HitCount < c.HitCount {
		return true
	}
	if candidate.HitCount > c.HitCount {
		return false
	}
	return candidate.ID > c.ID
}

func parseCursor(raw string) (pageCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return pageCursor{}, nil
	}
	// BUG FIX: Use "||" delimiter instead of "|" to avoid conflicts with entity IDs containing pipes
	// Example: entity ID "item|variant" would be incorrectly split with single pipe delimiter
	parts := strings.Split(raw, "||")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return pageCursor{}, errors.New("searchoverlay: invalid cursor")
	}
	score, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return pageCursor{}, errors.New("searchoverlay: invalid cursor")
	}
	hitCount, err := strconv.Atoi(parts[1])
	if err != nil || hitCount < 0 {
		return pageCursor{}, errors.New("searchoverlay: invalid cursor")
	}
	return pageCursor{Score: score, HitCount: hitCount, ID: parts[2], Valid: true}, nil
}

func encodeCursor(candidate rankedCandidate) string {
	// BUG FIX: Use "||" delimiter instead of "|" to avoid conflicts with entity IDs
	return strconv.FormatFloat(candidate.Score, 'g', -1, 64) + "||" + strconv.Itoa(candidate.HitCount) + "||" + candidate.ID
}

func (o *overlay) scoreCandidates(
	ctx context.Context,
	query QueryExpr,
	postingsByEntity map[string]map[string]struct{},
	tokenResults map[string]map[string]map[string]struct{},
) (map[string]float64, error) {
	entityIDs := make([]string, 0, len(postingsByEntity))
	for id := range postingsByEntity {
		entityIDs = append(entityIDs, id)
	}

	scores := make(map[string]float64, len(entityIDs))
	positiveTokens := positiveQueryTokens(query)
	if len(entityIDs) == 0 || len(positiveTokens) == 0 {
		return scores, nil
	}

	docCount, totalDocLen, err := o.computeCorpusStats(ctx)
	if err != nil {
		return nil, err
	}
	if docCount == 0 {
		return scores, nil
	}
	avgDocLen := float64(totalDocLen) / float64(docCount)
	if avgDocLen <= 0 {
		return scores, nil
	}

	docLengths, err := o.loadDocLengths(ctx, entityIDs)
	if err != nil {
		return nil, err
	}

	dfByToken := make(map[string]int, len(positiveTokens))
	for _, token := range positiveTokens {
		dfByToken[token] = len(tokenResults[token])
	}

	for _, id := range entityIDs {
		docLen := float64(docLengths[id])
		if docLen <= 0 {
			docLen = 1
		}
		score := 0.0
		for _, token := range positiveTokens {
			tokenMap := tokenResults[token]
			if tokenMap == nil {
				continue
			}
			entityFields := tokenMap[id]
			if entityFields == nil {
				continue
			}
			tf := float64(len(entityFields))
			if tf <= 0 {
				continue
			}
			df := float64(dfByToken[token])
			if df <= 0 {
				continue
			}
			score += bm25Score(tf, df, float64(docCount), docLen, avgDocLen)
		}
		scores[id] = score
	}

	return scores, nil
}

func (o *overlay) computeCorpusStats(ctx context.Context) (docCount int, totalDocLen int, err error) {
	args := kv.QueryArgs{
		PartitionKey: o.tokenPartition(),
		StartRowKey:  lexkey.Empty,
		EndRowKey:    lexkey.Empty,
		Operator:     kv.Scan,
	}

	items, err := o.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: token registry scan failed",
			"index", o.name,
			"err", err,
		)
		return 0, 0, err
	}

	for _, it := range items {
		var reg tokenRegistry
		if err := json.Unmarshal(it.Value, &reg); err != nil {
			o.log.WarnContext(ctx, "searchoverlay: skipping malformed token registry",
				"index", o.name,
				"err", err,
			)
			continue
		}
		docCount++
		totalDocLen += countTokens(reg)
	}

	return docCount, totalDocLen, nil
}

func (o *overlay) loadDocLengths(ctx context.Context, entityIDs []string) (map[string]int, error) {
	pks := make([]lexkey.PrimaryKey, 0, len(entityIDs))
	for _, id := range entityIDs {
		pk := lexkey.NewPrimaryKey(
			o.tokenPartition(),
			lexkey.Encode(id),
		)
		pks = append(pks, pk)
	}

	items, err := o.store.GetBatch(ctx, pks...)
	if err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: token registry GetBatch failed",
			"index", o.name,
			"err", err,
		)
		return nil, err
	}

	lengths := make(map[string]int, len(entityIDs))
	for i, res := range items {
		id := entityIDs[i]
		if !res.Found || res.Item == nil {
			lengths[id] = 0
			continue
		}
		var reg tokenRegistry
		if err := json.Unmarshal(res.Item.Value, &reg); err != nil {
			lengths[id] = 0
			continue
		}
		lengths[id] = countTokens(reg)
	}

	return lengths, nil
}

func positiveQueryTokens(query QueryExpr) []string {
	set := make(map[string]struct{})
	for _, tok := range query.Tokens {
		if tok.IsNot {
			continue
		}
		if tok.Text == "" {
			continue
		}
		set[tok.Text] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for token := range set {
		out = append(out, token)
	}
	sort.Strings(out)
	return out
}

func bm25Score(tf, df, totalDocs, docLen, avgDocLen float64) float64 {
	if df <= 0 || totalDocs <= 0 || avgDocLen <= 0 {
		return 0
	}

	const (
		k1 = 1.2
		b  = 0.75
	)

	idf := math.Log(1 + (totalDocs-df+0.5)/(df+0.5))
	normalization := tf + k1*(1-b+b*(docLen/avgDocLen))
	if normalization == 0 {
		return 0
	}
	return idf * ((tf * (k1 + 1)) / normalization)
}

func sortHitsByScore(hits []SearchHit, hitCounts map[string]int) {
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		if hitCounts[hits[i].ID] != hitCounts[hits[j].ID] {
			return hitCounts[hits[i].ID] > hitCounts[hits[j].ID]
		}
		return hits[i].ID < hits[j].ID
	})
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
func (o *overlay) saveTokenRegistry(entityID string, reg tokenRegistry, batch map[string][]*kv.BatchItem) error {
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
func (o *overlay) deleteTokenRegistry(entityID string, batch map[string][]*kv.BatchItem) {
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
