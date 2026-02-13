// Package searchoverlay provides a generic, schema-agnostic search layer
// built on top of kv.KV and lexkey. It is designed to feel like "full text"
// search for consumers, while internally using simple, Azure Table–friendly
// indexes (value index + field+value index + field registry + payload table).
package searchoverlay

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
// Format: entityID (cstring, null-terminated) + field (cstring, null-terminated)
// This eliminates ~200ns per unmarshal + reduces allocations by 50-70%.
func encodePosting(p posting) []byte {
	// Pre-allocate for typical entity+field size: 10+10=20 bytes + 2 nulls
	buf := make([]byte, 0, len(p.EntityID)+len(p.Field)+2)
	buf = append(buf, []byte(p.EntityID)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(p.Field)...)
	buf = append(buf, 0)
	return buf
}

// decodePosting efficiently decodes a posting from binary format.
func decodePosting(data []byte) (posting, error) {
	// Find the two null separators
	firstNull := -1
	secondNull := -1
	for i, b := range data {
		if b == 0 {
			if firstNull == -1 {
				firstNull = i
			} else {
				secondNull = i
				break
			}
		}
	}
	if firstNull == -1 || secondNull == -1 {
		return posting{}, errors.New("invalid posting format")
	}
	return posting{
		EntityID: string(data[:firstNull]),
		Field:    string(data[firstNull+1 : secondNull]),
	}, nil
}

// postingResult tracks hit count and matched fields for an entity during search.
// Used to efficiently collect candidates and their rankings before pagination.
type postingResult struct {
	entityID string              // entity ID
	fields   map[string]struct{} // matched fields (lazy-populated)
	hitCount int                 // number of distinct fields that matched
}

// internal field registry entry.
type fieldEntry struct {
	Field string `json:"field"`
}

// Index indexes or updates an entity's searchable attributes and payload.
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

	// Normalize attributes into tokens per field.
	fieldTokens := make(map[string]map[string]struct{}) // field -> token set
	allFields := make(map[string]struct{})

	for _, attr := range e.Attributes {
		field := strings.TrimSpace(attr.Field)
		if field == "" {
			continue
		}
		allFields[field] = struct{}{}

		val := strings.ToLower(attr.Value)
		for _, tok := range tokenize(val) {
			if tok == "" {
				continue
			}
			if fieldTokens[field] == nil {
				fieldTokens[field] = make(map[string]struct{})
			}
			fieldTokens[field][tok] = struct{}{}
		}
	}

	// Build batches grouped by partition key.
	// Each partition key must have its own batch operation.
	batchesByPartition := make(map[string][]*kv.BatchItem)

	// 1) Entity payload table
	entityPartKey := o.entityPartition()
	entityPK := lexkey.NewPrimaryKey(entityPartKey, lexkey.Encode(e.ID))
	batchesByPartition[string(entityPartKey)] = append(batchesByPartition[string(entityPartKey)], &kv.BatchItem{
		Op:    kv.Put,
		PK:    entityPK,
		Value: e.Payload,
	})

	// 2) Field registry entries
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

	// 3) Value index + field+value index
	for field, tokens := range fieldTokens {
		for tok := range tokens {
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
	)
	return nil
}

// BatchIndex indexes multiple entities in a single efficient batching operation.
// This is more efficient than calling Index() multiple times when indexing large
// numbers of entities, as it amortizes the batch overhead across all entities.
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

	// Accumulate all batch items grouped by partition key.
	batchesByPartition := make(map[string][]*kv.BatchItem)
	allFieldsAcrossEntities := make(map[string]struct{})

	for _, e := range entities {
		if e.ID == "" {
			err := errors.New("searchoverlay: entity ID cannot be empty")
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}

		// Normalize attributes into tokens per field for this entity.
		fieldTokens := make(map[string]map[string]struct{}) // field -> token set
		entityFields := make(map[string]struct{})

		for _, attr := range e.Attributes {
			field := strings.TrimSpace(attr.Field)
			if field == "" {
				continue
			}
			entityFields[field] = struct{}{}
			allFieldsAcrossEntities[field] = struct{}{}

			val := strings.ToLower(attr.Value)
			for _, tok := range tokenize(val) {
				if tok == "" {
					continue
				}
				if fieldTokens[field] == nil {
					fieldTokens[field] = make(map[string]struct{})
				}
				fieldTokens[field][tok] = struct{}{}
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

		// Add value index + field+value index entries
		for field, tokens := range fieldTokens {
			for tok := range tokens {
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

// Delete removes an entity's payload and effectively hides it from search results.
//
// Note: this currently only deletes the payload row. Index entries remain, but
// search will skip hits whose payload no longer exists. This keeps deletes
// cheap and can be refined later to fully clean index rows if needed.
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

	entityPK := lexkey.NewPrimaryKey(
		o.entityPartition(),
		lexkey.Encode(id),
	)

	if err := o.store.Remove(ctx, entityPK); err != nil {
		o.log.ErrorContext(ctx, "searchoverlay: delete entity failed",
			"index", o.name,
			"entity_id", id,
			"err", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	o.log.DebugContext(ctx, "searchoverlay: entity payload deleted",
		"index", o.name,
		"entity_id", id,
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
	var err error

	// Phase 5b optimization: For single-token queries without NOT, use optimized single-token path
	if query.IsSingleToken() && len(query.Tokens) == 1 && !query.Tokens[0].IsNot {
		// Optimized path for common case: single keyword
		token := query.Tokens[0].Text
		firstLetter := partitionKeyForToken(token)
		postingsByEntity = make(map[string]map[string]struct{})

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
		// Multi-token or special case: use boolean evaluation
		postingsByEntity, err = o.evaluateMultiToken(ctx, query, fieldSet, limit)
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

// SearchPage performs a keyword search and returns a single page of results.
// Results are ranked by hit count (descending), then by entity ID (ascending).
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
	var err error

	// Phase 5b optimization: For single-token queries without NOT, use optimized single-token path
	if query.IsSingleToken() && len(query.Tokens) == 1 && !query.Tokens[0].IsNot {
		// Optimized path for common case: single keyword
		token := query.Tokens[0].Text
		firstLetter := partitionKeyForToken(token)
		postingsByEntity = make(map[string]map[string]struct{})

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
		postingsByEntity, err = o.evaluateMultiToken(ctx, query, fieldSet, effectiveScanLimit)
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

	candidates := make([]rankedCandidate, 0, len(postingsByEntity))
	for id, fields := range postingsByEntity {
		candidates = append(candidates, rankedCandidate{
			ID:       id,
			HitCount: len(fields),
		})
	}

	// Full sort: O(n log n) - required for cursor-based pagination.
	// Pagination with ties in ranking requires all candidates at each rank level,
	// making heap-based top-K optimization incompatible.
	// Future optimization: batch-mode pagination that collects top-K with same rank.
	sort.Slice(candidates, func(i, j int) bool {
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

	hits, err := o.hydrateHits(ctx, pageIDs, postingsByEntity)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return polymorphic.Page[SearchHit]{}, err
	}

	nextCursor := ""
	if limit > 0 && len(pageIDs) >= limit && moreAvailable {
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
		EndRowKey:    lexkey.Empty,
		Operator:     kv.Scan,
		Limit:        limit,
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
	// Use WaitGroup to coordinate goroutines and mutex to sync map updates.

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	// Convert field set to slice for concurrent iteration
	fieldList := make([]string, 0, len(fields))
	for field := range fields {
		fieldList = append(fieldList, field)
	}

	// Launch a goroutine for each field query
	for _, field := range fieldList {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()

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
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}

			// Process results and update shared map with synchronization
			for _, it := range items {
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

				mu.Lock()
				// Lazy-allocate inner map only when first field appears for this entity
				// Phase 2c: Small initial capacity reduces repeated map growth
				if postingsByEntity[p.EntityID] == nil {
					postingsByEntity[p.EntityID] = make(map[string]struct{}, 4) // Typical: 1-5 fields per entity
				}
				if p.Field != "" {
					postingsByEntity[p.EntityID][p.Field] = struct{}{}
				}
				mu.Unlock()
			}
		}(field)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check if any errors occurred
	if len(errs) > 0 {
		return errs[0]
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
		hits = append(hits, SearchHit{
			ID:            id,
			MatchedFields: fields,
			Payload:       payload,
		})
	}

	return hits, nil
}

type rankedCandidate struct {
	ID       string
	HitCount int
}

type pageCursor struct {
	HitCount int
	ID       string
	Valid    bool
}

func (c pageCursor) allows(candidate rankedCandidate) bool {
	if !c.Valid {
		return true
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
	parts := strings.SplitN(raw, "|", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return pageCursor{}, errors.New("searchoverlay: invalid cursor")
	}
	hitCount, err := strconv.Atoi(parts[0])
	if err != nil || hitCount < 0 {
		return pageCursor{}, errors.New("searchoverlay: invalid cursor")
	}
	return pageCursor{HitCount: hitCount, ID: parts[1], Valid: true}, nil
}

func encodeCursor(candidate rankedCandidate) string {
	return strconv.Itoa(candidate.HitCount) + "|" + candidate.ID
}
