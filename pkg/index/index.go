package index

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/index")

// storedPosting represents a single document in a term's posting list.
type storedPosting struct {
	DocID string `json:"doc_id"`
	Meta  []byte `json:"meta"`
}

// storedDocument represents indexed terms for a document.
type storedDocument struct {
	ID    string            `json:"id"`
	Terms map[string][]byte `json:"terms"`
}

// indexStore is the default implementation of InvertedIndex using kv.KV storage.
type indexStore struct {
	store kv.KV
	name  string
}

// New creates a new InvertedIndex instance using the provided kv store and
// an optional namespace name to partition keys.
func New(store kv.KV, name string) InvertedIndex {
	return &indexStore{store: store, name: name}
}

func (i *indexStore) termPartition(term string) lexkey.LexKey {
	return lexkey.Encode("index", i.name, "term", term)
}

func (i *indexStore) documentPartition() lexkey.LexKey {
	return lexkey.Encode("index", i.name, "document")
}

func (i *indexStore) metaPartition() lexkey.LexKey {
	return lexkey.Encode("index", i.name, "meta")
}

// Index adds or updates a document in the inverted index.
func (i *indexStore) Index(ctx context.Context, doc Document) error {
	ctx, span := tracer.Start(ctx, "index.Index",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.String("doc_id", doc.ID),
			attribute.Int("term_count", len(doc.Terms)),
		))
	defer span.End()

	if doc.ID == "" {
		err := errors.New("doc_id cannot be empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, "doc_id cannot be empty")
		return err
	}

	// Store the document metadata
	sd := storedDocument{
		ID:    doc.ID,
		Terms: make(map[string][]byte),
	}
	for term, meta := range doc.Terms {
		sd.Terms[term] = meta
	}
	docBytes, err := json.Marshal(sd)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal document", "index", i.name, "doc_id", doc.ID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	docPK := lexkey.NewPrimaryKey(i.documentPartition(), lexkey.Encode(doc.ID))
	err = i.store.Put(ctx, &kv.Item{PK: docPK, Value: docBytes})
	if err != nil {
		slog.ErrorContext(ctx, "failed to store document", "index", i.name, "doc_id", doc.ID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Add document to each term's posting list
	for term, meta := range doc.Terms {
		posting := storedPosting{DocID: doc.ID, Meta: meta}
		postingBytes, err := json.Marshal(posting)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal posting", "index", i.name, "doc_id", doc.ID, "term", term, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}

		termPK := lexkey.NewPrimaryKey(i.termPartition(term), lexkey.Encode(doc.ID))
		err = i.store.Put(ctx, &kv.Item{PK: termPK, Value: postingBytes})
		if err != nil {
			slog.ErrorContext(ctx, "failed to store posting", "index", i.name, "doc_id", doc.ID, "term", term, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	slog.DebugContext(ctx, "document indexed", "index", i.name, "doc_id", doc.ID, "term_count", len(doc.Terms))
	return nil
}

// BatchIndex adds or updates multiple documents in a single efficient batching operation.
func (i *indexStore) BatchIndex(ctx context.Context, docs []Document) error {
	ctx, span := tracer.Start(ctx, "index.BatchIndex",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.Int("doc_count", len(docs)),
		))
	defer span.End()

	if len(docs) == 0 {
		return nil
	}

	// Accumulate all batch items grouped by partition key
	batchesByPartition := make(map[string][]*kv.BatchItem)

	for _, doc := range docs {
		if doc.ID == "" {
			err := errors.New("doc_id cannot be empty")
			span.RecordError(err)
			span.SetStatus(codes.Error, "doc_id cannot be empty")
			return err
		}

		// Store the document metadata
		sd := storedDocument{
			ID:    doc.ID,
			Terms: make(map[string][]byte),
		}
		for term, meta := range doc.Terms {
			sd.Terms[term] = meta
		}
		docBytes, err := json.Marshal(sd)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal document", "index", i.name, "doc_id", doc.ID, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}

		// Add document to document partition batch
		docPartKey := i.documentPartition()
		docPK := lexkey.NewPrimaryKey(docPartKey, lexkey.Encode(doc.ID))
		batchesByPartition[string(docPartKey)] = append(batchesByPartition[string(docPartKey)], &kv.BatchItem{
			Op:    kv.Put,
			PK:    docPK,
			Value: docBytes,
		})

		// Add document to each term's posting list
		for term, meta := range doc.Terms {
			posting := storedPosting{DocID: doc.ID, Meta: meta}
			postingBytes, err := json.Marshal(posting)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal posting", "index", i.name, "doc_id", doc.ID, "term", term, "err", err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			termPartKey := i.termPartition(term)
			termPK := lexkey.NewPrimaryKey(termPartKey, lexkey.Encode(doc.ID))
			batchesByPartition[string(termPartKey)] = append(batchesByPartition[string(termPartKey)], &kv.BatchItem{
				Op:    kv.Put,
				PK:    termPK,
				Value: postingBytes,
			})
		}
	}

	// Execute each batch grouped by partition key
	for _, batch := range batchesByPartition {
		if err := i.store.Batch(ctx, batch); err != nil {
			slog.ErrorContext(ctx, "failed to batch index documents", "index", i.name, "doc_count", len(docs), "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	slog.DebugContext(ctx, "documents batch indexed", "index", i.name, "doc_count", len(docs))
	return nil
}

// Delete removes a document from the inverted index.
func (i *indexStore) Delete(ctx context.Context, docID string) error {
	ctx, span := tracer.Start(ctx, "index.Delete",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.String("doc_id", docID),
		))
	defer span.End()

	if docID == "" {
		err := errors.New("doc_id cannot be empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, "doc_id cannot be empty")
		return err
	}

	// Retrieve the document to get all its terms
	doc, err := i.GetDocument(ctx, docID)
	if err != nil {
		slog.ErrorContext(ctx, "failed to retrieve document for deletion", "index", i.name, "doc_id", docID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if doc == nil {
		return nil // Document already deleted
	}

	// Delete document entry
	docPK := lexkey.NewPrimaryKey(i.documentPartition(), lexkey.Encode(docID))
	err = i.store.Remove(ctx, docPK)
	if err != nil {
		slog.ErrorContext(ctx, "failed to delete document", "index", i.name, "doc_id", docID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Delete document from each term's posting list
	for term := range doc.Terms {
		termPK := lexkey.NewPrimaryKey(i.termPartition(term), lexkey.Encode(docID))
		err = i.store.Remove(ctx, termPK)
		if err != nil {
			slog.ErrorContext(ctx, "failed to delete posting", "index", i.name, "doc_id", docID, "term", term, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	slog.DebugContext(ctx, "document deleted", "index", i.name, "doc_id", docID)
	return nil
}

// Search returns documents matching the given query term.
func (i *indexStore) Search(ctx context.Context, term string) ([]SearchResult, error) {
	ctx, span := tracer.Start(ctx, "index.Search",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.String("term", term),
		))
	defer span.End()

	part := i.termPartition(term)
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := i.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		slog.ErrorContext(ctx, "failed to query postings", "index", i.name, "term", term, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var results []SearchResult
	for _, it := range items {
		var sp storedPosting
		if err := json.Unmarshal(it.Value, &sp); err != nil {
			slog.WarnContext(ctx, "skipping malformed posting", "index", i.name, "term", term, "err", err)
			continue
		}
		results = append(results, SearchResult{ID: sp.DocID, Score: 1.0, Meta: sp.Meta})
	}

	slog.DebugContext(ctx, "search completed", "index", i.name, "term", term, "result_count", len(results))
	return results, nil
}

// SearchMulti returns documents matching all terms in the query.
func (i *indexStore) SearchMulti(ctx context.Context, terms []string) ([]SearchResult, error) {
	ctx, span := tracer.Start(ctx, "index.SearchMulti",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.Int("term_count", len(terms)),
		))
	defer span.End()

	if len(terms) == 0 {
		return []SearchResult{}, nil
	}

	// Search for first term
	firstResults, err := i.Search(ctx, terms[0])
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if len(terms) == 1 {
		return firstResults, nil
	}

	// Build set of doc IDs from first term
	docIDs := make(map[string]SearchResult)
	for _, r := range firstResults {
		docIDs[r.ID] = r
	}

	// Intersect with remaining terms
	for _, term := range terms[1:] {
		results, err := i.Search(ctx, term)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		// Keep only documents in both sets
		newDocs := make(map[string]SearchResult)
		for _, r := range results {
			if doc, exists := docIDs[r.ID]; exists {
				// Increment score for documents matching multiple terms
				r.Score = doc.Score + 1.0
				newDocs[r.ID] = r
			}
		}
		docIDs = newDocs
	}

	// Convert back to slice
	var out []SearchResult
	for _, r := range docIDs {
		out = append(out, r)
	}

	slog.DebugContext(ctx, "multi-search completed", "index", i.name, "term_count", len(terms), "result_count", len(out))
	return out, nil
}

// HasDocument checks if a document exists in the index.
func (i *indexStore) HasDocument(ctx context.Context, docID string) (bool, error) {
	doc, err := i.GetDocument(ctx, docID)
	return doc != nil, err
}

// GetDocument retrieves the indexed terms for a document.
func (i *indexStore) GetDocument(ctx context.Context, docID string) (*Document, error) {
	ctx, span := tracer.Start(ctx, "index.GetDocument",
		trace.WithAttributes(
			attribute.String("index", i.name),
			attribute.String("doc_id", docID),
		))
	defer span.End()

	docPK := lexkey.NewPrimaryKey(i.documentPartition(), lexkey.Encode(docID))
	item, err := i.store.Get(ctx, docPK)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get document", "index", i.name, "doc_id", docID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	var sd storedDocument
	if err := json.Unmarshal(item.Value, &sd); err != nil {
		slog.ErrorContext(ctx, "failed to unmarshal document", "index", i.name, "doc_id", docID, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	doc := &Document{
		ID:    sd.ID,
		Terms: make(map[string][]byte),
	}
	for term, raw := range sd.Terms {
		doc.Terms[term] = raw
	}

	slog.DebugContext(ctx, "document retrieved", "index", i.name, "doc_id", docID, "term_count", len(doc.Terms))
	return doc, nil
}

// EnumerateDocuments streams all documents in the index.
func (i *indexStore) EnumerateDocuments(ctx context.Context) enumerators.Enumerator[Document] {
	part := i.documentPartition()
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inner := i.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (Document, bool, error) {
		var sd storedDocument
		if err := json.Unmarshal(it.Value, &sd); err != nil {
			return Document{}, false, nil
		}

		doc := Document{
			ID:    sd.ID,
			Terms: make(map[string][]byte),
		}
		for term, raw := range sd.Terms {
			doc.Terms[term] = raw
		}

		return doc, true, nil
	})
}

// EnumeratePostings streams all documents for a given term.
func (i *indexStore) EnumeratePostings(ctx context.Context, term string) enumerators.Enumerator[SearchResult] {
	part := i.termPartition(term)
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inner := i.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (SearchResult, bool, error) {
		var sp storedPosting
		if err := json.Unmarshal(it.Value, &sp); err != nil {
			return SearchResult{}, false, nil
		}

		result := SearchResult{ID: sp.DocID, Score: 1.0, Meta: sp.Meta}
		return result, true, nil
	})
}

// TermCount returns the total number of unique terms in the index.
func (i *indexStore) TermCount(ctx context.Context) (int, error) {
	ctx, span := tracer.Start(ctx, "index.TermCount",
		trace.WithAttributes(attribute.String("index", i.name)))
	defer span.End()

	// This is a naive implementation that counts all postings across all terms
	// In a production system, you might maintain a separate counter
	termSet := make(map[string]bool)

	// Enumerate all documents and collect unique terms
	enum := i.EnumerateDocuments(ctx)
	err := enumerators.ForEach(enum, func(doc Document) error {
		for term := range doc.Terms {
			termSet[term] = true
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	return len(termSet), nil
}

// DocumentCount returns the total number of documents in the index.
func (i *indexStore) DocumentCount(ctx context.Context) (int, error) {
	ctx, span := tracer.Start(ctx, "index.DocumentCount",
		trace.WithAttributes(attribute.String("index", i.name)))
	defer span.End()

	part := i.documentPartition()
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := i.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	return len(items), nil
}
