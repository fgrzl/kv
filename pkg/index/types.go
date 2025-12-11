package index

import (
	"context"

	"github.com/fgrzl/enumerators"
)

// Document represents a searchable document with content that can be indexed.
type Document struct {
	ID    string
	Terms map[string][]byte // term -> metadata (e.g., term frequency, positions)
}

// SearchResult represents a document ID and score from a search operation.
type SearchResult struct {
	ID    string
	Score float64
	Meta  []byte
}

// InvertedIndex provides full-text search capabilities backed by a kv.KV store.
type InvertedIndex interface {
	// Index adds or updates a document in the inverted index.
	Index(ctx context.Context, doc Document) error

	// BatchIndex adds or updates multiple documents in the inverted index efficiently.
	// This is more efficient than calling Index() multiple times.
	BatchIndex(ctx context.Context, docs []Document) error

	// Delete removes a document from the inverted index.
	Delete(ctx context.Context, docID string) error

	// Search returns documents matching the given query term.
	Search(ctx context.Context, term string) ([]SearchResult, error)

	// SearchMulti returns documents matching all terms in the query.
	// Results are scored by term frequency/presence.
	SearchMulti(ctx context.Context, terms []string) ([]SearchResult, error)

	// HasDocument checks if a document exists in the index.
	HasDocument(ctx context.Context, docID string) (bool, error)

	// GetDocument retrieves the indexed terms for a document.
	GetDocument(ctx context.Context, docID string) (*Document, error)

	// EnumerateDocuments streams all documents in the index.
	EnumerateDocuments(ctx context.Context) enumerators.Enumerator[Document]

	// EnumeratePostings streams all documents for a given term.
	EnumeratePostings(ctx context.Context, term string) enumerators.Enumerator[SearchResult]

	// TermCount returns the total number of unique terms in the index.
	TermCount(ctx context.Context) (int, error)

	// DocumentCount returns the total number of documents in the index.
	DocumentCount(ctx context.Context) (int, error)
}
