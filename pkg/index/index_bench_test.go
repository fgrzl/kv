package index

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchIndex creates an inverted index for benchmarks.
func setupBenchIndex(b *testing.B) InvertedIndex {
	path := filepath.Join(b.TempDir(), "bench-index")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	return New(store, "bench")
}

// BenchmarkIndexDocument measures single document indexing performance.
func BenchmarkIndexDocument(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":      []byte(`{"position": 0}`),
				"programming": []byte(`{"position": 1}`),
				"software":    []byte(`{"position": 2}`),
				"engineering": []byte(`{"position": 3}`),
			},
		}
		if err := idx.Index(ctx, doc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchSingleTerm measures single-term search performance.
func BenchmarkSearchSingleTerm(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents with overlapping terms using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":                     []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"database":                   []byte(fmt.Sprintf(`{"id": %d}`, i)),
				fmt.Sprintf("topic%d", i%10): []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := idx.Search(ctx, "golang")
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkSearchMultipleTerms measures multi-term search performance.
func BenchmarkSearchMultipleTerms(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":                       []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"programming":                  []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"database":                     []byte(fmt.Sprintf(`{"id": %d}`, i)),
				fmt.Sprintf("category%d", i%5): []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := idx.SearchMulti(ctx, []string{"golang", "programming"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkSearchMultipleTermsLargeIndex measures multi-term search on a large index.
func BenchmarkSearchMultipleTermsLargeIndex(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 10000 documents using BatchIndex for efficiency
	docs := make([]Document, 0, 10000)
	for i := 0; i < 10000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":                   []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"programming":              []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"database":                 []byte(fmt.Sprintf(`{"id": %d}`, i)),
				fmt.Sprintf("tag%d", i%20): []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := idx.SearchMulti(ctx, []string{"golang", "programming", "database"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkDeleteDocument measures document deletion performance.
func BenchmarkDeleteDocument(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Pre-index documents for deletion using BatchIndex for efficiency
	docs := make([]Document, 0, b.N)
	for i := 0; i < b.N; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":      []byte(`{"position": 0}`),
				"programming": []byte(`{"position": 1}`),
				"database":    []byte(`{"position": 2}`),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.Delete(ctx, fmt.Sprintf("doc-%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnumerateDocuments measures document enumeration performance.
func BenchmarkEnumerateDocuments(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":      []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"programming": []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		enum := idx.EnumerateDocuments(ctx)
		_ = enumerators.ForEach(enum, func(doc Document) error {
			count++
			return nil
		})
		if count != 1000 {
			b.Fatalf("expected 1000 documents, got %d", count)
		}
	}
}

// BenchmarkEnumeratePostings measures posting enumeration performance for a term.
func BenchmarkEnumeratePostings(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents with common term using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"common":                   []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"unique":                   []byte(fmt.Sprintf(`{"id": %d}`, i)),
				fmt.Sprintf("tag%d", i%10): []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		enum := idx.EnumeratePostings(ctx, "common")
		_ = enumerators.ForEach(enum, func(result SearchResult) error {
			count++
			return nil
		})
		if count != 1000 {
			b.Fatalf("expected 1000 postings, got %d", count)
		}
	}
}

// BenchmarkTermCount measures term counting performance.
func BenchmarkTermCount(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents with 5 terms each using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":      []byte(`{}`),
				"programming": []byte(`{}`),
				"database":    []byte(`{}`),
				"software":    []byte(`{}`),
				"engineering": []byte(`{}`),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, err := idx.TermCount(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if count != 5 {
			b.Fatalf("expected 5 unique terms, got %d", count)
		}
	}
}

// BenchmarkDocumentCount measures document counting performance.
func BenchmarkDocumentCount(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index 1000 documents using BatchIndex for efficiency
	docs := make([]Document, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang": []byte(`{}`),
				"term":   []byte(`{}`),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, err := idx.DocumentCount(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("expected 1000 documents, got %d", count)
		}
	}
}

// BenchmarkHasDocument measures document existence checking.
func BenchmarkHasDocument(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Pre-index a document
	doc := Document{
		ID: "doc-0",
		Terms: map[string][]byte{
			"golang": []byte(`{}`),
		},
	}
	if err := idx.Index(ctx, doc); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exists, err := idx.HasDocument(ctx, "doc-0")
		if err != nil {
			b.Fatal(err)
		}
		if !exists {
			b.Fatal("expected document to exist")
		}
	}
}

// BenchmarkGetDocument measures document retrieval performance.
func BenchmarkGetDocument(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Index a document with many terms
	terms := make(map[string][]byte)
	for j := 0; j < 50; j++ {
		terms[fmt.Sprintf("term%d", j)] = []byte(fmt.Sprintf(`{"pos": %d}`, j))
	}
	doc := Document{
		ID:    "doc-0",
		Terms: terms,
	}
	if err := idx.Index(ctx, doc); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		retrieved, err := idx.GetDocument(ctx, "doc-0")
		if err != nil {
			b.Fatal(err)
		}
		if retrieved == nil || len(retrieved.Terms) != 50 {
			b.Fatal("expected document with 50 terms")
		}
	}
}

// BenchmarkConcurrentSearch measures performance under concurrent search load.
func BenchmarkConcurrentSearch(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Pre-index documents using BatchIndex for efficiency
	docs := make([]Document, 0, 500)
	for i := 0; i < 500; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":                     []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"programming":                []byte(fmt.Sprintf(`{"id": %d}`, i)),
				fmt.Sprintf("topic%d", i%10): []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			results, err := idx.Search(ctx, "golang")
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected search results")
			}
		}
	})
}

// BenchmarkConcurrentIndexing measures performance under concurrent indexing load.
func BenchmarkConcurrentIndexing(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			doc := Document{
				ID: fmt.Sprintf("doc-concurrent-%d-%d", counter, counter),
				Terms: map[string][]byte{
					"golang":      []byte(`{}`),
					"programming": []byte(`{}`),
				},
			}
			if err := idx.Index(ctx, doc); err != nil {
				b.Fatal(err)
			}
			counter++
		}
	})
}

// BenchmarkReindexDocument measures re-indexing performance (update scenario).
func BenchmarkReindexDocument(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Initial document
	doc := Document{
		ID: "doc-0",
		Terms: map[string][]byte{
			"golang": []byte(`{}`),
			"old":    []byte(`{}`),
		},
	}
	if err := idx.Index(ctx, doc); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updated := Document{
			ID: "doc-0",
			Terms: map[string][]byte{
				"golang":                []byte(`{}`),
				fmt.Sprintf("new%d", i): []byte(`{}`),
			},
		}
		if err := idx.Index(ctx, updated); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMixedWorkload measures performance under mixed read/write load.
func BenchmarkMixedWorkload(b *testing.B) {
	idx := setupBenchIndex(b)
	ctx := context.Background()

	// Pre-index some documents using BatchIndex for efficiency
	docs := make([]Document, 0, 100)
	for i := 0; i < 100; i++ {
		docs = append(docs, Document{
			ID: fmt.Sprintf("doc-%d", i),
			Terms: map[string][]byte{
				"golang":      []byte(fmt.Sprintf(`{"id": %d}`, i)),
				"programming": []byte(fmt.Sprintf(`{"id": %d}`, i)),
			},
		})
	}
	if err := idx.BatchIndex(ctx, docs); err != nil {
		b.Fatal(err)
	}

	var mu sync.Mutex
	counter := 100

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%3 == 0 {
				// Search operation (2/3 of load)
				_, _ = idx.Search(ctx, "golang")
			} else if i%3 == 1 {
				// Index operation (1/3 of load)
				mu.Lock()
				docID := counter
				counter++
				mu.Unlock()
				doc := Document{
					ID: fmt.Sprintf("doc-%d", docID),
					Terms: map[string][]byte{
						"golang": []byte(`{}`),
					},
				}
				_ = idx.Index(ctx, doc)
			} else {
				// Count operation (rest)
				_, _ = idx.DocumentCount(ctx)
			}
			i++
		}
	})
}
