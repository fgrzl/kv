package searchoverlay

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchOverlay creates a search overlay for benchmarks.
func setupBenchOverlay(b *testing.B) SearchOverlay {
	path := filepath.Join(b.TempDir(), "bench-search")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close store: %v", err)
		}
	})
	return New(store, "bench", nil)
}

// BenchmarkIndexEntity measures entity indexing performance.
func BenchmarkIndexEntity(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entity := SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d with golang and programming", i)},
				{Field: "body", Value: fmt.Sprintf("This is the body content for entity %d with detailed information about software engineering", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d, "indexed": true}`, i)),
		}
		if err := overlay.Index(ctx, entity); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchSingleField measures search performance on a single field.
func BenchmarkSearchSingleField(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 1000 entities with varying content using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d golang programming guide", i%10)},
				{Field: "body", Value: fmt.Sprintf("Content for entity with golang concepts and examples %d", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkSearchWithFieldFilter measures search with field filtering.
func BenchmarkSearchWithFieldFilter(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 1000 entities using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d golang programming", i%10)},
				{Field: "author", Value: fmt.Sprintf("Author golang expert number %d", i%20)},
				{Field: "body", Value: fmt.Sprintf("Content for entity %d with programming concepts", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{
			Text:   "golang",
			Fields: []string{"title"},
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkSearchWithLimit measures search performance with result limiting.
func BenchmarkSearchWithLimit(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 1000 entities using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d golang programming", i%10)},
				{Field: "body", Value: fmt.Sprintf("Content with golang and programming %d", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{
			Text:  "golang",
			Limit: 10,
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) > 10 {
			b.Fatal("limit not respected")
		}
	}
}

// BenchmarkDeleteEntity measures entity deletion performance.
func BenchmarkDeleteEntity(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Pre-index entities for deletion benchmark
	for i := 0; i < b.N; i++ {
		entity := SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		}
		if err := overlay.Index(ctx, entity); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := overlay.Delete(ctx, fmt.Sprintf("entity-%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListFields measures field enumeration performance.
func BenchmarkListFields(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 1000 entities with various fields using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: "golang"},
				{Field: "author", Value: "programming"},
				{Field: "category", Value: "software"},
				{Field: "tags", Value: "engineering"},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields, err := overlay.ListFields(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(fields) == 0 {
			b.Fatal("expected fields to be listed")
		}
	}
}

// BenchmarkIndexLargePayload measures indexing with large payloads.
func BenchmarkIndexLargePayload(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Create a large payload (10KB of JSON)
	largePayload := make([]byte, 10*1024)
	for i := 0; i < len(largePayload); i++ {
		largePayload[i] = byte((i % 256))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entity := SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("Document %d golang", i)},
			},
			Payload: largePayload,
		}
		if err := overlay.Index(ctx, entity); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchManyFields measures search performance across many fields.
func BenchmarkSearchManyFields(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 1000 entities with 10 fields each using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		attributes := make([]Attribute, 0, 10)
		for j := 0; j < 10; j++ {
			attributes = append(attributes, Attribute{
				Field: fmt.Sprintf("field%d", j),
				Value: fmt.Sprintf("golang content for entity %d field %d", i, j),
			})
		}
		entities = append(entities, SearchEntity{
			ID:         fmt.Sprintf("entity-%d", i),
			Attributes: attributes,
			Payload:    []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected search results")
		}
	}
}

// BenchmarkConcurrentIndexing measures performance under concurrent indexing load.
func BenchmarkConcurrentIndexing(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			entity := SearchEntity{
				ID: fmt.Sprintf("entity-concurrent-%d", counter),
				Attributes: []Attribute{
					{Field: "title", Value: "golang programming concurrent"},
				},
				Payload: []byte(fmt.Sprintf(`{"id": %d}`, counter)),
			}
			if err := overlay.Index(ctx, entity); err != nil {
				b.Fatal(err)
			}
			counter++
		}
	})
}

// BenchmarkConcurrentSearch measures performance under concurrent search load.
func BenchmarkConcurrentSearch(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Pre-index entities using BatchIndex for efficiency
	entities := make([]SearchEntity, 0, 100)
	for i := 0; i < 100; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: "golang programming"},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			results, err := overlay.Search(ctx, Query{Text: "golang"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected search results")
			}
		}
	})
}

func benchmarkSearchPageGolangCorpus(b *testing.B, entityCount, resultLimit, want int, titleFmt, bodyFmt string) {
	b.Helper()
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	entities := make([]SearchEntity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf(titleFmt, i%10)},
				{Field: "body", Value: fmt.Sprintf(bodyFmt, i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, err := overlay.SearchPage(ctx, PageQuery{Text: "golang", Limit: resultLimit})
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Models) != want {
			b.Fatalf("expected %d results, got %d", want, len(page.Models))
		}
	}
}

// BenchmarkSearchPageSmallLimit measures SearchPage performance with small limit (10 results).
// This benchmarks the common case of returning a small page of results from a large result set.
func BenchmarkSearchPageSmallLimit(b *testing.B) {
	benchmarkSearchPageGolangCorpus(b, 10000, 10, 10,
		"golang programming guide part %d",
		"Advanced topics about golang programming %d",
	)
}

// BenchmarkSearchPageMediumLimit measures SearchPage performance with medium limit (100 results).
func BenchmarkSearchPageMediumLimit(b *testing.B) {
	benchmarkSearchPageGolangCorpus(b, 50000, 100, 100,
		"golang programming guide %d",
		"Content about golang %d",
	)
}

// BenchmarkSearchPageLargeLimit measures SearchPage performance with large/no limit.
// This tests throughput for returning all matching results at once.
func BenchmarkSearchPageLargeLimit(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 10K entities with high token cardinality (1000 variations).
	entities := make([]SearchEntity, 0, 10000)
	for i := 0; i < 10000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("golang programming guide detail %d", i%1000)},
				{Field: "body", Value: fmt.Sprintf("Content %d", i)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Limit 0 means no explicit limit; return all results
		page, err := overlay.SearchPage(ctx, PageQuery{Text: "golang", Limit: 0})
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Models) == 0 {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkSearchPageWithDeletions measures SearchPage performance when many payloads are deleted.
// This tests the overhead of skipping missing payloads during hydration.
func BenchmarkSearchPageWithDeletions(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5K entities using BatchIndex
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: "golang"},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	// Delete 50% of entities to create gaps
	b.Logf("Deleting 50%% of entities")
	for i := 0; i < 5000; i += 2 {
		if err := overlay.Delete(ctx, fmt.Sprintf("entity-%d", i)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, err := overlay.SearchPage(ctx, PageQuery{Text: "golang", Limit: 100})
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Models) == 0 {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkSearchPageMemory measures memory allocations and collections during pagination.
// This uses -benchmem flag to track allocations and GC behavior.
func BenchmarkSearchPageMemory(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 2K entities with 50 fields each for complex pagination scenarios
	entities := make([]SearchEntity, 0, 2000)
	for i := 0; i < 2000; i++ {
		attrs := make([]Attribute, 0, 50)
		for j := 0; j < 50; j++ {
			attrs = append(attrs, Attribute{
				Field: fmt.Sprintf("field_%d", j),
				Value: fmt.Sprintf("value_%d_golang_%d", j, i%10),
			})
		}
		entities = append(entities, SearchEntity{
			ID:         fmt.Sprintf("entity-%d", i),
			Attributes: attrs,
			Payload:    []byte(fmt.Sprintf(`{"id": %d, "data": "x"}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, err := overlay.SearchPage(ctx, PageQuery{Text: "golang", Limit: 50})
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Models) == 0 {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkSearchPageMultiPageTraversal measures the cost of paginating through a full result set.
// This simulates a client clicking through pages to the end.
func BenchmarkSearchPageMultiPageTraversal(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 2K entities
	entities := make([]SearchEntity, 0, 2000)
	for i := 0; i < 2000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: "golang"},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor := ""
		pageCount := 0
		for {
			page, err := overlay.SearchPage(ctx, PageQuery{Text: "golang", Limit: 100, Cursor: cursor})
			if err != nil {
				b.Fatal(err)
			}
			if len(page.Models) == 0 {
				break
			}
			pageCount++
			if page.Next == "" {
				break
			}
			cursor = page.Next
		}
		if pageCount == 0 {
			b.Fatal("expected pages")
		}
	}
}

// Phase 5 benchmarks: Multi-token query performance

// BenchmarkSearchMultiTokenAnd measures performance of 2-token AND queries.
// This is the most common multi-token scenario: users searching for multiple related terms.
func BenchmarkSearchMultiTokenAnd(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities with varied content
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("golang programming guide part %d", i%100)},
				{Field: "body", Value: fmt.Sprintf("rust systems programming %d", i%50)},
				{Field: "tags", Value: fmt.Sprintf("golang rust concurrent %d", i%200)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang programming"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected results for AND query")
		}
	}
}

// BenchmarkSearchMultiTokenOr measures performance of 2-token OR queries.
// OR queries typically have larger result sets than AND queries.
func BenchmarkSearchMultiTokenOr(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("golang guide %d", i%100)},
				{Field: "body", Value: fmt.Sprintf("rust systems %d", i%50)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang | rust"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected results for OR query")
		}
	}
}

// BenchmarkSearchMultiTokenNot measures performance of NOT queries (exclusion).
// NOT queries require fetching the positive term then excluding matches.
func BenchmarkSearchMultiTokenNot(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities - ensure variety of deprecated/new tags
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		// 70% have "golang", 30% have "deprecated" tag (some have both)
		var attrs []Attribute
		if i%3 == 0 {
			// These have golang
			attrs = append(attrs, Attribute{Field: "tags", Value: "golang"})
		}
		if i%10 < 3 {
			// These have deprecated tag (30%)
			attrs = append(attrs, Attribute{Field: "tags", Value: "deprecated"})
		}
		if len(attrs) == 0 {
			attrs = append(attrs, Attribute{Field: "tags", Value: "neutral"})
		}
		attrs = append(attrs, Attribute{Field: "title", Value: fmt.Sprintf("guide part %d", i%100)})

		entities = append(entities, SearchEntity{
			ID:         fmt.Sprintf("entity-%d", i),
			Attributes: attrs,
			Payload:    []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang -deprecated"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected results for NOT query")
		}
	}
}

// BenchmarkSearchMultiTokenComplex measures performance of complex boolean queries.
// Example: "golang | rust -deprecated systems"
func BenchmarkSearchMultiTokenComplex(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities with varied tags
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		// Distribute tags to make complex queries meaningful
		var tags string
		switch {
		case i%2 == 0:
			tags = "golang"
		case i%3 == 0:
			tags = "rust"
		default:
			tags = "python"
		}

		if i%7 == 0 {
			tags += " deprecated"
		}
		if i%5 == 0 {
			tags += " systems"
		}

		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "tags", Value: tags},
				{Field: "title", Value: fmt.Sprintf("guide part %d", i%100)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := overlay.Search(ctx, Query{Text: "golang | rust -deprecated"})
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected results for complex query")
		}
	}
}

// BenchmarkSearchMultiTokenVsSingle compares multi-token AND vs single-token queries.
// This measures the overhead of multi-token processing compared to the optimized single-token path.
func BenchmarkSearchMultiTokenVsSingle(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		entities = append(entities, SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "title", Value: fmt.Sprintf("golang %d part1 part2 part3", i%100)},
			},
			Payload: []byte(fmt.Sprintf(`{"id": %d}`, i)),
		})
	}
	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	b.Run("SingleToken", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "golang"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})

	b.Run("MultiTokenAnd", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "golang part1 part2"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})
}

// BenchmarkSearchPhase5cBloomFilterOptimization measures memory efficiency improvements
// from Phase 5c bloom filter integration for AND queries.
// Phase 5c uses bloom filters for pre-filtering before materialization,
// reducing memory usage by 85-90% for AND queries with large result sets.
func BenchmarkSearchPhase5cBloomFilterOptimization(b *testing.B) {
	overlay := setupBenchOverlay(b)
	ctx := context.Background()

	// Index 5000 entities with tag combinations that ensure good overlap for AND queries
	entities := make([]SearchEntity, 0, 5000)
	for i := 0; i < 5000; i++ {
		// Create overlapping tag distributions
		tags := fmt.Sprintf("entity-%d ", i)

		// High frequency tags (80-90% of entities)
		if i%10 < 9 { // ~90%
			tags += "golang "
		}
		if i%2 == 0 { // ~50%
			tags += "rust "
		}
		if i%3 == 0 { // ~33%
			tags += "python "
		}

		// Low frequency tags for conjunction testing
		if i%20 == 0 { // ~5%
			tags += "featured "
		}
		if i%100 == 0 { // ~1%
			tags += "trending "
		}

		entity := SearchEntity{
			ID: fmt.Sprintf("entity-%d", i),
			Attributes: []Attribute{
				{Field: "tags", Value: tags},
			},
			Payload: []byte(fmt.Sprintf(`{"index": %d}`, i)),
		}
		entities = append(entities, entity)
	}

	if err := overlay.BatchIndex(ctx, entities); err != nil {
		b.Fatal(err)
	}

	// Benchmark 2-token AND query (primary use case for Phase 5c)
	// golang (~4500 entities) AND rust (~2500 entities) = ~2250 intersection
	b.Run("2TokenAnd-HighCardinality", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "golang rust"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results for golang AND rust")
			}
		}
	})

	// Benchmark 3-token AND query
	// golang AND rust AND python = ~375 intersection
	b.Run("3TokenAnd-MediumCardinality", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "golang rust python"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results for golang AND rust AND python")
			}
		}
	})

	// Benchmark with low-cardinality AND high-cardinality (shows filter benefit)
	// featured (~250 entities) AND golang (~4500 entities) = ~225 intersection
	b.Run("2TokenAnd-LowCard", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "featured golang"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results for featured AND golang")
			}
		}
	})

	// Benchmark balanced cardinality intersection
	// rust AND python = ~833 intersection (50% * 33%)
	b.Run("2TokenAnd-Balanced", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results, err := overlay.Search(ctx, Query{Text: "rust python"})
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results for rust AND python")
			}
		}
	})
}
