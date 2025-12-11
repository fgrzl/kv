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
	b.Cleanup(func() { store.Close() })
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
