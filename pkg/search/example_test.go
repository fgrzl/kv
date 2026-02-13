package searchoverlay_test

import (
	"context"
	"fmt"
	"log"
	"os"

	searchoverlay "github.com/fgrzl/kv/pkg/search"
	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// Example demonstrates basic usage of the search overlay.
func Example() {
	// Setup: create a KV store backend
	dir, err := os.MkdirTemp("", "search-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := pebble.NewPebbleStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Create search overlay
	search := searchoverlay.New(store, "docs", nil)
	ctx := context.Background()

	// Index entities with searchable attributes
	entities := []searchoverlay.SearchEntity{
		{
			ID: "doc1",
			Attributes: []searchoverlay.Attribute{
				{Field: "title", Value: "Golang Programming Guide"},
				{Field: "author", Value: "John Doe"},
			},
			Payload: []byte(`{"views": 1000}`),
		},
		{
			ID: "doc2",
			Attributes: []searchoverlay.Attribute{
				{Field: "title", Value: "Python Data Science"},
				{Field: "author", Value: "Jane Smith"},
			},
			Payload: []byte(`{"views": 2000}`),
		},
	}

	if err := search.BatchIndex(ctx, entities); err != nil {
		log.Fatal(err)
	}

	// Search for entities
	results, err := search.Search(ctx, searchoverlay.Query{
		Text: "golang",
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, hit := range results {
		fmt.Printf("Found: %s (matched fields: %v)\n", hit.ID, hit.MatchedFields)
	}

	// Update an entity (incremental - only changed tokens are re-indexed)
	updatedEntity := searchoverlay.SearchEntity{
		ID: "doc1",
		Attributes: []searchoverlay.Attribute{
			{Field: "title", Value: "Advanced Golang Patterns"},
			{Field: "author", Value: "John Doe"}, // unchanged
		},
		Payload: []byte(`{"views": 1500}`),
	}

	if err := search.Index(ctx, updatedEntity); err != nil {
		log.Fatal(err)
	}

	// Old token "programming" is removed, new token "advanced" is added
	results, err = search.Search(ctx, searchoverlay.Query{
		Text: "programming",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Results for 'programming': %d\n", len(results))

	results, err = search.Search(ctx, searchoverlay.Query{
		Text: "advanced",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Results for 'advanced': %d\n", len(results))

	// Output:
	// Found: doc1 (matched fields: [title])
	// Results for 'programming': 0
	// Results for 'advanced': 1
}

// Example_fieldScoping demonstrates field-scoped search.
func Example_fieldScoping() {
	dir, err := os.MkdirTemp("", "search-field-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := pebble.NewPebbleStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	search := searchoverlay.New(store, "products", nil)
	ctx := context.Background()

	// Index products
	products := []searchoverlay.SearchEntity{
		{
			ID: "prod1",
			Attributes: []searchoverlay.Attribute{
				{Field: "name", Value: "Apple iPhone"},
				{Field: "category", Value: "Electronics"},
			},
			Payload: []byte(`{"price": 999}`),
		},
		{
			ID: "prod2",
			Attributes: []searchoverlay.Attribute{
				{Field: "name", Value: "Fresh Apple"},
				{Field: "category", Value: "Groceries"},
			},
			Payload: []byte(`{"price": 2}`),
		},
	}

	if err := search.BatchIndex(ctx, products); err != nil {
		log.Fatal(err)
	}

	// Search across all fields
	results, err := search.Search(ctx, searchoverlay.Query{
		Text: "apple",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("All fields: %d results\n", len(results))

	// Search only in 'category' field
	results, err = search.Search(ctx, searchoverlay.Query{
		Text:   "electronics",
		Fields: []string{"category"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Category only: %d results\n", len(results))

	// Output:
	// All fields: 2 results
	// Category only: 1 results
}
