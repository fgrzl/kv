package searchoverlay

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// Integration tests for Phase 5b: Multi-token query support

func setupMultiTokenTestData(t *testing.T, overlay SearchOverlay) {
	ctx := context.Background()

	entities := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Programming Guide"},
				{Field: "tags", Value: "golang programming"},
			},
			Payload: []byte(`{"category": "programming"}`),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "title", Value: "Rust Systems Programming"},
				{Field: "tags", Value: "rust systems low-level"},
			},
			Payload: []byte(`{"category": "systems"}`),
		},
		{
			ID: "doc3",
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Web Development"},
				{Field: "tags", Value: "golang web backend"},
			},
			Payload: []byte(`{"category": "web"}`),
		},
		{
			ID: "doc4",
			Attributes: []Attribute{
				{Field: "title", Value: "Python Data Science"},
				{Field: "tags", Value: "python data machine-learning"},
			},
			Payload: []byte(`{"category": "data"}`),
		},
	}

	for _, e := range entities {
		err := overlay.Index(ctx, e)
		require.NoError(t, err)
	}
}

func TestMultiTokenImplicitAnd(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: "golang programming" should find docs with both tokens
	results, err := overlay.Search(ctx, Query{Text: "golang programming"})

	// Assert
	require.NoError(t, err)
	// Only doc1 has both "golang" and "programming" in the same or different fields
	assert.Greater(t, len(results), 0, "should find at least one result")

	// Verify all results have both tokens
	for _, hit := range results {
		assert.Equal(t, "doc1", hit.ID)
	}
}

func TestMultiTokenExplicitOr(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: "golang | rust" should find docs with either token
	results, err := overlay.Search(ctx, Query{Text: "golang | rust"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 3) // doc1, doc2, doc3 all have golang or rust

	// Verify result IDs
	resultIDs := make(map[string]bool)
	for _, hit := range results {
		resultIDs[hit.ID] = true
	}
	assert.True(t, resultIDs["doc1"], "should find doc1 with golang")
	assert.True(t, resultIDs["doc2"], "should find doc2 with rust")
	assert.True(t, resultIDs["doc3"], "should find doc3 with golang")
	assert.False(t, resultIDs["doc4"], "should not find doc4 (python)")
}

func TestMultiTokenNot(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: "-programming golang" should find golang doc without programming
	results, err := overlay.Search(ctx, Query{Text: "golang -programming"})

	// Assert
	require.NoError(t, err)
	// Should find doc3 (golang without programming) but not doc1 (has programming)
	foundDoc3 := false
	for _, hit := range results {
		assert.NotEqual(t, "doc1", hit.ID, "should exclude doc1 with programming tag")
		if hit.ID == "doc3" {
			foundDoc3 = true
		}
	}
	assert.True(t, foundDoc3, "should find doc3 (golang without programming)")
}

func TestMultiTokenComplexExpression(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: "golang | rust -systems" should find golang OR (rust without systems)
	results, err := overlay.Search(ctx, Query{Text: "golang | rust -systems"})

	// Assert
	require.NoError(t, err)
	// Should find doc1 and doc3 (golang), but not doc2 (rust with systems)
	resultIDs := make(map[string]bool)
	for _, hit := range results {
		resultIDs[hit.ID] = true
	}
	assert.True(t, resultIDs["doc1"], "should find doc1 with golang")
	assert.True(t, resultIDs["doc3"], "should find doc3 with golang")
	assert.False(t, resultIDs["doc2"], "should exclude doc2 (rust with systems)")
}

func TestMultiTokenWithFieldFilter(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: Search "golang programming" in title field only
	results, err := overlay.Search(ctx, Query{
		Text:   "golang programming",
		Fields: []string{"title"},
	})

	// Assert
	require.NoError(t, err)
	// Should find documents with both tokens in title field
	// doc1 has "Golang Programming Guide" in title
	foundDoc1 := false
	for _, hit := range results {
		if hit.ID == "doc1" {
			foundDoc1 = true
			// Verify matched field is title
			assert.Contains(t, hit.MatchedFields, "title")
		}
	}
	assert.True(t, foundDoc1, "should find doc1 with both tokens in title")
}

func TestMultiTokenSearchPageWithOr(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: Paginate through "golang | rust" results
	page1, err := overlay.SearchPage(ctx, PageQuery{
		Text:   "golang | rust",
		Limit:  2,
		Cursor: "",
	})

	// Assert
	require.NoError(t, err)
	assert.Greater(t, len(page1.Models), 0, "should find items")
	assert.NotEmpty(t, page1.Next, "should have next cursor")

	// Get next page
	page2, err := overlay.SearchPage(ctx, PageQuery{
		Text:   "golang | rust",
		Limit:  2,
		Cursor: page1.Next,
	})

	require.NoError(t, err)
	assert.Greater(t, len(page2.Models), 0, "should find items on page 2")
}

func TestMultiTokenSingleTokenOptimization(t *testing.T) {
	// Arrange: Verify that single-token "queries" still use optimized path
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: Single token query should work (backward compat)
	results, err := overlay.Search(ctx, Query{Text: "golang"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 2) // doc1 and doc3 have golang
	for _, hit := range results {
		assert.True(t, hit.ID == "doc1" || hit.ID == "doc3")
	}
}

func TestMultiTokenQuotedPhrase(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: Quoted phrase should be treated as single token
	results, err := overlay.Search(ctx, Query{Text: `"golang programming"`})

	// Assert
	require.NoError(t, err)
	// Should find doc1 which has both "golang" and "programming"
	foundDoc1 := false
	for _, hit := range results {
		if hit.ID == "doc1" {
			foundDoc1 = true
		}
	}
	assert.True(t, foundDoc1, "should find doc1 with quoted phrase")
}

func TestMultiTokenEmptyResults(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	overlay := New(store, "test", slog.Default())

	setupMultiTokenTestData(t, overlay)
	ctx := context.Background()

	// Act: Query that matches nothing
	results, err := overlay.Search(ctx, Query{Text: "golang rust"})

	// Assert: No results (AND of two unrelated items)
	require.NoError(t, err)
	assert.Len(t, results, 0, "should find no results when AND matches nothing")
}
