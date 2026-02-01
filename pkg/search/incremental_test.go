package searchoverlay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIncrementalUpdatesAreCorrect validates that re-indexing properly
// removes stale postings and only adds new ones, avoiding index bloat.
func TestIncrementalUpdatesAreCorrect(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Index entity with multiple fields and tokens
	entity1 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang Programming Guide"},
			{Field: "author", Value: "John Doe"},
			{Field: "tags", Value: "backend language"},
		},
		Payload: []byte("v1"),
	}
	require.NoError(t, overlay.Index(ctx, entity1))

	// Act - Update with completely different content
	entity2 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Python Data Science"},
			{Field: "author", Value: "Jane Smith"},
			{Field: "tags", Value: "machine-learning"},
		},
		Payload: []byte("v2"),
	}
	require.NoError(t, overlay.Index(ctx, entity2))

	// Assert - New tokens are found
	newTokens := []string{"python", "data", "science", "jane", "smith", "machine", "learning"}
	for _, token := range newTokens {
		results, err := overlay.Search(ctx, Query{Text: token})
		require.NoError(t, err)
		assert.Len(t, results, 1, "new token '%s' should be found", token)
		assert.Equal(t, "doc1", results[0].ID)
		assert.Equal(t, []byte("v2"), results[0].Payload, "should return updated payload")
	}

	// Assert - Old tokens are NOT found (stale postings removed)
	staleTokens := []string{"golang", "programming", "guide", "john", "doe", "backend", "language"}
	for _, token := range staleTokens {
		results, err := overlay.Search(ctx, Query{Text: token})
		require.NoError(t, err)
		assert.Len(t, results, 0, "stale token '%s' should not be found after re-indexing", token)
	}
}

// TestIncrementalUpdatePreservesUnchangedFields validates that unchanged
// tokens are not unnecessarily re-written.
func TestIncrementalUpdatePreservesUnchangedFields(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Index with common token
	entity1 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Programming Tutorial"},
			{Field: "status", Value: "published"},
		},
		Payload: []byte("v1"),
	}
	require.NoError(t, overlay.Index(ctx, entity1))

	// Act - Update with some fields unchanged
	entity2 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Advanced Programming"},
			{Field: "status", Value: "published"}, // unchanged
		},
		Payload: []byte("v2"),
	}
	require.NoError(t, overlay.Index(ctx, entity2))

	// Assert - Unchanged token still works
	results, err := overlay.Search(ctx, Query{Text: "published"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0].ID)

	// New token works
	results, err = overlay.Search(ctx, Query{Text: "advanced"})
	require.NoError(t, err)
	assert.Len(t, results, 1)

	// Removed token doesn't work
	results, err = overlay.Search(ctx, Query{Text: "tutorial"})
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

// TestDeleteRemovesAllPostings validates that Delete properly cleans
// up all index entries using the token registry.
func TestDeleteRemovesAllPostings(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Index multiple entities with overlapping tokens
	entity1 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang Guide"},
		},
		Payload: []byte("payload1"),
	}
	entity2 := SearchEntity{
		ID: "doc2",
		Attributes: []Attribute{
			{Field: "title", Value: "Python Guide"},
		},
		Payload: []byte("payload2"),
	}

	require.NoError(t, overlay.Index(ctx, entity1))
	require.NoError(t, overlay.Index(ctx, entity2))

	// Both should be found initially
	results, err := overlay.Search(ctx, Query{Text: "guide"})
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Act - Delete first entity
	require.NoError(t, overlay.Delete(ctx, "doc1"))

	// Assert - doc1 is completely gone
	results, err = overlay.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results, 0, "deleted entity should not appear in search")

	// doc2 still works
	results, err = overlay.Search(ctx, Query{Text: "python"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc2", results[0].ID)

	// Shared token only returns doc2 now
	results, err = overlay.Search(ctx, Query{Text: "guide"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc2", results[0].ID)
}

// TestBatchIndexWithIncremental validates BatchIndex also performs
// incremental updates correctly.
func TestBatchIndexWithIncremental(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Initial batch
	batch1 := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Alpha"},
			},
			Payload: []byte("v1"),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "title", Value: "Beta"},
			},
			Payload: []byte("v1"),
		},
	}
	require.NoError(t, overlay.BatchIndex(ctx, batch1))

	// Act - Re-index with changes
	batch2 := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Gamma"}, // changed
			},
			Payload: []byte("v2"),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "title", Value: "Beta"}, // unchanged
			},
			Payload: []byte("v2"),
		},
	}
	require.NoError(t, overlay.BatchIndex(ctx, batch2))

	// Assert - Old token removed for doc1
	results, err := overlay.Search(ctx, Query{Text: "alpha"})
	require.NoError(t, err)
	assert.Len(t, results, 0)

	// New token added for doc1
	results, err = overlay.Search(ctx, Query{Text: "gamma"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0].ID)

	// Unchanged token still works for doc2
	results, err = overlay.Search(ctx, Query{Text: "beta"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc2", results[0].ID)
}
