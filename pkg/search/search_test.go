package searchoverlay

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

func createTestOverlay(t *testing.T) SearchOverlay {
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return New(store, "test", slog.Default())
}

func TestShouldIndexEntitySuccessfully(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()
	entity := SearchEntity{
		ID: "entity1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang Programming Guide"},
			{Field: "body", Value: "Learn concurrent programming in Go"},
		},
		Payload: []byte(`{"category": "programming"}`),
	}

	// Act
	err := overlay.Index(ctx, entity)

	// Assert
	require.NoError(t, err)
	results, err := overlay.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "entity1", results[0].ID)
	assert.NotEmpty(t, results[0].MatchedFields)
}

func TestShouldReturnErrorWhenEntityIDIsEmpty(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()
	entity := SearchEntity{
		ID: "",
		Attributes: []Attribute{
			{Field: "title", Value: "Test"},
		},
		Payload: []byte("test"),
	}

	// Act
	err := overlay.Index(ctx, entity)

	// Assert
	assert.Error(t, err)
}

func TestShouldDeleteEntityAndRemoveFromSearchResults(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()
	entity := SearchEntity{
		ID: "entity1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: []byte("test"),
	}
	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	err = overlay.Delete(ctx, "entity1")

	// Assert
	require.NoError(t, err)
	results, err := overlay.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestShouldReturnErrorWhenDeletingEntityWithEmptyID(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Act
	err := overlay.Delete(ctx, "")

	// Assert
	assert.Error(t, err)
}

func TestShouldFindDocumentsWhenSearchingSingleToken(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entities := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Programming"},
			},
			Payload: []byte("doc1"),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Concurrency"},
			},
			Payload: []byte("doc2"),
		},
		{
			ID: "doc3",
			Attributes: []Attribute{
				{Field: "title", Value: "Rust Systems Programming"},
			},
			Payload: []byte("doc3"),
		},
	}

	for _, e := range entities {
		err := overlay.Index(ctx, e)
		require.NoError(t, err)
	}

	// Act
	results, err := overlay.Search(ctx, Query{Text: "golang"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 2)

	ids := make(map[string]bool)
	for _, r := range results {
		ids[r.ID] = true
	}
	assert.True(t, ids["doc1"])
	assert.True(t, ids["doc2"])
}

func TestShouldFilterResultsByFieldWhenSpecified(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entities := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Programming"},
				{Field: "body", Value: "Learn Go in depth"},
			},
			Payload: []byte("doc1"),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "title", Value: "Python Tutorial"},
				{Field: "body", Value: "Golang is great for backends"},
			},
			Payload: []byte("doc2"),
		},
	}

	for _, e := range entities {
		err := overlay.Index(ctx, e)
		require.NoError(t, err)
	}

	// Act - Search only in title field
	results, err := overlay.Search(ctx, Query{
		Text:   "golang",
		Fields: []string{"title"},
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0].ID)
	assert.Contains(t, results[0].MatchedFields, "title")
}

func TestShouldSearchAcrossMultipleFields(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang Guide"},
			{Field: "author", Value: "Go Community"},
			{Field: "tags", Value: "golang programming concurrency"},
		},
		Payload: []byte("doc1"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{
		Text:   "golang",
		Fields: []string{"title", "author", "tags"},
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0].ID)
}

func TestShouldReturnNilWhenSearchQueryIsEmpty(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: []byte("doc1"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: ""})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestShouldReturnEmptyResultsWhenTokenNotFound(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: []byte("doc1"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: "nonexistent"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestShouldRespectResultLimitParameter(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Index 5 entities with "golang"
	for i := 1; i <= 5; i++ {
		entity := SearchEntity{
			ID: string(rune('a' + i - 1)),
			Attributes: []Attribute{
				{Field: "title", Value: "Golang Programming"},
			},
			Payload: []byte("test"),
		}
		err := overlay.Index(ctx, entity)
		require.NoError(t, err)
	}

	// Act
	results, err := overlay.Search(ctx, Query{
		Text:  "golang",
		Limit: 2,
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestShouldListAllIndexedFieldsSuccessfully(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entities := []SearchEntity{
		{
			ID: "doc1",
			Attributes: []Attribute{
				{Field: "title", Value: "Test"},
				{Field: "body", Value: "Content"},
			},
			Payload: []byte("doc1"),
		},
		{
			ID: "doc2",
			Attributes: []Attribute{
				{Field: "author", Value: "John"},
				{Field: "date", Value: "2024"},
			},
			Payload: []byte("doc2"),
		},
	}

	for _, e := range entities {
		err := overlay.Index(ctx, e)
		require.NoError(t, err)
	}

	// Act
	fields, err := overlay.ListFields(ctx)

	// Assert
	require.NoError(t, err)
	assert.Len(t, fields, 4)

	fieldSet := make(map[string]bool)
	for _, f := range fields {
		fieldSet[f] = true
	}
	assert.True(t, fieldSet["title"])
	assert.True(t, fieldSet["body"])
	assert.True(t, fieldSet["author"])
	assert.True(t, fieldSet["date"])
}

func TestShouldReturnEmptyFieldsWhenNothingIndexed(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Act
	fields, err := overlay.ListFields(ctx)

	// Assert
	require.NoError(t, err)
	assert.Len(t, fields, 0)
}

func TestShouldTokenizeAttributeValuesCorrectly(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Entity with special characters
	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Hello-World! Test_123 CamelCase"},
		},
		Payload: []byte("doc1"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act & Assert - Various tokens should be found
	tests := []struct {
		query string
		found bool
	}{
		{"hello", true},
		{"world", true},
		{"test", true},
		{"camelcase", true},
		{"123", true},
		{"hello world", true}, // First token matched
	}

	for _, test := range tests {
		results, err := overlay.Search(ctx, Query{Text: test.query})
		require.NoError(t, err)
		if test.found {
			assert.NotEmpty(t, results, "query '%s' should find results", test.query)
		} else {
			assert.Empty(t, results, "query '%s' should not find results", test.query)
		}
	}
}

func TestShouldUpdateEntityAttributesWhenReindexed(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Index initial entity
	entity1 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang Programming"},
		},
		Payload: []byte("version1"),
	}

	err := overlay.Index(ctx, entity1)
	require.NoError(t, err)

	// Act - Update the entity
	entity2 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Python Programming"},
		},
		Payload: []byte("version2"),
	}

	err = overlay.Index(ctx, entity2)
	require.NoError(t, err)

	// Assert - Search for new content
	results, err := overlay.Search(ctx, Query{Text: "python"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "doc1", results[0].ID)

	// Old content should still match (index entries remain)
	results, err = overlay.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestShouldPreserveEntityPayloadInSearchResults(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	payload := []byte(`{"title": "Test", "category": "tech"}`)
	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: payload,
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: "golang"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, payload, results[0].Payload)
}

func TestShouldIsolateSeparateOverlayInstances(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	overlay1 := New(store, "index1", slog.Default())
	overlay2 := New(store, "index2", slog.Default())
	ctx := context.Background()

	// Index same ID in different overlays
	entity1 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: []byte("index1"),
	}

	entity2 := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Python"},
		},
		Payload: []byte("index2"),
	}

	err = overlay1.Index(ctx, entity1)
	require.NoError(t, err)

	err = overlay2.Index(ctx, entity2)
	require.NoError(t, err)

	// Act & Assert - Verify isolation
	results1, err := overlay1.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results1, 1)
	assert.Equal(t, []byte("index1"), results1[0].Payload)

	results2, err := overlay2.Search(ctx, Query{Text: "python"})
	require.NoError(t, err)
	assert.Len(t, results2, 1)
	assert.Equal(t, []byte("index2"), results2[0].Payload)

	// Cross-index verification
	results1NoGolang, err := overlay1.Search(ctx, Query{Text: "python"})
	require.NoError(t, err)
	assert.Len(t, results1NoGolang, 0)

	results2NoGolang, err := overlay2.Search(ctx, Query{Text: "golang"})
	require.NoError(t, err)
	assert.Len(t, results2NoGolang, 0)
}

func TestShouldSearchAllFieldsWhenFieldListIsEmpty(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
		},
		Payload: []byte("test"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act - Search with empty field list (should search all fields)
	results, err := overlay.Search(ctx, Query{
		Text:   "golang",
		Fields: []string{},
	})

	// Assert - Should return results when field list is empty
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestShouldHandleWhitespaceInSearchQueries(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "  title  ", Value: "  Golang  Programming  "},
		},
		Payload: []byte("test"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: "  golang  "})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

func TestShouldTrackMatchedFieldsInResults(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "Golang"},
			{Field: "body", Value: "Learn Golang"},
			{Field: "tags", Value: "golang programming"},
		},
		Payload: []byte("test"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: "golang"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Len(t, results[0].MatchedFields, 3)

	fieldSet := make(map[string]bool)
	for _, f := range results[0].MatchedFields {
		fieldSet[f] = true
	}
	assert.True(t, fieldSet["title"])
	assert.True(t, fieldSet["body"])
	assert.True(t, fieldSet["tags"])
}

func TestShouldPerformCaseInsensitiveSearch(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	entity := SearchEntity{
		ID: "doc1",
		Attributes: []Attribute{
			{Field: "title", Value: "GOLANG Programming"},
		},
		Payload: []byte("test"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act & Assert - Various cases should match
	tests := []string{"golang", "GOLANG", "Golang", "GoLaNg"}

	for _, query := range tests {
		results, err := overlay.Search(ctx, Query{Text: query})
		require.NoError(t, err, "query '%s' should work", query)
		assert.Len(t, results, 1, "query '%s' should find result", query)
	}
}

func TestShouldIndexAndSearchEntityWithManyAttributes(t *testing.T) {
	// Arrange
	overlay := createTestOverlay(t)
	ctx := context.Background()

	// Create entity with many attributes
	attrs := []Attribute{
		{Field: "title", Value: "Golang"},
		{Field: "body", Value: "Content"},
		{Field: "author", Value: "John"},
		{Field: "category", Value: "Programming"},
		{Field: "tags", Value: "golang go concurrency"},
		{Field: "description", Value: "A guide to Go"},
	}

	entity := SearchEntity{
		ID:         "doc1",
		Attributes: attrs,
		Payload:    []byte("test"),
	}

	err := overlay.Index(ctx, entity)
	require.NoError(t, err)

	// Act
	results, err := overlay.Search(ctx, Query{Text: "golang"})

	// Assert
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Len(t, results[0].MatchedFields, 3) // title, tags, description
}
