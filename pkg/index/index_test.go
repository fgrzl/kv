package index

import (
	"context"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

func createTestIndex(t *testing.T) InvertedIndex {
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return New(store, "test")
}

func TestShouldIndexDocumentSuccessfully(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	doc := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"hello": []byte("meta1"), "world": []byte("meta2")},
	}

	// Act
	err := idx.Index(ctx, doc)

	// Assert
	require.NoError(t, err)
	retrieved, err := idx.GetDocument(ctx, "doc1")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "doc1", retrieved.ID)
	assert.Equal(t, 2, len(retrieved.Terms))
}

func TestShouldReturnErrorWhenDocumentIDIsEmpty(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	doc := Document{
		ID:    "",
		Terms: map[string][]byte{"hello": []byte("meta1")},
	}

	// Act
	err := idx.Index(ctx, doc)

	// Assert
	assert.Error(t, err)
}

func TestShouldDeleteDocumentSuccessfully(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	doc := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"hello": []byte("meta1")},
	}
	err := idx.Index(ctx, doc)
	require.NoError(t, err)

	// Act
	err = idx.Delete(ctx, "doc1")

	// Assert
	require.NoError(t, err)
	exists, err := idx.HasDocument(ctx, "doc1")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestShouldFindDocumentsWhenSearchingSingleTerm(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"golang": []byte("meta1"), "programming": []byte("meta1")}},
		{ID: "doc2", Terms: map[string][]byte{"golang": []byte("meta2"), "concurrency": []byte("meta2")}},
		{ID: "doc3", Terms: map[string][]byte{"rust": []byte("meta3"), "systems": []byte("meta3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	results, err := idx.Search(ctx, "golang")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, len(results))
	docIDs := make(map[string]bool)
	for _, r := range results {
		docIDs[r.ID] = true
	}
	assert.True(t, docIDs["doc1"])
	assert.True(t, docIDs["doc2"])
}

func TestShouldFindDocumentsWhenSearchingMultipleTerms(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"golang": []byte("m1"), "fast": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"golang": []byte("m2"), "concurrent": []byte("m2")}},
		{ID: "doc3", Terms: map[string][]byte{"fast": []byte("m3"), "efficient": []byte("m3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	results, err := idx.SearchMulti(ctx, []string{"golang", "fast"})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "doc1", results[0].ID)
}

func TestShouldCalculateRelevanceScoreWhenSearchingMultipleTerms(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"a": []byte("m1"), "b": []byte("m1"), "c": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"a": []byte("m2"), "b": []byte("m2")}},
		{ID: "doc3", Terms: map[string][]byte{"a": []byte("m3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	resultThreeTerms, err := idx.SearchMulti(ctx, []string{"a", "b", "c"})
	require.NoError(t, err)
	resultTwoTerms, err := idx.SearchMulti(ctx, []string{"a", "b"})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, len(resultThreeTerms))
	assert.Equal(t, "doc1", resultThreeTerms[0].ID)
	assert.Equal(t, 3.0, resultThreeTerms[0].Score)

	assert.Equal(t, 2, len(resultTwoTerms))
	maxScore := 0.0
	for _, r := range resultTwoTerms {
		if r.Score > maxScore {
			maxScore = r.Score
		}
	}
	assert.Equal(t, 2.0, maxScore)
}

func TestShouldEnumerateAllDocumentsSuccessfully(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"a": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"b": []byte("m2")}},
		{ID: "doc3", Terms: map[string][]byte{"c": []byte("m3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	var enumDocs []Document
	enum := idx.EnumerateDocuments(ctx)
	err := enumerators.ForEach(enum, func(doc Document) error {
		enumDocs = append(enumDocs, doc)
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 3, len(enumDocs))
}

func TestShouldEnumerateAllPostingsForATerm(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"golang": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"golang": []byte("m2")}},
		{ID: "doc3", Terms: map[string][]byte{"rust": []byte("m3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	var results []SearchResult
	enum := idx.EnumeratePostings(ctx, "golang")
	err := enumerators.ForEach(enum, func(result SearchResult) error {
		results = append(results, result)
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestShouldCountUniqueTermsCorrectly(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"golang": []byte("m1"), "fast": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"golang": []byte("m2"), "concurrent": []byte("m2")}},
		{ID: "doc3", Terms: map[string][]byte{"rust": []byte("m3")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	count, err := idx.TermCount(ctx)

	// Assert
	require.NoError(t, err)
	// Unique terms: golang, fast, concurrent, rust = 4
	assert.Equal(t, 4, count)
}

func TestShouldCountIndexedDocumentsCorrectly(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	docs := []Document{
		{ID: "doc1", Terms: map[string][]byte{"a": []byte("m1")}},
		{ID: "doc2", Terms: map[string][]byte{"b": []byte("m2")}},
	}
	for _, doc := range docs {
		err := idx.Index(ctx, doc)
		require.NoError(t, err)
	}

	// Act
	count, err := idx.DocumentCount(ctx)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestShouldPreserveDocumentTermsWhenReindexing(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	doc1 := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"golang": []byte("m1"), "fast": []byte("m1")},
	}
	err := idx.Index(ctx, doc1)
	require.NoError(t, err)

	// Act - Update the document with different terms
	doc2 := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"python": []byte("m2"), "slow": []byte("m2")},
	}
	err = idx.Index(ctx, doc2)
	require.NoError(t, err)

	// Assert - Search for old term should still return results
	results, err := idx.Search(ctx, "golang")
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

func TestShouldReturnEmptyResultsWhenSearchingNonexistentTerm(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()
	doc := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"golang": []byte("m1")},
	}
	err := idx.Index(ctx, doc)
	require.NoError(t, err)

	// Act
	results, err := idx.Search(ctx, "nonexistent")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestShouldReturnEmptyResultsWhenSearchMultiWithEmptyTerms(t *testing.T) {
	// Arrange
	idx := createTestIndex(t)
	ctx := context.Background()

	// Act
	results, err := idx.SearchMulti(ctx, []string{})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestShouldIsolateSeparateIndexInstancesOnSameStore(t *testing.T) {
	// Arrange
	path := t.TempDir()
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	ctx := context.Background()

	idx1 := New(store, "index1")
	idx2 := New(store, "index2")

	doc1 := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"golang": []byte("m1")},
	}
	doc2 := Document{
		ID:    "doc1",
		Terms: map[string][]byte{"python": []byte("m2")},
	}
	err = idx1.Index(ctx, doc1)
	require.NoError(t, err)
	err = idx2.Index(ctx, doc2)
	require.NoError(t, err)

	// Act
	results1, err := idx1.Search(ctx, "golang")
	require.NoError(t, err)
	results2, err := idx2.Search(ctx, "golang")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, len(results1))
	assert.Equal(t, 0, len(results2))
}
