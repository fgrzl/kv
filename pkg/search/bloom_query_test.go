package searchoverlay

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bloom Filter Tests
// ==================

func TestBloomFilterAddAndContains(t *testing.T) {
	// Arrange
	bf := NewBloomFilter(100)
	elements := []string{"hello", "world", "golang", "test"}

	// Act: Add elements
	for _, e := range elements {
		bf.Add(e)
	}

	// Assert: All added elements should be found
	for _, e := range elements {
		assert.True(t, bf.Contains(e), "element %s should be in filter", e)
	}
}

func TestBloomFilterFalseNegatives(t *testing.T) {
	// Arrange
	bf := NewBloomFilter(100)
	bf.Add("golang")

	// Act & Assert: An element not added must not be found (no false negatives)
	assert.False(t, bf.Contains("rust"), "non-existent element should not be found")
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// Arrange: Create filter with 1000 elements, expected FP rate ~0.1%
	bf := NewBloomFilter(1000)
	added := make(map[string]bool)

	// Add 500 real elements
	for i := 0; i < 500; i++ {
		token := strings.Join([]string{"token", string(rune(i + 65))}, "")
		bf.Add(token)
		added[token] = true
	}

	// Act: Check 1000 different elements not in the filter
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		token := strings.Join([]string{"notadded", string(rune(i + 1000 + 65))}, "")
		if _, exists := added[token]; !exists && bf.Contains(token) {
			falsePositives++
		}
	}

	// Assert: FP rate with 10 bits/element and k=3 should be ~1-5% in practice
	fpRate := float64(falsePositives) / 1000.0
	assert.Less(t, fpRate, 0.10, "FP rate %.2f%% is too high (expected <10%%)", fpRate*100)
}

func TestBloomFilterEmptyFilter(t *testing.T) {
	// Arrange & Act
	bf := NewBloomFilter(100)

	// Assert: Empty filter should not contain anything
	testElements := []string{"test", "empty", "filter", "should", "not", "contain"}
	for _, e := range testElements {
		assert.False(t, bf.Contains(e), "empty filter should not contain %s", e)
	}
}

func TestBloomFilterSmallCardinality(t *testing.T) {
	// Arrange
	bf := NewBloomFilter(10)

	// Act: Add just a few elements
	bf.Add("a")
	bf.Add("b")
	bf.Add("c")

	// Assert
	assert.True(t, bf.Contains("a"))
	assert.True(t, bf.Contains("b"))
	assert.True(t, bf.Contains("c"))
	assert.False(t, bf.Contains("d"))
}

func TestBloomFilterEstimatedCardinality(t *testing.T) {
	// Arrange
	bf := NewBloomFilter(100)

	// Act: Add 50 elements and check cardinality estimate
	for i := 0; i < 50; i++ {
		bf.Add(string(rune(i)))
	}

	// Assert: Estimate should be in reasonable range (not exact due to hash collisions)
	estimate := bf.EstimatedCardinality()
	// Very loose bounds: should be between 40 and 70 for 50 actual elements
	assert.Greater(t, estimate, 30, "cardinality estimate %d too low", estimate)
	assert.Less(t, estimate, 100, "cardinality estimate %d too high", estimate)
}

func TestBloomFilterLargeElementCount(t *testing.T) {
	// Arrange
	bf := NewBloomFilter(100000)

	// Act: Add 50000 elements
	for i := 0; i < 50000; i++ {
		bf.Add(strings.Join([]string{"elem", string(rune(i % 256))}, ""))
	}

	// Assert: Query existing patterns
	for i := 0; i < 50000; i++ {
		token := strings.Join([]string{"elem", string(rune(i % 256))}, "")
		assert.True(t, bf.Contains(token), "element should be found")
	}
}

// Query Parser Tests
// ==================

func TestParseQuerySingleToken(t *testing.T) {
	// Arrange
	query := "golang"

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 1)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, OpAnd, result.Tokens[0].Op)
	assert.False(t, result.Tokens[0].IsNot)
	assert.True(t, result.IsSingleToken())
}

func TestParseQueryMultiTokenImplicitAnd(t *testing.T) {
	// Arrange
	query := "golang rust python"

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 3)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, "python", result.Tokens[2].Text)
	for _, tok := range result.Tokens {
		assert.Equal(t, OpAnd, tok.Op)
	}
}

func TestParseQueryExplicitOr(t *testing.T) {
	// Arrange
	query := "golang | rust"

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, OpAnd, result.Tokens[0].Op) // First token is AND by default
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, OpOr, result.Tokens[1].Op)
}

func TestParseQueryNotPrefix(t *testing.T) {
	// Arrange
	query := "-deprecated golang"

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "deprecated", result.Tokens[0].Text)
	assert.True(t, result.Tokens[0].IsNot)
	assert.Equal(t, "golang", result.Tokens[1].Text)
	assert.False(t, result.Tokens[1].IsNot)
}

func TestParseQueryNotExclamation(t *testing.T) {
	// Arrange
	query := "!deprecated golang"

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "deprecated", result.Tokens[0].Text)
	assert.True(t, result.Tokens[0].IsNot)
}

func TestParseQueryComplexExpression(t *testing.T) {
	// Arrange
	query := "golang -deprecated | rust python !boring"

	// Act
	result := ParseQuery(query)

	// Assert: golang AND NOT deprecated OR (rust AND python AND NOT boring)
	require.Len(t, result.Tokens, 5)

	// First part: golang AND -deprecated
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.False(t, result.Tokens[0].IsNot)
	assert.Equal(t, OpAnd, result.Tokens[0].Op)

	assert.Equal(t, "deprecated", result.Tokens[1].Text)
	assert.True(t, result.Tokens[1].IsNot)
	assert.Equal(t, OpAnd, result.Tokens[1].Op)

	// Second part: OR rust AND python AND !boring
	assert.Equal(t, "rust", result.Tokens[2].Text)
	assert.False(t, result.Tokens[2].IsNot)
	assert.Equal(t, OpOr, result.Tokens[2].Op)

	assert.Equal(t, "python", result.Tokens[3].Text)
	assert.False(t, result.Tokens[3].IsNot)
	assert.Equal(t, OpAnd, result.Tokens[3].Op)

	assert.Equal(t, "boring", result.Tokens[4].Text)
	assert.True(t, result.Tokens[4].IsNot)
	assert.Equal(t, OpAnd, result.Tokens[4].Op)
}

func TestParseQueryQuotedToken(t *testing.T) {
	// Arrange
	query := `"hello world" test`

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "hello world", result.Tokens[0].Text)
	assert.Equal(t, "test", result.Tokens[1].Text)
}

func TestParseQueryEmptyQuery(t *testing.T) {
	// Arrange
	query := ""

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 0)
}

func TestParseQueryWhitespaceOnly(t *testing.T) {
	// Arrange
	query := "   \t  \n  "

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 0)
}

func TestParseQueryMultipleSpaces(t *testing.T) {
	// Arrange
	query := "golang    rust     python"

	// Act
	result := ParseQuery(query)

	// Assert: Multiple spaces should be collapsed
	require.Len(t, result.Tokens, 3)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, "python", result.Tokens[2].Text)
}

func TestParseQueryCaseNormalization(t *testing.T) {
	// Arrange
	query := "GOLANG Rust PyThOn"

	// Act
	result := ParseQuery(query)

	// Assert: Tokens should be lowercased
	require.Len(t, result.Tokens, 3)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, "python", result.Tokens[2].Text)
}

func TestQueryExprString(t *testing.T) {
	// Arrange
	query := "golang -deprecated | rust"
	expr := ParseQuery(query)

	// Act
	str := expr.String()

	// Assert: String representation should be readable
	assert.Contains(t, str, "golang")
	assert.Contains(t, str, "-deprecated")
	assert.Contains(t, str, "|")
	assert.Contains(t, str, "rust")
}

func TestQueryExprIsSingleTokenFalseForMulti(t *testing.T) {
	// Arrange
	expr := ParseQuery("golang rust")

	// Act & Assert
	assert.False(t, expr.IsSingleToken())
}

func TestQueryExprIsSingleTokenFalseForNot(t *testing.T) {
	// Arrange
	expr := ParseQuery("-golang")

	// Act & Assert
	assert.False(t, expr.IsSingleToken())
}

func TestQueryExprIsSingleTokenTrueForSimple(t *testing.T) {
	// Arrange
	expr := ParseQuery("golang")

	// Act & Assert
	assert.True(t, expr.IsSingleToken())
}

func TestParseQueryOrKeyword(t *testing.T) {
	// Arrange
	query := "golang OR rust"

	// Act
	result := ParseQuery(query)

	// Assert: OR keyword should work like | operator
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, OpOr, result.Tokens[1].Op)
}

func TestParseQueryConsecutiveOperators(t *testing.T) {
	// Arrange
	query := "golang | | rust"

	// Act
	result := ParseQuery(query)

	// Assert: Consecutive | should be collapsed by fields.Fields
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
	assert.Equal(t, OpOr, result.Tokens[1].Op)
}

func TestParseQueryLeadingTrailingOperators(t *testing.T) {
	// Arrange
	query := "| golang rust |" // Leading/trailing OR is ignored

	// Act
	result := ParseQuery(query)

	// Assert
	require.Len(t, result.Tokens, 2)
	assert.Equal(t, "golang", result.Tokens[0].Text)
	assert.Equal(t, "rust", result.Tokens[1].Text)
}

func TestStripNormalize(t *testing.T) {
	// Test cases for normalization
	tests := []struct {
		input    string
		expected string
	}{
		{"Hello World", "helloworld"},
		{"Test-123", "test123"},
		{"C++", "c"},
		{"Go-Lang!", "golang"},
		{"ALLCAPS", "allcaps"},
		{"!@#$%", ""},
		{"123Numbers456", "123numbers456"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := stripNormalize(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
