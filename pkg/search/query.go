package searchoverlay

import (
	"strings"
	"unicode"
)

// QueryOp represents a boolean operator.
type QueryOp int

const (
	OpAnd QueryOp = iota // Implicit in Google-style: "token1 token2" means AND
	OpOr                 // OR: token1 | token2
	OpNot                // NOT: -token or !token
)

// QueryToken represents a single token with its operator context.
type QueryToken struct {
	Text  string // The actual token text (lowercase)
	Op    QueryOp
	IsNot bool // True if this is a NOT token (negation)
}

// QueryExpr represents a parsed multi-token query.
// Phase 5: Supports Google-style syntax for AND/OR/NOT operations.
// Examples:
//
//	"golang rust" → [Token{Text:"golang"}, Token{Text:"rust"}] (implicit AND)
//	"golang | rust" → [Token{Text:"golang", Op:OR}, Token{Text:"rust"}] (OR)
//	"-deprecated golang" → [Token{Text:"deprecated", IsNot:true}, Token{Text:"golang"}] (NOT and AND)
type QueryExpr struct {
	Tokens []QueryToken
}

// ParseQuery parses a query string into a QueryExpr.
// Supports Google-style syntax:
//   - Tokens separated by spaces are implicitly AND'ed
//   - Use "|" or "OR" for explicit OR
//   - Use "-" or "!" prefix for NOT
//   - Words in quotes are treated as single token
//
// Examples:
//
//	"golang rust" → AND both
//	"golang | rust" → OR both
//	"-deprecated golang" → golang AND NOT deprecated
//	"golang -deprecated | rust" → (golang AND NOT deprecated) OR (rust)
func ParseQuery(text string) QueryExpr {
	tokens := tokenizeQuery(text)
	return QueryExpr{Tokens: tokens}
}

// tokenizeQuery splits the query into tokens and applies operators.
func tokenizeQuery(text string) []QueryToken {
	// Custom parser to handle quotes and operators
	parts := smartSplit(text)
	if len(parts) == 0 {
		return nil
	}

	var result []QueryToken
	var currentOp QueryOp = OpAnd

	for _, part := range parts {
		// Skip empty parts
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check for operators
		if part == "|" || strings.EqualFold(part, "or") {
			// Switch to OR mode for next token
			currentOp = OpOr
			continue
		}

		// Check for NOT prefix
		isNot := false
		token := part
		if strings.HasPrefix(token, "-") || strings.HasPrefix(token, "!") {
			isNot = true
			token = token[1:] // Remove prefix
		}

		// Remove quotes if present
		if (strings.HasPrefix(token, "\"") && strings.HasSuffix(token, "\"")) ||
			(strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'")) {
			token = token[1 : len(token)-1]
		}

		// Normalize whitespace/case
		token = strings.ToLower(strings.TrimSpace(token))
		if token == "" {
			continue
		}

		result = append(result, QueryToken{
			Text:  token,
			Op:    currentOp,
			IsNot: isNot,
		})

		// Reset to AND after each token (OR is one-shot for next token)
		if currentOp == OpOr {
			currentOp = OpAnd
		}
	}

	return result
}

// smartSplit intelligently splits a query string respecting quoted strings.
// Unlike strings.Fields, this preserves quoted strings as single tokens.
func smartSplit(text string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range text {
		if (r == '"' || r == '\'') && !inQuote {
			// Start of quote
			inQuote = true
			quoteChar = r
			current.WriteRune(r)
		} else if r == quoteChar && inQuote {
			// End of quote
			inQuote = false
			current.WriteRune(r)
		} else if unicode.IsSpace(r) && !inQuote {
			// Whitespace outside quotes: delimiter
			if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
		} else {
			// Regular character
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// IsSingleToken returns true if the query is a simple single-token query (no complex boolean ops).
func (q QueryExpr) IsSingleToken() bool {
	if len(q.Tokens) != 1 {
		return false
	}
	return !q.Tokens[0].IsNot
}

// String returns a human-readable representation of the query.
func (q QueryExpr) String() string {
	if len(q.Tokens) == 0 {
		return "(empty)"
	}

	var parts []string
	for i, t := range q.Tokens {
		s := t.Text
		if t.IsNot {
			s = "-" + s
		}
		if i > 0 && t.Op == OpOr {
			parts = append(parts, "|", s)
		} else {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " ")
}

// StripNormalize removes non-alphanumeric characters and normalizes case.
// Used during indexing to ensure consistent token comparison.
func stripNormalize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}
