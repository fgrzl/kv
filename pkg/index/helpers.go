package index

import (
	"context"
	"fmt"
	"strings"
)

// ParseTerms splits a query string into individual terms.
// Converts to lowercase for case-insensitive search.
func ParseTerms(query string) []string {
	terms := strings.Fields(strings.ToLower(query))
	var result []string
	for _, t := range terms {
		if t != "" {
			result = append(result, t)
		}
	}
	return result
}

// QueryBuilder provides a fluent API for building search queries.
type QueryBuilder struct {
	terms []string
	multi bool
	ctx   context.Context
	idx   InvertedIndex
	err   error
}

// NewQueryBuilder creates a new query builder for the given index.
func NewQueryBuilder(ctx context.Context, idx InvertedIndex) *QueryBuilder {
	return &QueryBuilder{ctx: ctx, idx: idx}
}

// Term adds a single term to the query.
func (qb *QueryBuilder) Term(term string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if term != "" {
		qb.terms = append(qb.terms, term)
	}
	return qb
}

// Terms adds multiple terms to the query.
func (qb *QueryBuilder) Terms(terms ...string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	for _, t := range terms {
		if t != "" {
			qb.terms = append(qb.terms, t)
		}
	}
	return qb
}

// AllTerms searches for documents matching all terms (AND operation).
func (qb *QueryBuilder) AllTerms() *QueryBuilder {
	qb.multi = true
	return qb
}

// Execute runs the query and returns results.
func (qb *QueryBuilder) Execute() ([]SearchResult, error) {
	if qb.err != nil {
		return nil, qb.err
	}

	if len(qb.terms) == 0 {
		return []SearchResult{}, nil
	}

	if len(qb.terms) == 1 && !qb.multi {
		return qb.idx.Search(qb.ctx, qb.terms[0])
	}

	return qb.idx.SearchMulti(qb.ctx, qb.terms)
}

// SummaryStats provides statistics about an inverted index.
type SummaryStats struct {
	DocumentCount  int
	TermCount      int
	AvgTermsPerDoc float64
}

// Stats returns summary statistics for the index.
func Stats(ctx context.Context, idx InvertedIndex) (*SummaryStats, error) {
	docCount, err := idx.DocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get document count: %w", err)
	}

	termCount, err := idx.TermCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get term count: %w", err)
	}

	avgTermsPerDoc := 0.0
	if docCount > 0 {
		avgTermsPerDoc = float64(termCount) / float64(docCount)
	}

	return &SummaryStats{
		DocumentCount:  docCount,
		TermCount:      termCount,
		AvgTermsPerDoc: avgTermsPerDoc,
	}, nil
}
