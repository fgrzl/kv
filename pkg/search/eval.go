package searchoverlay

import (
	"context"
)

// EvaluationMode determines how multi-token queries are evaluated.
type EvaluationMode int

const (
	// EvalModeBloom: Use bloom filters for AND/OR/NOT (fast, ~0.1% FP rate)
	EvalModeBloom EvaluationMode = iota
	// EvalModeMaterialized: Materialize result sets and perform set operations (accurate)
	EvalModeMaterialized
)

// evaluateMultiToken evaluates a multi-token query against the index.
// It returns entity IDs that match the query expression.
// Phase 5: Supports complex boolean expressions on multiple tokens.
func (o *overlay) evaluateMultiToken(ctx context.Context, query QueryExpr, fields map[string]struct{}, limit int) (map[string]map[string]struct{}, error) {
	// Build result set for each token (entity -> matched fields)
	tokenResults := make(map[string]map[string]map[string]struct{}) // token -> entityID -> fields

	// Evaluate each token and collect results
	for _, tok := range query.Tokens {
		postingsByEntity := make(map[string]map[string]struct{})

		token := tok.Text
		firstLetter := partitionKeyForToken(token)

		// Query index for this token with specified fields or all fields
		if len(fields) == 0 {
			if err := o.searchValueIndex(ctx, token, firstLetter, limit, postingsByEntity); err != nil {
				return nil, err
			}
		} else {
			if err := o.searchFieldIndexes(ctx, token, firstLetter, limit, fields, postingsByEntity); err != nil {
				return nil, err
			}
		}

		tokenResults[token] = postingsByEntity
	}

	// Evaluate boolean expression based on query structure
	return o.combineBooleanResults(tokenResults, query), nil
}

// combineBooleanResults applies the boolean query logic to combine token result sets.
// Handles AND, OR, and NOT operators according to Google-style precedence.
func (o *overlay) combineBooleanResults(tokenResults map[string]map[string]map[string]struct{}, query QueryExpr) map[string]map[string]struct{} {
	if len(query.Tokens) == 0 {
		return make(map[string]map[string]struct{})
	}

	var result map[string]map[string]struct{}

	// Process tokens in order, respecting operator precedence
	for i, tok := range query.Tokens {
		set := tokenResults[tok.Text]

		if i == 0 {
			// First token initializes result
			if tok.IsNot {
				// NOT as first operator: has nothing to exclude from, skip it
				continue
			}
			result = copyEntityMap(set)
		} else {
			// Subsequent tokens: apply operator
			if tok.Op == OpAnd {
				if tok.IsNot {
					// AND NOT: exclude
					result = subtractEntityMaps(result, set)
				} else {
					// AND: intersect
					result = intersectEntityMaps(result, set)
				}
			} else if tok.Op == OpOr {
				if tok.IsNot {
					// OR NOT: exclude from union
					// This is unusual: result = result | (nothing - set)
					// Simplify: just continue with result
					continue
				}
				// OR: union
				result = unionEntityMaps(result, set)
			}
		}
	}

	if result == nil {
		result = make(map[string]map[string]struct{})
	}
	return result
}

// Set operation helpers

func copyEntityMap(m map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(m))
	for k, v := range m {
		fieldsCopy := make(map[string]struct{}, len(v))
		for fk := range v {
			fieldsCopy[fk] = struct{}{}
		}
		result[k] = fieldsCopy
	}
	return result
}

func intersectEntityMaps(a, b map[string]map[string]struct{}) map[string]map[string]struct{} {
	if len(a) > len(b) {
		a, b = b, a
	}
	result := make(map[string]map[string]struct{})
	for k, fields := range a {
		if bFields, ok := b[k]; ok {
			// Entity exists in both sets: union the matched fields
			mergedFields := make(map[string]struct{}, len(fields)+len(bFields))
			for f := range fields {
				mergedFields[f] = struct{}{}
			}
			for f := range bFields {
				mergedFields[f] = struct{}{}
			}
			result[k] = mergedFields
		}
	}
	return result
}

func unionEntityMaps(a, b map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(a)+len(b))
	for k, fields := range a {
		fieldsCopy := make(map[string]struct{}, len(fields))
		for f := range fields {
			fieldsCopy[f] = struct{}{}
		}
		result[k] = fieldsCopy
	}
	for k, fields := range b {
		if existing, ok := result[k]; ok {
			// Merge fields
			for f := range fields {
				existing[f] = struct{}{}
			}
		} else {
			fieldsCopy := make(map[string]struct{}, len(fields))
			for f := range fields {
				fieldsCopy[f] = struct{}{}
			}
			result[k] = fieldsCopy
		}
	}
	return result
}

func subtractEntityMaps(a, b map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for k, fields := range a {
		if _, exists := b[k]; !exists {
			// Entity not in b: include it
			fieldsCopy := make(map[string]struct{}, len(fields))
			for f := range fields {
				fieldsCopy[f] = struct{}{}
			}
			result[k] = fieldsCopy
		}
	}
	return result
}
