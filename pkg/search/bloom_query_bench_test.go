package searchoverlay

import (
	"fmt"
	"testing"
)

// BenchmarkBloomFilterAdd measures bloom filter insertion performance.
func BenchmarkBloomFilterAdd(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			bf := NewBloomFilter(size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bf.Add(fmt.Sprintf("token_%d", i%size))
			}
		})
	}
}

// BenchmarkBloomFilterContains measures bloom filter membership testing.
func BenchmarkBloomFilterContains(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			bf := NewBloomFilter(size)

			// Pre-populate with 50% of max size
			for i := 0; i < size/2; i++ {
				bf.Add(fmt.Sprintf("token_%d", i))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Mix of hits and misses
				if i%2 == 0 {
					bf.Contains(fmt.Sprintf("token_%d", i%(size/2))) // likely hit
				} else {
					bf.Contains(fmt.Sprintf("miss_%d", i%size)) // likely miss
				}
			}
		})
	}
}

// BenchmarkbloomFilterEstimateCardinality measures cardinality estimation.
func BenchmarkBloomFilterEstimateCardinality(b *testing.B) {
	bf := NewBloomFilter(10000)

	// Add 5000 elements
	for i := 0; i < 5000; i++ {
		bf.Add(fmt.Sprintf("token_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.EstimatedCardinality()
	}
}

// BenchmarkQueryParse measures query string parsing performance.
func BenchmarkQueryParse(b *testing.B) {
	queries := []string{
		"golang",                               // single token
		"golang rust",                          // implicit AND
		"golang | rust",                        // explicit OR
		"-deprecated golang",                   // NOT operator
		"golang -old | rust python",            // complex query
		`"web development" golang -deprecated`, // quoted phrase
	}

	for _, query := range queries {
		b.Run(fmt.Sprintf("Query_%s", query), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ParseQuery(query)
			}
		})
	}
}

// BenchmarkSmartSplit measures custom string splitting with quote handling.
func BenchmarkSmartSplit(b *testing.B) {
	testCases := []string{
		"golang rust",                      // simple tokens
		`"hello world" test`,               // with quoted string
		"a  b   c",                         // multiple spaces
		`"one" "two" "three"`,              // multiple quoted
		"golang -deprecated | rust python", // operators and tokens
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("Parse_%v_chars", len(tc)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = smartSplit(tc)
			}
		})
	}
}

// BenchmarkQueryExprIsSingleToken measures single token detection.
func BenchmarkQueryExprIsSingleToken(b *testing.B) {
	queries := []string{
		"golang",        // single
		"golang rust",   // multi
		"-golang",       // single with NOT
		"golang | rust", // OR
	}

	exprs := make([]QueryExpr, 0, len(queries))
	for _, q := range queries {
		exprs = append(exprs, ParseQuery(q))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exprs[i%len(exprs)].IsSingleToken()
	}
}

// BenchmarkSetOperations measures performance of map-based set operations.
func BenchmarkSetOperations(b *testing.B) {
	b.Run("IntersectSmall", func(b *testing.B) {
		mapA := make(map[string]map[string]struct{})
		mapB := make(map[string]map[string]struct{})

		// Create small sets with 10 entities each
		for i := 0; i < 10; i++ {
			mapA[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}, "field2": {}}
			mapB[fmt.Sprintf("entity_%d", i%5+i/2)] = map[string]struct{}{"field1": {}}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = intersectEntityMaps(mapA, mapB)
		}
	})

	b.Run("IntersectMedium", func(b *testing.B) {
		mapA := make(map[string]map[string]struct{})
		mapC := make(map[string]map[string]struct{})

		// Create medium sets with 1000 entities each
		for i := 0; i < 1000; i++ {
			mapA[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}, "field2": {}}
			if i%3 == 0 {
				mapC[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}}
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = intersectEntityMaps(mapA, mapC)
		}
	})

	b.Run("UnionSmall", func(b *testing.B) {
		mapA := make(map[string]map[string]struct{})
		mapD := make(map[string]map[string]struct{})

		for i := 0; i < 10; i++ {
			mapA[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}}
			mapD[fmt.Sprintf("entity_%d", i+5)] = map[string]struct{}{"field2": {}}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = unionEntityMaps(mapA, mapD)
		}
	})

	b.Run("UnionLarge", func(b *testing.B) {
		mapA := make(map[string]map[string]struct{})
		mapE := make(map[string]map[string]struct{})

		for i := 0; i < 5000; i++ {
			mapA[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}}
			mapE[fmt.Sprintf("entity_%d", i+2500)] = map[string]struct{}{"field2": {}}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = unionEntityMaps(mapA, mapE)
		}
	})

	b.Run("SubtractSmall", func(b *testing.B) {
		mapA := make(map[string]map[string]struct{})
		mapF := make(map[string]map[string]struct{})

		for i := 0; i < 100; i++ {
			mapA[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}}
			if i%3 == 0 {
				mapF[fmt.Sprintf("entity_%d", i)] = map[string]struct{}{"field1": {}}
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = subtractEntityMaps(mapA, mapF)
		}
	})
}

// BenchmarkCopyEntityMap measures map copying performance.
func BenchmarkCopyEntityMap(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Entities_%d", size), func(b *testing.B) {
			original := make(map[string]map[string]struct{})
			for i := 0; i < size; i++ {
				fields := make(map[string]struct{})
				for j := 0; j < 3; j++ {
					fields[fmt.Sprintf("field_%d", j)] = struct{}{}
				}
				original[fmt.Sprintf("entity_%d", i)] = fields
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = copyEntityMap(original)
			}
		})
	}
}
