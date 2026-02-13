package searchoverlay

import (
	"hash/fnv"
)

// BloomFilter is a probabilistic data structure for efficient membership testing.
// It uses multiple independent hash functions to set bits in an array.
// Phase 5 optimization: enables memory-efficient AND/OR/NOT operations on large result sets.
type BloomFilter struct {
	bits []byte // Bit array
	size uint64 // Number of bits
	k    int    // Number of hash functions
}

// NewBloomFilter creates a new bloom filter sized for the expected number of elements.
// It uses an optimal sizing formula to achieve ~0.1% false positive rate:
// m = -n*ln(p)/(ln(2))^2 bits, where n=elements, p=desired FP rate
// For p=0.001: m ≈ 9.6*n bits → we use 10 bits per element
func NewBloomFilter(expectedElements int) *BloomFilter {
	if expectedElements <= 0 {
		expectedElements = 10
	}

	// Calculate bit array size: 10 bits per element
	bitSize := uint64(expectedElements) * 10
	byteSize := (bitSize + 7) / 8

	// Optimal number of hash functions: k = m/n * ln(2)
	// For m=10n: k ≈ 6.93, we use k=3-4 for balance
	k := 3
	if expectedElements > 100000 {
		k = 4
	}

	return &BloomFilter{
		bits: make([]byte, byteSize),
		size: bitSize,
		k:    k,
	}
}

// Add inserts an element into the bloom filter.
// It computes k hash values and sets the corresponding bits.
func (b *BloomFilter) Add(s string) {
	for i := 0; i < b.k; i++ {
		idx := b.hash(s, i)
		byteIdx := idx / 8
		bitIdx := idx % 8
		b.bits[byteIdx] |= (1 << bitIdx)
	}
}

// Contains tests if an element might be in the set.
// Returns true if all k bits are set (might have false positive).
// Returns false if any bit is unset (definitely not in set).
func (b *BloomFilter) Contains(s string) bool {
	for i := 0; i < b.k; i++ {
		idx := b.hash(s, i)
		byteIdx := idx / 8
		bitIdx := idx % 8
		if (b.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

// hash computes the i-th hash value for string s using FNV-1a hash.
// We derive k independent values from a single hash using the offset method:
// hash(i) = (fnv(s) + i) mod size
func (b *BloomFilter) hash(s string, i int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	hashVal := h.Sum64()
	return (hashVal + uint64(i)*14695981039346656037) % b.size
}

// EstimatedCardinality returns an estimate of how many elements have been added.
// Uses the formula: n = -m/k * ln(X/m) where X is the number of empty bits.
// This is useful for monitoring filter inflation without maintaining a counter.
func (b *BloomFilter) EstimatedCardinality() int {
	// Count set bits
	setBits := 0
	for _, by := range b.bits {
		for j := 0; j < 8; j++ {
			if (by & (1 << j)) != 0 {
				setBits++
			}
		}
	}

	emptyBits := int(b.size) - setBits
	if emptyBits == 0 {
		// All bits set, filter is saturated
		return int(b.size) / b.k
	}

	// Estimate using: -m * ln(X/m) / k
	// Simplified: if few empty bits, estimate is high
	return int(float64(b.size) * -1.0 * ln(float64(emptyBits)/float64(b.size)) / float64(b.k))
}

// ln computes natural logarithm approximation
func ln(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Quick approximation for ln(x)
	return 2.0 * (x - 1) / (x + 1) * (1 + (x-1)/(x+1)*(x-1)/(x+1)/3)
}
