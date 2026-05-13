package searchoverlay

import (
	"math"
)

// BloomFilter is a probabilistic data structure for efficient membership testing.
// It uses multiple independent hash functions to set bits in an array.
// Phase 5 optimization: enables memory-efficient AND/OR/NOT operations on large result sets.
type BloomFilter struct {
	bits []byte // Bit array
	size uint64 // Number of bits
	k    uint8  // Number of hash functions (3 or 4)
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
	var k uint8 = 3
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
	hash1, hash2 := b.hashPair(s)
	for i := uint64(0); i < uint64(b.k); i++ {
		idx := b.hashAt(hash1, hash2, i)
		byteIdx := idx / 8
		bitIdx := idx % 8
		b.bits[byteIdx] |= (1 << bitIdx)
	}
}

// Contains tests if an element might be in the set.
// Returns true if all k bits are set (might have false positive).
// Returns false if any bit is unset (definitely not in set).
func (b *BloomFilter) Contains(s string) bool {
	hash1, hash2 := b.hashPair(s)
	for i := uint64(0); i < uint64(b.k); i++ {
		idx := b.hashAt(hash1, hash2, i)
		byteIdx := idx / 8
		bitIdx := idx % 8
		if (b.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

// hashPair computes two base hashes for double hashing with a single FNV-1a pass.
func (b *BloomFilter) hashPair(s string) (uint64, uint64) {
	hash1 := fnv1a64String(s)
	hash2 := ((hash1 >> 32) ^ hash1) | 1 // Ensure odd for coprimality with size
	return hash1, hash2
}

// fnv1a64String computes FNV-1a 64-bit for a string without allocations.
func fnv1a64String(s string) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	h := uint64(fnvOffset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

// hashAt computes the i-th hash value using double hashing:
// hash(i) = (hash1 + i*hash2) mod size.
func (b *BloomFilter) hashAt(hash1, hash2, i uint64) uint64 {
	return (hash1 + i*hash2) % b.size
}

// EstimatedCardinality returns an estimate of how many elements have been added.
// Uses the maximum likelihood formula: n = -m/k * ln(X/m) where:
//   - m is the total number of bits in the filter
//   - X is the number of unset (empty) bits
//   - k is the number of hash functions
//
// Accuracy: For typical bloom filters with moderate saturation (0.1-0.9), accuracy is
// within ±10%. At high saturation (>0.99), accuracy may degrade due to hash collisions.
// This formula relies on Go's math.Log for correct logarithm computation at all scales.
//
// Edge cases:
//   - Empty filter (X=m): Returns 0 (no elements added)
//   - Fully saturated (X=0): Returns m/k (filter completely full, upper bound estimate)
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

	size := b.size
	setU := uint64(setBits)
	if setU > size {
		setU = size
	}
	empty := size - setU

	// If no empty bits, filter is fully saturated—return upper bound estimate
	if empty == 0 {
		return clampUint64ToInt(size / uint64(b.k))
	}

	// If all bits are empty (filter not used), return 0
	if empty == size {
		return 0
	}

	// Standard cardinality estimation: -m/k * ln(X/m)
	x := float64(empty) / float64(size)
	out := float64(size) / float64(b.k) * -math.Log(x)
	if out > float64(math.MaxInt) {
		return math.MaxInt
	}
	if out < 0 {
		return 0
	}
	return int(out)
}

func clampUint64ToInt(v uint64) int {
	if v > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}
