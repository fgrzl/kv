package merkle

import (
	"sync"

	"github.com/zeebo/blake3"
)

// hasherPool reduces allocations in hot hashing paths by reusing blake3 hashers.
var hasherPool = sync.Pool{
	New: func() interface{} { return blake3.New() },
}

// cachedPaddingHash and cachedDeletedHash are precomputed once at init time.
// This avoids recomputing domain-separated constant hashes on every use.
var (
	cachedPaddingHash = computeDomainHash(domainPadding, nil)
	cachedDeletedHash = computeDomainHash(domainDeleted, nil)
)

// ComputeHash computes the BLAKE3-256 hash of user data (leaf payload).
func ComputeHash(data []byte) []byte {
	hash := blake3.Sum256(data)
	return hash[:]
}

// computeDomainHash computes a domain-separated BLAKE3-256 hash.
// Domain separation prevents hash collisions between different node types
// (user leaves, padding, deleted markers, internal nodes).
func computeDomainHash(domain string, data []byte) []byte {
	h := blake3.New()
	h.Write([]byte(domain))
	if data != nil {
		h.Write(data)
	}
	return h.Sum(nil)
}

// deletedHash returns the canonical hash for deleted leaf markers.
func deletedHash() []byte { return cachedDeletedHash }

// hashByteSlices computes the BLAKE3 hash of multiple byte slices concatenated.
// Used for internal node hashing (combining child hashes).
func hashByteSlices(slices [][]byte) []byte {
	h := hasherPool.Get().(*blake3.Hasher)
	h.Reset()
	for _, s := range slices {
		h.Write(s)
	}
	sum := h.Sum(nil)
	hasherPool.Put(h)
	return sum
}
