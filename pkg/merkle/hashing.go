package merkle

import (
	"encoding/binary"
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

// computeDomainHash computes a domain-separated BLAKE3-256 hash with length-prefixed
// framing so domains cannot collide via prefix overlap.
// Encoding: uvarint(len(domain)) || domain || data
func computeDomainHash(domain string, data []byte) []byte {
	h := blake3.New()
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(domain)))
	h.Write(lenBuf[:n])
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
