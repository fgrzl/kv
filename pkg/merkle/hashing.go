package merkle

import "github.com/zeebo/blake3"

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
	h.Write(data)
	return h.Sum(nil)
}

// paddingHash returns the canonical hash for padding nodes.
func paddingHash() []byte {
	return computeDomainHash(domainPadding, nil)
}

// deletedHash returns the canonical hash for deleted leaf markers.
func deletedHash() []byte {
	return computeDomainHash(domainDeleted, nil)
}

// hashByteSlices computes the BLAKE3 hash of multiple byte slices concatenated.
// Used for internal node hashing (combining child hashes).
func hashByteSlices(slices [][]byte) []byte {
	h := blake3.New()
	for _, s := range slices {
		h.Write(s)
	}
	return h.Sum(nil)
}
