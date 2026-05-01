// Package merkle implements a persistent, index-based Merkle tree for incremental catalog verification.
//
// The overlay is split across smaller files:
// - state.go: core types and configuration
// - api.go: public tree operations
// - build_diff.go: bulk-build and diff traversal helpers
// - hashing.go, storage.go, metadata.go, recompute.go, leaf_reads.go, batch_mutations.go: shared helpers
package merkle
