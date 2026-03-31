// Package merkle implements a persistent, index-based Merkle tree for incremental catalog verification.
//
// # PURPOSE
//
// This Merkle tree serves as a derived index for an event-sourced catalog system, enabling:
//   - Incremental verification: surgical updates (AddLeaf, UpdateLeaf, RemoveLeaf) modify only O(log N) nodes
//   - Efficient diff detection: compare root hashes to detect catalog divergence
//   - Deterministic snapshots: identical leaf sequences always produce identical root hashes
//
// # WHAT THIS IS NOT
//
// This is NOT a content-addressed data structure like a Merkle DAG or IPFS. It is an INDEX:
//   - Leaf order matters: leaves[0], leaves[1], ... must be deterministic and identical across replicas
//   - Leaf indices are stable: removing a leaf does NOT shift subsequent indices (soft delete only)
//   - No automatic rebalancing: tree structure is determined solely by leaf count and branching factor
//
// # CRITICAL INVARIANT: DETERMINISTIC LEAF ORDER
//
// The catalog discovery service and application servers MUST produce identical leaf sequences.
// Leaf order is determined by entity enumeration order (e.g., lexicographic sort by entity ID).
// ANY difference in leaf order will produce different root hashes and false-positive diffs.
//
// # INCREMENTAL UPDATES
//
// Surgical updates modify only the path from the changed leaf to the root:
//   - UpdateLeaf: O(log N) - recompute parent, grandparent, ..., root
//   - AddLeaf: O(log N) - may add padding, may grow tree height by 1
//   - RemoveLeaf: O(log N) - soft delete (replaces leaf with deletion marker)
//
// Adding a leaf that changes tree height (e.g., 15→16 with branching=2) requires recomputing
// all internal nodes, but this is rare and still faster than full rebuild for large catalogs.
//
// # FULL REBUILD
//
// Full rebuilds (Build) are allowed ONLY when:
//   - Initializing a new stage/space
//   - Manual recovery after corruption detection
//   - Schema migration or branching factor change
//
// Build() is NOT automatically triggered by surgical updates.
//
// # CONCURRENCY
//
// This implementation assumes a single-writer model. Concurrent writes to the same stage/space
// WILL corrupt the tree. Use external synchronization (e.g., leader election, distributed lock).
//
// # STORAGE LAYOUT
//
// Keys: merkle/{stage}/{space}/{level}/{index}
//   - level=0: leaf nodes (entity hashes)
//   - level=1..N: internal nodes (parent hashes)
//   - level=root: root hash (optimization for GetRootHash without probing max level)
//   - level=meta: metadata (leaf count, max level)
//
// The rootKey is a read optimization: instead of probing storage to find max level,
// we store a copy of the root hash at a well-known key. The authoritative root is
// always the hash at (maxLevel, index=0).
//
// # METADATA CONSISTENCY
//
// Metadata (leafCount, maxLevel, rootKey) is advisory and derived from leaf structure.
// On crash between leaf write and metadata update, metadata may be stale but can always
// be reconstructed via recomputeAllInternalNodes(). The authoritative tree structure is
// defined by the leaves themselves, not the cached metadata.
package merkle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/zeebo/blake3"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/merkle")

// leafPool reduces allocations for Leaf structs during JSON unmarshaling in hot paths.
// Used in getHash() to avoid per-read allocations during tree traversal.
var leafPool = sync.Pool{
	New: func() interface{} {
		return &Leaf{}
	},
}

// Constants for internal keys and metadata.
const (
	defaultBatchSize = 500_000
	metaPrefix       = "meta"
	leafCountKey     = "leafcount"
	maxLevelKey      = "maxlevel"
	rootKey          = "root"
)

// Domain-separated hash prefixes for synthetic nodes.
// These ensure padding hashes, deleted markers, and internal node hashes
// are in separate hash domains and cannot collide with user data hashes.
const (
	domainPadding = "merkle:padding:v1"
	domainDeleted = "merkle:deleted:v1"
)

// Branch is an in-memory Merkle node (for tree construction only).
type Branch struct {
	Hash  []byte
	Level int
	Index int
}

// Leaf is a persisted Merkle leaf payload.
type Leaf struct {
	Ref  string `json:"ref"`
	Hash []byte `json:"hash"`
}

// Tree implements a persistent Merkle tree for entity snapshots.
// It provides both bulk operations (Build) and surgical updates (AddLeaf, UpdateLeaf, RemoveLeaf).
//
// CONCURRENCY: This implementation is NOT thread-safe. Concurrent modifications
// to the same tree (stage/space) will result in data corruption. Use external
// synchronization (e.g., mutex) if concurrent access is required.
type Tree struct {
	store     kv.KV
	branching int
	batchSize int
	// childBuf is reused across calls to getChildHashes to eliminate repeated allocations.
	// CONCURRENCY: SAFE because Tree assumes single-writer model (documented above).
	// NOT safe for concurrent use - requires external synchronization if needed.
	childBuf [][]byte
}

type NodePosition struct {
	Level int
	Index int
}

var (
	// rootRef is a sentinel NodePosition representing the root node.
	// Level=-1 signals "unknown level, determine from storage".
	rootRef = NodePosition{Level: -1, Index: 0}

	// Exported errors
	ErrEmptyLeaves      = errors.New("no leaves provided")
	ErrInvalidBranching = errors.New("branching factor must be at least 2")
	ErrInvalidStage     = errors.New("stage cannot be empty")
	ErrInvalidSpace     = errors.New("space cannot be empty")
	ErrInvalidIndex     = errors.New("leaf index must be non-negative")
	ErrInvalidLeaf      = errors.New("leaf hash cannot be nil")

	// Internal invariant violations (should never happen in correct usage)
	errCorruptedMetadata = errors.New("corrupted metadata: invariant violation")
	errInvalidTreeState  = errors.New("invalid tree state: structural invariant violated")
)

// Option configures a Tree.
type Option func(*Tree)

func WithBranching(n int) Option {
	return func(m *Tree) {
		if n >= 2 {
			m.branching = n
		}
	}
}

func WithBatchSize(size int) Option {
	return func(m *Tree) {
		if size > 0 {
			m.batchSize = size
		}
	}
}

// NewTree constructs a persistent Merkle tree for snapshots.
// INVARIANT: branching factor must be >= 2 for valid tree structure.
func NewTree(store kv.KV, opts ...Option) *Tree {
	if store == nil {
		panic("NewTree: store cannot be nil")
	}
	m := &Tree{
		store:     store,
		branching: 2,
		batchSize: defaultBatchSize,
	}
	for _, o := range opts {
		o(m)
	}
	// Validate branching factor
	if m.branching < 2 {
		panic(fmt.Sprintf("NewTree: branching factor %d < 2", m.branching))
	}
	if m.batchSize < 1 {
		panic(fmt.Sprintf("NewTree: batch size %d < 1", m.batchSize))
	}
	// Pre-allocate reusable buffer
	m.childBuf = make([][]byte, 0, m.branching)
	return m
}

// Build builds and persists a Merkle tree from a leaf enumerator.
func (m *Tree) Build(ctx context.Context, stage, space string, leaves enumerators.Enumerator[Leaf]) error {
	ctx, span := tracer.Start(ctx, "merkle.Build",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
		))
	defer span.End()

	slog.DebugContext(ctx, "starting Merkle tree build", "stage", stage, "space", space)
	if err := m.pruneOldNodes(ctx, stage, space); err != nil {
		err = fmt.Errorf("prune old nodes for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to prune old nodes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	currNodes, err := m.persistLeaves(ctx, stage, space, leaves)
	if err != nil {
		err = fmt.Errorf("persist leaves for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to persist leaves", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(currNodes) == 0 {
		slog.DebugContext(ctx, "Merkle tree build completed with no leaves", "stage", stage, "space", space)
		span.SetAttributes(attribute.Int("leaves", 0))
		return nil
	}

	// Store actual leaf count BEFORE padding
	actualLeafCount := len(currNodes)

	currNodes, err = m.padLeaves(ctx, stage, space, currNodes)
	if err != nil {
		slog.ErrorContext(ctx, "failed to pad leaves", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	currNodes, err = m.persistInternalLevels(ctx, stage, space, currNodes)
	if err != nil {
		slog.ErrorContext(ctx, "failed to persist internal levels", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := m.persistRoot(ctx, stage, space, currNodes); err != nil {
		slog.ErrorContext(ctx, "failed to persist root", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Store actual leaf count metadata
	if err := m.setLeafCountMetadata(ctx, stage, space, actualLeafCount); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Store max level metadata (optimization: avoids probing storage in GetRootHash)
	maxLevel := m.calculateMaxLevel(len(currNodes))
	if err := m.setMaxLevelMetadata(ctx, stage, space, maxLevel); err != nil {
		slog.ErrorContext(ctx, "failed to persist max level", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "Merkle tree build completed", "stage", stage, "space", space, "leaves", actualLeafCount)
	span.SetAttributes(attribute.Int("leaves", actualLeafCount))
	return nil
}

// Diff returns leaves that are present in 'curr' but not in 'prev' (one-way diff).
func (m *Tree) Diff(ctx context.Context, prev, curr, space string) enumerators.Enumerator[Leaf] {
	return m.diffNode(ctx, prev, curr, space, rootRef)
}

// SymmetricDiff returns added and removed leaves between two trees (curr vs prev, prev vs curr).
func (m *Tree) SymmetricDiff(ctx context.Context, prev, curr, space string) enumerators.Enumerator[Leaf] {

	visited := make(map[string]struct{})

	return enumerators.Chain(

		// prev -> curr track visited
		enumerators.Map(
			m.diffNode(ctx, prev, curr, space, rootRef),
			func(leaf Leaf) (Leaf, error) {
				visited[leaf.Ref] = struct{}{}
				return leaf, nil
			}),

		// curr -> prev filter visited
		enumerators.FilterMap(
			m.diffNode(ctx, curr, prev, space, rootRef),
			func(leaf Leaf) (Leaf, bool, error) {
				_, ok := visited[leaf.Ref]
				return leaf, !ok, nil
			}))
}

// GetRootHash returns the Merkle root hash and underlying value for a given stage/space.
func (m *Tree) GetRootHash(ctx context.Context, stage, space string) ([]byte, []byte, error) {
	return m.getHash(ctx, stage, space, rootRef)
}

// Prune removes all Merkle nodes for the given stage and space.
func (m *Tree) Prune(ctx context.Context, stage, space string) error {
	slog.DebugContext(ctx, "pruning Merkle tree", "stage", stage, "space", space)
	partition := lexkey.Encode("merkle", stage, space)
	rangeKey := lexkey.NewRangeKeyFull(partition)
	err := m.store.RemoveRange(ctx, rangeKey)
	if err != nil {
		slog.ErrorContext(ctx, "failed to prune Merkle tree", "stage", stage, "space", space, "err", err)
		return err
	}
	slog.DebugContext(ctx, "Merkle tree pruned", "stage", stage, "space", space)
	return nil
}

// GetLeafCount returns the number of actual leaves in the tree (excluding padding).
// Note: Soft-deleted leaves (via RemoveLeaf) are still counted.
func (m *Tree) GetLeafCount(ctx context.Context, stage, space string) (int, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return 0, err
	}
	return m.getLeafCountMetadata(ctx, stage, space)
}

// GetLeaf retrieves a specific leaf by index.
// Returns the leaf data, or an error if the index is out of bounds.
func (m *Tree) GetLeaf(ctx context.Context, stage, space string, leafIndex int) (Leaf, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return Leaf{}, err
	}
	if leafIndex < 0 {
		return Leaf{}, ErrInvalidIndex
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return Leaf{}, err
	}
	if leafIndex >= leafCount {
		return Leaf{}, fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}

	pkVal := pk(stage, space, 0, leafIndex)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return Leaf{}, fmt.Errorf("failed to get leaf: %w", err)
	}
	if item == nil {
		return Leaf{}, fmt.Errorf("leaf not found at index %d", leafIndex)
	}

	var leaf Leaf
	if err := json.Unmarshal(item.Value, &leaf); err != nil {
		return Leaf{}, fmt.Errorf("failed to unmarshal leaf: %w", err)
	}
	return leaf, nil
}

// EnumerateLeaves returns a lazy enumerator over all leaves in the tree.
// The enumeration includes soft-deleted leaves (with empty Ref field).
func (m *Tree) EnumerateLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	if err := validateStageSpace(stage, space); err != nil {
		return enumerators.Error[Leaf](err)
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return enumerators.Error[Leaf](err)
	}

	return &leafEnumerator{
		ctx:       ctx,
		tree:      m,
		stage:     stage,
		space:     space,
		leafCount: leafCount,
		index:     -1,
	}
}

// leafEnumerator implements enumerators.Enumerator[Leaf]
type leafEnumerator struct {
	ctx       context.Context
	tree      *Tree
	stage     string
	space     string
	leafCount int
	index     int
	current   Leaf
	err       error
}

func (e *leafEnumerator) MoveNext() bool {
	e.index++
	if e.index >= e.leafCount {
		return false
	}
	leaf, err := e.tree.GetLeaf(e.ctx, e.stage, e.space, e.index)
	if err != nil {
		e.err = err
		return false
	}
	e.current = leaf
	return true
}

func (e *leafEnumerator) Current() (Leaf, error) {
	return e.current, e.err
}

func (e *leafEnumerator) Err() error {
	return e.err
}

func (e *leafEnumerator) Dispose() {
	// No resources to clean up
}

// UpdateLeaf updates a single leaf and recomputes the path to the root.
// This is a surgical update - only the affected path is recomputed, not the entire tree.
func (m *Tree) UpdateLeaf(ctx context.Context, stage, space string, leafIndex int, newLeaf Leaf) error {
	if err := validateStageSpace(stage, space); err != nil {
		return err
	}
	if leafIndex < 0 {
		return ErrInvalidIndex
	}
	if newLeaf.Hash == nil {
		return ErrInvalidLeaf
	}

	ctx, span := tracer.Start(ctx, "merkle.UpdateLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("leafIndex", leafIndex),
		))
	defer span.End()

	slog.DebugContext(ctx, "updating leaf", "stage", stage, "space", space, "leafIndex", leafIndex)

	// Persist the updated leaf
	val, err := json.Marshal(newLeaf)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	pkVal := pk(stage, space, 0, leafIndex)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val}); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Recompute path to root
	if err := m.recomputePathToRoot(ctx, stage, space, leafIndex); err != nil {
		slog.ErrorContext(ctx, "failed to recompute path to root", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "leaf updated", "stage", stage, "space", space, "leafIndex", leafIndex)
	return nil
}

// AddLeaf appends a new leaf to the tree and updates affected nodes.
// Returns the index at which the leaf was added.
//
// PERFORMANCE: O(log N) updates in most cases. When adding the leaf increases tree height
// (e.g., going from 15 to 16 leaves with branching=2), all internal nodes are recomputed,
// but this is still faster than full rebuild for large trees and happens rarely.
//
// INVARIANT: Leaf indices are append-only and stable. New leaf always gets index = current leaf count.
func (m *Tree) AddLeaf(ctx context.Context, stage, space string, newLeaf Leaf) (int, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return 0, err
	}
	if newLeaf.Hash == nil {
		return 0, ErrInvalidLeaf
	}

	ctx, span := tracer.Start(ctx, "merkle.AddLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
		))
	defer span.End()

	slog.DebugContext(ctx, "adding leaf", "stage", stage, "space", space)

	// Get current leaf count
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Persist the new leaf
	val, err := json.Marshal(newLeaf)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	pkVal := pk(stage, space, 0, leafCount)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val}); err != nil {
		slog.ErrorContext(ctx, "failed to persist leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Check if we need to add padding
	newLeafCount := leafCount + 1
	if newLeafCount%m.branching != 0 {
		// Calculate target padded count
		paddedCount := ((newLeafCount / m.branching) + 1) * m.branching
		// Add padding nodes if needed
		if err := m.ensurePaddingExists(ctx, stage, space, newLeafCount, paddedCount); err != nil {
			slog.ErrorContext(ctx, "failed to add padding", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return 0, err
		}
	}

	// Update leaf count metadata BEFORE recomputing nodes
	if err := m.setLeafCountMetadata(ctx, stage, space, newLeafCount); err != nil {
		slog.ErrorContext(ctx, "failed to update leaf count", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	// Recompute affected paths
	if err := m.recomputeAffectedNodes(ctx, stage, space, leafCount, newLeafCount); err != nil {
		slog.ErrorContext(ctx, "failed to recompute affected nodes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	slog.DebugContext(ctx, "leaf added", "stage", stage, "space", space, "leafIndex", leafCount)
	span.SetAttributes(attribute.Int("leafIndex", leafCount))
	return leafCount, nil
}

// RemoveLeaf marks a leaf as deleted (soft delete) and recomputes the path.
//
// IMPORTANT: This is a soft delete. The leaf is replaced with a deletion marker
// but the leaf count remains unchanged, and the leaf index is preserved.
// To check if a leaf is deleted, verify if its Ref field is empty.
//
// Rationale: Hard deletion would require reindexing all subsequent leaves,
// invalidating any external references to leaf indices. Soft delete maintains
// index stability at the cost of some storage overhead.
func (m *Tree) RemoveLeaf(ctx context.Context, stage, space string, leafIndex int) error {
	if err := validateStageSpace(stage, space); err != nil {
		return err
	}
	if leafIndex < 0 {
		return ErrInvalidIndex
	}

	ctx, span := tracer.Start(ctx, "merkle.RemoveLeaf",
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("space", space),
			attribute.Int("leafIndex", leafIndex),
		))
	defer span.End()

	slog.DebugContext(ctx, "removing leaf", "stage", stage, "space", space, "leafIndex", leafIndex)

	// INVARIANT: Soft delete preserves leaf indices. Hard delete would require reindexing
	// all subsequent leaves, breaking external references and catalog diffs.
	// Create a deleted marker leaf with domain-separated hash
	deletedLeaf := Leaf{
		Ref:  "",
		Hash: deletedHash(),
	}

	// Update with the deleted marker
	if err := m.UpdateLeaf(ctx, stage, space, leafIndex, deletedLeaf); err != nil {
		slog.ErrorContext(ctx, "failed to remove leaf", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx, "leaf removed", "stage", stage, "space", space, "leafIndex", leafIndex)
	return nil
}

// --- Build Helper Methods ---

func (m *Tree) pruneOldNodes(ctx context.Context, stage, space string) error {
	merklePrefix := lexkey.Encode("merkle", stage, space)
	merkleRange := lexkey.NewRangeKey(merklePrefix, lexkey.Empty, lexkey.Empty)
	return m.store.RemoveRange(ctx, merkleRange)
}

func (m *Tree) persistLeaves(ctx context.Context, stage, space string, leaves enumerators.Enumerator[Leaf]) ([]Branch, error) {
	// Pre-allocate with reasonable capacity
	currNodes := make([]Branch, 0, 1000)
	batch := make([]*kv.BatchItem, 0, m.batchSize)

	index := 0
	writeLeafNode := func(idx int, val []byte) error {
		pk := pk(stage, space, 0, idx)
		batch = append(batch, &kv.BatchItem{PK: pk, Value: val, Op: kv.Put})
		if len(batch) >= m.batchSize {
			if err := m.store.Batch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
		return nil
	}

	if err := enumerators.ForEach(leaves, func(leaf Leaf) error {
		val, err := json.Marshal(leaf)
		if err != nil {
			return err
		}
		if err := writeLeafNode(index, val); err != nil {
			return err
		}
		currNodes = append(currNodes, Branch{Hash: leaf.Hash, Level: 0, Index: index})
		index++
		return nil
	}); err != nil {
		return nil, err
	}

	if len(batch) > 0 {
		if err := m.store.Batch(ctx, batch); err != nil {
			return nil, err
		}
	}
	return currNodes, nil
}

func (m *Tree) padLeaves(ctx context.Context, stage, space string, currNodes []Branch) ([]Branch, error) {
	if len(currNodes)%m.branching == 0 {
		return currNodes, nil
	}
	batch := make([]*kv.BatchItem, 0, m.batchSize)
	writePadding := func(idx int, val []byte) error {
		pk := pk(stage, space, 0, idx)
		batch = append(batch, &kv.BatchItem{PK: pk, Value: val, Op: kv.Put})
		if len(batch) >= m.batchSize {
			if err := m.store.Batch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	}
	for len(currNodes)%m.branching != 0 {
		// INVARIANT: Padding nodes use domain-separated hashes to prevent collision with user data
		hash := paddingHash()
		leaf := Leaf{Hash: hash}
		val, err := json.Marshal(leaf)
		if err != nil {
			return nil, err
		}
		if err := writePadding(len(currNodes), val); err != nil {
			return nil, err
		}
		currNodes = append(currNodes, Branch{Hash: hash, Level: 0, Index: len(currNodes)})
	}
	if len(batch) > 0 {
		if err := m.store.Batch(ctx, batch); err != nil {
			return nil, err
		}
	}
	return currNodes, nil
}

func (m *Tree) persistInternalLevels(ctx context.Context, stage, space string, nodes []Branch) ([]Branch, error) {
	level := 1
	for len(nodes) > 1 {
		var (
			next  []Branch
			batch []*kv.BatchItem
		)
		next, batch = m.hashNodeLevel(stage, space, nodes, level)
		if err := m.commitBatchIfNeeded(ctx, batch); err != nil {
			return nil, err
		}
		nodes = next
		level++
	}
	return nodes, nil
}

func (m *Tree) hashNodeLevel(stage, space string, nodes []Branch, level int) ([]Branch, []*kv.BatchItem) {
	numGroups := (len(nodes) + m.branching - 1) / m.branching

	// Pre-allocate slices for better performance
	next := make([]Branch, 0, numGroups)
	batch := make([]*kv.BatchItem, 0, numGroups)

	for i := 0; i < len(nodes); i += m.branching {
		end := min(i+m.branching, len(nodes))
		sum := hashNodes(nodes[i:end])
		pkVal := pk(stage, space, level, i/m.branching)
		batch = append(batch, &kv.BatchItem{PK: pkVal, Value: sum, Op: kv.Put})
		next = append(next, Branch{Hash: sum, Level: level, Index: i / m.branching})
	}
	return next, batch
}

func (m *Tree) commitBatchIfNeeded(ctx context.Context, batch []*kv.BatchItem) error {
	if len(batch) == 0 {
		return nil
	}
	return m.store.Batch(ctx, batch)
}

func hashNodes(nodes []Branch) []byte {
	hashes := make([][]byte, len(nodes))
	for i, n := range nodes {
		hashes[i] = n.Hash
	}
	return hashByteSlices(hashes)
}

func (m *Tree) persistRoot(ctx context.Context, stage, space string, nodes []Branch) error {
	if len(nodes) != 1 {
		return fmt.Errorf("persistRoot: expected 1 node, got %d", len(nodes))
	}
	root := nodes[0]
	pkVal := pk(stage, space, rootKey, "")
	return m.store.Put(ctx, &kv.Item{PK: pkVal, Value: root.Hash})
}

func (m *Tree) diffNode(ctx context.Context, prev, curr, space string, ref NodePosition) enumerators.Enumerator[Leaf] {
	prevHash, _, errPrev := m.getHash(ctx, prev, space, ref)
	currHash, currVal, errCurr := m.getHash(ctx, curr, space, ref)

	if errPrev != nil || errCurr != nil {
		var combined error
		if errPrev != nil && errCurr != nil {
			combined = fmt.Errorf("get hash errors: prev=%v curr=%v", errPrev, errCurr)
		} else if errPrev != nil {
			combined = fmt.Errorf("get hash prev: %w", errPrev)
		} else {
			combined = fmt.Errorf("get hash curr: %w", errCurr)
		}
		return enumerators.Error[Leaf](combined)
	}

	// Handle root-level empty tree edge cases
	if ref == rootRef {
		// Both trees empty
		if prevHash == nil && currHash == nil {
			return enumerators.Empty[Leaf]()
		}
		// prev empty, curr has data - return all current leaves
		if prevHash == nil && currHash != nil {
			return m.scanLeaves(ctx, curr, space)
		}
		// curr empty, prev has data - one-way diff emits nothing
		if currHash == nil && prevHash != nil {
			return enumerators.Empty[Leaf]()
		}
	}

	// Early exit if identical hashes (use bytes.Equal for correctness)
	if prevHash != nil && currHash != nil && bytes.Equal(prevHash, currHash) {
		return enumerators.Empty[Leaf]()
	}

	// Leaf level: return changed or new leaf
	if ref.Level == 0 {
		if currHash != nil && (prevHash == nil || !bytes.Equal(prevHash, currHash)) {
			return decodeLeafEnumerator(currVal)
		}
		return enumerators.Empty[Leaf]()
	}

	// Internal node: walk subtree if one side is nil
	if prevHash == nil && currHash != nil {
		return m.walkSubtree(ctx, curr, space, ref)
	}
	if currHash == nil {
		return enumerators.Empty[Leaf]() // Nothing to emit for one-way diff
	}

	// If unknown level, determine maxLevel from storage
	if ref.Level < 0 {
		ref.Level = m.findMaxLevel(ctx, curr, space)
		if ref.Level < 0 {
			return enumerators.Empty[Leaf]()
		}
	}

	// Recurse on child nodes with differences
	chunks := m.diffChildNodes(ctx, prev, curr, space, ref)
	if len(chunks) == 0 {
		return enumerators.Empty[Leaf]()
	}
	return enumerators.FlatMap(enumerators.Slice(chunks), func(child NodePosition) enumerators.Enumerator[Leaf] {
		return m.diffNode(ctx, prev, curr, space, child)
	})
}

func decodeLeafEnumerator(val []byte) enumerators.Enumerator[Leaf] {
	if val == nil {
		return enumerators.Empty[Leaf]()
	}
	var leaf Leaf
	if err := json.Unmarshal(val, &leaf); err == nil && leaf.Ref != "" {
		return enumerators.Slice([]Leaf{leaf})
	}
	return enumerators.Empty[Leaf]()
}

func (m *Tree) diffChildNodes(ctx context.Context, prev, curr, space string, ref NodePosition) []NodePosition {
	chunks := make([]NodePosition, 0, m.branching)
	for i := 0; i < m.branching; i++ {
		child := NodePosition{Level: ref.Level - 1, Index: ref.Index*m.branching + i}
		prevChildHash, _, _ := m.getHash(ctx, prev, space, child)
		currChildHash, _, _ := m.getHash(ctx, curr, space, child)
		if prevChildHash != nil || currChildHash != nil {
			chunks = append(chunks, child)
		}
	}
	return chunks
}

func (m *Tree) findMaxLevel(ctx context.Context, stage, space string) int {
	for h := 0; ; h++ {
		pkVal := pk(stage, space, h, 0)
		item, err := m.store.Get(ctx, pkVal)
		if err != nil || item == nil {
			return h - 1
		}
	}
}

func (m *Tree) walkSubtree(ctx context.Context, stage, space string, ref NodePosition) enumerators.Enumerator[Leaf] {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = pk(stage, space, rootKey, "")
	} else {
		pkVal = pk(stage, space, ref.Level, ref.Index)
	}
	item, err := m.store.Get(ctx, pkVal)
	if err != nil || item == nil {
		return enumerators.Empty[Leaf]()
	}
	if ref.Level == 0 {
		return decodeLeafEnumerator(item.Value)
	}
	chunks := make([]NodePosition, 0, m.branching)
	for i := 0; i < m.branching; i++ {
		chunks = append(chunks, NodePosition{Level: ref.Level - 1, Index: ref.Index*m.branching + i})
	}
	return enumerators.FlatMap(enumerators.Slice(chunks), func(child NodePosition) enumerators.Enumerator[Leaf] {
		return m.walkSubtree(ctx, stage, space, child)
	})
}

// scanLeaves enumerates all non-deleted leaves for a given stage/space.
// Used for empty/non-empty diff edge cases. Returns a lazy enumerator.
func (m *Tree) scanLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	return enumerators.FilterMap(
		m.EnumerateLeaves(ctx, stage, space),
		func(leaf Leaf) (Leaf, bool, error) {
			// Only include non-deleted leaves
			return leaf, leaf.Ref != "", nil
		},
	)
}

// --- Storage Helper ---

func (m *Tree) getHash(ctx context.Context, stage, space string, ref NodePosition) ([]byte, []byte, error) {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = pk(stage, space, rootKey, "")
	} else {
		pkVal = pk(stage, space, ref.Level, ref.Index)
	}
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return nil, nil, fmt.Errorf("store get: %w", err)
	}
	if item == nil {
		return nil, nil, nil
	}
	if ref.Level == 0 {
		// Use pooled Leaf to reduce allocations
		leaf := leafPool.Get().(*Leaf)
		defer func() {
			// Clear the Leaf before returning to pool
			*leaf = Leaf{}
			leafPool.Put(leaf)
		}()
		if err := json.Unmarshal(item.Value, leaf); err != nil {
			return nil, nil, fmt.Errorf("unmarshal leaf: %w", err)
		}
		return leaf.Hash, item.Value, nil
	}
	return item.Value, item.Value, nil
}

// validateStageSpace validates that stage and space are non-empty.
func validateStageSpace(stage, space string) error {
	if stage == "" {
		return ErrInvalidStage
	}
	if space == "" {
		return ErrInvalidSpace
	}
	return nil
}

// --- Metadata Helpers ---

// setLeafCountMetadata stores the actual leaf count (excluding padding).
func (m *Tree) setLeafCountMetadata(ctx context.Context, stage, space string, count int) error {
	pkVal := pk(stage, space, metaPrefix, leafCountKey)
	val := []byte(fmt.Sprintf("%d", count))
	return m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val})
}

// getLeafCountMetadata retrieves the actual leaf count from metadata.
func (m *Tree) getLeafCountMetadata(ctx context.Context, stage, space string) (int, error) {
	pkVal := pk(stage, space, metaPrefix, leafCountKey)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return 0, fmt.Errorf("get leaf count metadata for %s/%s: %w", stage, space, err)
	}
	if item == nil {
		return 0, nil // No metadata means empty tree
	}
	var count int
	if _, err := fmt.Sscanf(string(item.Value), "%d", &count); err != nil {
		return 0, fmt.Errorf("parse leaf count metadata for %s/%s: %w", stage, space, err)
	}
	if count < 0 {
		return 0, fmt.Errorf("%w: negative leaf count %d for %s/%s", errCorruptedMetadata, count, stage, space)
	}
	return count, nil
}

// setMaxLevelMetadata stores the maximum tree level.
func (m *Tree) setMaxLevelMetadata(ctx context.Context, stage, space string, level int) error {
	pkVal := pk(stage, space, metaPrefix, maxLevelKey)
	val := []byte(fmt.Sprintf("%d", level))
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val}); err != nil {
		return fmt.Errorf("set max level metadata for %s/%s: %w", stage, space, err)
	}
	return nil
}

// --- Surgical Update Helpers ---

// recomputePathToRoot recomputes all parent hashes from a leaf to the root.
// PERFORMANCE: O(log N) nodes updated = tree height = log_branching(padded_leaf_count)
func (m *Tree) recomputePathToRoot(ctx context.Context, stage, space string, leafIndex int) error {
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return fmt.Errorf("get leaf count in recomputePathToRoot: %w", err)
	}
	if leafIndex >= leafCount {
		return fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}

	// Calculate max level from current tree structure
	paddedLeafCount := m.getPaddedCount(leafCount)
	maxLevel := m.calculateMaxLevel(paddedLeafCount)

	// Recompute from leaf's parent up to root (O(log N) operations)
	currentIndex := leafIndex
	for level := 1; level <= maxLevel; level++ {
		parentIndex := currentIndex / m.branching

		// Get all sibling hashes for this parent
		childHashes, err := m.getChildHashes(ctx, stage, space, level-1, parentIndex)
		if err != nil {
			return err
		}

		// Compute parent hash
		parentHash := hashByteSlices(childHashes)

		// Store parent node
		pkVal := pk(stage, space, level, parentIndex)
		if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: parentHash}); err != nil {
			return err
		}

		currentIndex = parentIndex
	}

	// Update root hash and persist max level metadata
	if err := m.updateRootHash(ctx, stage, space, maxLevel); err != nil {
		return fmt.Errorf("update root hash in recomputePathToRoot: %w", err)
	}
	return m.setMaxLevelMetadata(ctx, stage, space, maxLevel)
}

// getChildHashes retrieves all child hashes for a given parent node.
// Returns all existing child hashes in order.
func (m *Tree) getChildHashes(ctx context.Context, stage, space string, level, parentIndex int) ([][]byte, error) {
	// Reuse childBuf to avoid allocation on every call
	m.childBuf = m.childBuf[:0]

	for i := 0; i < m.branching; i++ {
		childIndex := parentIndex*m.branching + i
		childHash, _, err := m.getHash(ctx, stage, space, NodePosition{Level: level, Index: childIndex})
		if err != nil {
			return nil, err
		}
		if childHash != nil {
			m.childBuf = append(m.childBuf, childHash)
		}
	}

	return m.childBuf, nil
}

// updateRootHash updates the root hash from the topmost internal node.
// The root hash is stored at both (maxLevel, index=0) and at the rootKey.
// The rootKey is a read optimization: GetRootHash can retrieve the root without
// loading maxLevel metadata or probing storage. The authoritative root is always
// the hash at (maxLevel, index=0); rootKey is a cached copy.
func (m *Tree) updateRootHash(ctx context.Context, stage, space string, maxLevel int) error {
	if maxLevel < 0 {
		// Empty tree: no root to update
		return nil
	}
	rootHash, _, err := m.getHash(ctx, stage, space, NodePosition{Level: maxLevel, Index: 0})
	if err != nil {
		return fmt.Errorf("get root hash at level %d for %s/%s: %w", maxLevel, stage, space, err)
	}
	if rootHash == nil {
		// Should never happen: internal invariant violation
		return fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}
	pkVal := pk(stage, space, rootKey, "")
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: rootHash}); err != nil {
		return fmt.Errorf("store root hash for %s/%s: %w", stage, space, err)
	}
	return nil
}

// getPaddedCount returns the padded leaf count aligned to branching factor.
// INVARIANT: Padded count ensures complete tree levels (all internal nodes have exactly
// branching children, except possibly the rightmost node at each level).
func (m *Tree) getPaddedCount(leafCount int) int {
	if leafCount < 0 {
		panic(fmt.Sprintf("getPaddedCount: negative leaf count %d", leafCount))
	}
	if leafCount == 0 || leafCount%m.branching == 0 {
		return leafCount
	}
	return ((leafCount / m.branching) + 1) * m.branching
}

// calculateMaxLevel calculates the maximum tree level for a given leaf count.
// Returns 0 for single-leaf trees, -1 for empty trees.
// Level 0 = leaves, Level 1 = first internal level, etc.
func (m *Tree) calculateMaxLevel(leafCount int) int {
	if leafCount < 0 {
		panic(fmt.Sprintf("calculateMaxLevel: negative leaf count %d", leafCount))
	}
	if leafCount == 0 {
		return -1
	}
	if leafCount == 1 {
		return 0
	}
	level := 0
	nodes := leafCount
	for nodes > 1 {
		nodes = (nodes + m.branching - 1) / m.branching
		level++
	}
	return level
}

// ensurePaddingExists ensures padding leaves exist from start to end index.
// Uses batch operations for efficiency.
func (m *Tree) ensurePaddingExists(ctx context.Context, stage, space string, start, end int) error {
	if start >= end {
		return nil
	}

	// INVARIANT: Padding aligns leaf count to branching factor for balanced tree structure
	paddingLeaf := Leaf{Hash: paddingHash()}
	val, err := json.Marshal(paddingLeaf)
	if err != nil {
		return err
	}

	// Batch padding creation for better performance
	batch := make([]*kv.BatchItem, 0, end-start)
	for i := start; i < end; i++ {
		pkVal := pk(stage, space, 0, i)
		// During AddLeaf, we know padding doesn't exist, so skip the Get check
		// This is safe because we control when padding is created
		batch = append(batch, &kv.BatchItem{PK: pkVal, Value: val, Op: kv.Put})
	}

	if len(batch) > 0 {
		return m.store.Batch(ctx, batch)
	}
	return nil
}

// recomputeAffectedNodes recomputes all nodes affected by adding a new leaf.
func (m *Tree) recomputeAffectedNodes(ctx context.Context, stage, space string, oldCount, newCount int) error {
	// Check if tree structure changed (new level added or empty tree)
	oldMaxLevel := m.calculateMaxLevel(m.getPaddedCount(oldCount))
	newMaxLevel := m.calculateMaxLevel(m.getPaddedCount(newCount))

	if newMaxLevel != oldMaxLevel || oldCount == 0 {
		return m.recomputeAllInternalNodes(ctx, stage, space)
	}

	// Otherwise just update the path from the new leaf
	return m.recomputePathToRoot(ctx, stage, space, oldCount)
}

// recomputeAllInternalNodes rebuilds all internal nodes from current leaves.
// Called when tree structure changes (height increase) or after corruption recovery.
// PERFORMANCE: O(N) for full rebuild, but rare - only when adding leaf changes tree height.
func (m *Tree) recomputeAllInternalNodes(ctx context.Context, stage, space string) error {
	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return fmt.Errorf("get leaf count in recomputeAllInternalNodes: %w", err)
	}
	if leafCount == 0 {
		// Empty tree: clear max level metadata
		return m.setMaxLevelMetadata(ctx, stage, space, -1)
	}

	// INVARIANT: Pad leaf count to multiple of branching factor for balanced tree
	paddedCount := m.getPaddedCount(leafCount)
	if paddedCount != leafCount {
		// Ensure padding leaves exist
		if err := m.ensurePaddingExists(ctx, stage, space, leafCount, paddedCount); err != nil {
			return err
		}
	}

	maxLevel := m.calculateMaxLevel(paddedCount)

	// Special case: single leaf (maxLevel == 0)
	if maxLevel == 0 {
		// Root hash is just the single leaf's hash
		leafHash, _, err := m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
		if err != nil {
			return err
		}
		if leafHash != nil {
			pkVal := pk(stage, space, "root", "")
			if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: leafHash}); err != nil {
				return err
			}
		}
		return nil
	}

	// Recompute each level bottom-up
	for level := 1; level <= maxLevel; level++ {
		nodesAtPrevLevel := paddedCount
		for l := 1; l < level; l++ {
			nodesAtPrevLevel = (nodesAtPrevLevel + m.branching - 1) / m.branching
		}

		nodesAtThisLevel := (nodesAtPrevLevel + m.branching - 1) / m.branching

		for parentIdx := 0; parentIdx < nodesAtThisLevel; parentIdx++ {
			childHashes, err := m.getChildHashes(ctx, stage, space, level-1, parentIdx)
			if err != nil {
				return err
			}

			if len(childHashes) > 0 {
				parentHash := hashByteSlices(childHashes)
				pkVal := pk(stage, space, level, parentIdx)
				if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: parentHash}); err != nil {
					return err
				}
			}
		}
	}

	// Update root hash and persist max level metadata
	if err := m.updateRootHash(ctx, stage, space, maxLevel); err != nil {
		return fmt.Errorf("update root hash in recomputeAllInternalNodes: %w", err)
	}
	return m.setMaxLevelMetadata(ctx, stage, space, maxLevel)
}

// --- Hashing Helpers ---

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

// --- Storage Helpers ---

func pk(stage, space string, level, index any) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		lexkey.Encode("merkle", stage, space, level),
		lexkey.Encode(index),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
