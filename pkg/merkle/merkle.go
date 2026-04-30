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
// Build persists all nodes in one streamed BatchChunks pass to minimize KV round trips.
// Optional WithSkipPruneOnBuild(true) skips the initial RemoveRange when the stage/space is known empty.
//
// # CONCURRENCY
//
// This implementation assumes a single-writer model. Concurrent writes to the same stage/space
// WILL corrupt the tree. Use external synchronization (e.g., leader election, distributed lock).
//
// # STORAGE LAYOUT
//
// One KV partition per tree so remote backends can batch and range-delete efficiently:
//
//	PartitionKey = merkle/{stage}/{space}
//	RowKey node/{level}/{index} — leaves (level 0), internal nodes, logical root at (maxLevel,0)
//	RowKey root — cached root hash for GetRootHash without probing max level
//	RowKey meta/{name} — leaf count, max level, etc.
//
// The root row is a read optimization; the authoritative root hash is always the internal
// node at (maxLevel, index=0).
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
	// skipPruneOnBuild skips RemoveRange at the start of Build when the caller knows the
	// stage/space has no prior Merkle keys (saves a full scan on remote backends).
	skipPruneOnBuild bool
	// childBuf and childPKBuf are reused across calls to getChildHashes to eliminate repeated allocations.
	// CONCURRENCY: SAFE because Tree assumes single-writer model (documented above).
	// NOT safe for concurrent use - requires external synchronization if needed.
	childBuf   [][]byte
	childPKBuf []lexkey.PrimaryKey
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

// WithSkipPruneOnBuild skips the RemoveRange prune step at the start of Build.
// Use only when the stage/space is guaranteed empty or stale keys are acceptable;
// otherwise Build may leave orphaned nodes from a previous tree.
func WithSkipPruneOnBuild(skip bool) Option {
	return func(m *Tree) {
		m.skipPruneOnBuild = skip
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
	m.childPKBuf = make([]lexkey.PrimaryKey, 0, m.branching)
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
	if !m.skipPruneOnBuild {
		if err := m.pruneOldNodes(ctx, stage, space); err != nil {
			err = fmt.Errorf("prune old nodes for %s/%s: %w", stage, space, err)
			slog.ErrorContext(ctx, "failed to prune old nodes", "stage", stage, "space", space, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}
	paddedNodes, batchItems, actualLeafCount, err := m.collectBuildPutBatchItems(stage, space, leaves)
	if err != nil {
		err = fmt.Errorf("collect build batch items for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to collect build batch items", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(paddedNodes) == 0 {
		slog.DebugContext(ctx, "Merkle tree build completed with no leaves", "stage", stage, "space", space)
		span.SetAttributes(attribute.Int("leaves", 0))
		return nil
	}

	if err := m.store.BatchChunks(ctx, enumerators.Slice(batchItems), m.batchSize); err != nil {
		err = fmt.Errorf("persist merkle build for %s/%s: %w", stage, space, err)
		slog.ErrorContext(ctx, "failed to persist merkle build", "stage", stage, "space", space, "err", err)
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
	rangeKey := treeRangeKey(stage, space)
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

	pkVal := nodePK(stage, space, 0, leafIndex)
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
// It scans level-0 node rows in index order via one partition range query (not N Get calls).
func (m *Tree) EnumerateLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	if err := validateStageSpace(stage, space); err != nil {
		return enumerators.Error[Leaf](err)
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return enumerators.Error[Leaf](err)
	}
	if leafCount == 0 {
		return enumerators.Empty[Leaf]()
	}

	args := nodeLevelRangeQuery(stage, space, 0, 0, leafCount-1)
	return enumerators.Map(
		m.store.Enumerate(ctx, args),
		func(item *kv.Item) (Leaf, error) {
			if item == nil {
				return Leaf{}, fmt.Errorf("nil item in leaf enumeration")
			}
			var leaf Leaf
			if err := json.Unmarshal(item.Value, &leaf); err != nil {
				return Leaf{}, fmt.Errorf("unmarshal leaf in enumeration: %w", err)
			}
			return leaf, nil
		},
	)
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

	pkVal := nodePK(stage, space, 0, leafIndex)
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

	pkVal := nodePK(stage, space, 0, leafCount)
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
	return m.store.RemoveRange(ctx, treeRangeKey(stage, space))
}

// collectBuildPutBatchItems materializes all Put operations for a full tree build
// (leaves, padding, internal nodes, root, metadata) without touching the store.
// paddedNodes is the padded leaf tier; actualLeafCount is the pre-padding leaf count.
func (m *Tree) collectBuildPutBatchItems(stage, space string, leaves enumerators.Enumerator[Leaf]) (paddedNodes []Branch, batchItems []*kv.BatchItem, actualLeafCount int, err error) {
	partition := treePartition(stage, space)
	currNodes := make([]Branch, 0, 1000)
	batchItems = make([]*kv.BatchItem, 0, 1024)
	index := 0
	if err := enumerators.ForEach(leaves, func(leaf Leaf) error {
		val, e := json.Marshal(leaf)
		if e != nil {
			return e
		}
		batchItems = append(batchItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, index),
			Value: val,
			Op:    kv.Put,
		})
		currNodes = append(currNodes, Branch{Hash: leaf.Hash, Level: 0, Index: index})
		index++
		return nil
	}); err != nil {
		return nil, nil, 0, err
	}
	if len(currNodes) == 0 {
		return nil, nil, 0, nil
	}
	actualLeafCount = len(currNodes)

	for len(currNodes)%m.branching != 0 {
		hash := paddingHash()
		leaf := Leaf{Hash: hash}
		val, e := json.Marshal(leaf)
		if e != nil {
			return nil, nil, 0, e
		}
		idx := len(currNodes)
		batchItems = append(batchItems, &kv.BatchItem{
			PK:    nodePKInPartition(partition, 0, idx),
			Value: val,
			Op:    kv.Put,
		})
		currNodes = append(currNodes, Branch{Hash: hash, Level: 0, Index: idx})
	}

	nodes := currNodes
	level := 1
	for len(nodes) > 1 {
		var next []Branch
		var batch []*kv.BatchItem
		next, batch = m.hashNodeLevelInPartition(partition, nodes, level)
		batchItems = append(batchItems, batch...)
		nodes = next
		level++
	}
	if len(nodes) != 1 {
		return nil, nil, 0, fmt.Errorf("collectBuildPutBatchItems: expected 1 root node, got %d", len(nodes))
	}
	root := nodes[0]
	batchItems = append(batchItems, &kv.BatchItem{
		PK:    rootPKInPartition(partition),
		Value: root.Hash,
		Op:    kv.Put,
	})

	maxLevel := m.calculateMaxLevel(len(currNodes))
	batchItems = append(batchItems,
		&kv.BatchItem{
			PK:    metaPKInPartition(partition, leafCountKey),
			Value: []byte(fmt.Sprintf("%d", actualLeafCount)),
			Op:    kv.Put,
		},
		&kv.BatchItem{
			PK:    metaPKInPartition(partition, maxLevelKey),
			Value: []byte(fmt.Sprintf("%d", maxLevel)),
			Op:    kv.Put,
		},
	)

	return currNodes, batchItems, actualLeafCount, nil
}

func (m *Tree) hashNodeLevel(stage, space string, nodes []Branch, level int) ([]Branch, []*kv.BatchItem) {
	return m.hashNodeLevelInPartition(treePartition(stage, space), nodes, level)
}

func (m *Tree) hashNodeLevelInPartition(partition lexkey.LexKey, nodes []Branch, level int) ([]Branch, []*kv.BatchItem) {
	numGroups := (len(nodes) + m.branching - 1) / m.branching

	// Pre-allocate slices for better performance
	next := make([]Branch, 0, numGroups)
	batch := make([]*kv.BatchItem, 0, numGroups)

	for i := 0; i < len(nodes); i += m.branching {
		end := min(i+m.branching, len(nodes))
		sum := hashNodes(nodes[i:end])
		pkVal := nodePKInPartition(partition, level, i/m.branching)
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
		pkVal := nodePK(stage, space, h, 0)
		item, err := m.store.Get(ctx, pkVal)
		if err != nil || item == nil {
			return h - 1
		}
	}
}

func (m *Tree) walkSubtree(ctx context.Context, stage, space string, ref NodePosition) enumerators.Enumerator[Leaf] {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = rootPK(stage, space)
	} else {
		pkVal = nodePK(stage, space, ref.Level, ref.Index)
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
		pkVal = rootPK(stage, space)
	} else {
		pkVal = nodePK(stage, space, ref.Level, ref.Index)
	}
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return nil, nil, fmt.Errorf("store get: %w", err)
	}
	if item == nil {
		return nil, nil, nil
	}
	hash, err := m.hashFromStoredValue(ref.Level, item.Value)
	if err != nil {
		return nil, nil, err
	}
	return hash, item.Value, nil
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
	pkVal := metaPK(stage, space, leafCountKey)
	val := []byte(fmt.Sprintf("%d", count))
	return m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val})
}

// getLeafCountMetadata retrieves the actual leaf count from metadata.
func (m *Tree) getLeafCountMetadata(ctx context.Context, stage, space string) (int, error) {
	pkVal := metaPK(stage, space, leafCountKey)
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
	pkVal := metaPK(stage, space, maxLevelKey)
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

	return m.recomputePathToRootWithLeafCount(ctx, stage, space, leafIndex, leafCount)
}

func (m *Tree) recomputePathToRootWithLeafCount(ctx context.Context, stage, space string, leafIndex, leafCount int) error {
	if leafIndex >= leafCount {
		return fmt.Errorf("leaf index %d out of bounds (count: %d)", leafIndex, leafCount)
	}
	partition := treePartition(stage, space)

	// Calculate max level from current tree structure
	paddedLeafCount := m.getPaddedCount(leafCount)
	maxLevel := m.calculateMaxLevel(paddedLeafCount)
	var rootHash []byte

	// Recompute from leaf's parent up to root (O(log N) operations)
	currentIndex := leafIndex
	for level := 1; level <= maxLevel; level++ {
		parentIndex := currentIndex / m.branching

		// Get all sibling hashes for this parent
		childHashes, err := m.getChildHashesInPartition(ctx, partition, level-1, parentIndex)
		if err != nil {
			return err
		}

		// Compute parent hash
		parentHash := hashByteSlices(childHashes)

		// Store parent node
		pkVal := nodePKInPartition(partition, level, parentIndex)
		if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: parentHash}); err != nil {
			return err
		}
		if level == maxLevel {
			rootHash = parentHash
		}

		currentIndex = parentIndex
	}

	if maxLevel == 0 {
		var err error
		rootHash, _, err = m.getHash(ctx, stage, space, NodePosition{Level: 0, Index: 0})
		if err != nil {
			return fmt.Errorf("get root hash for single-leaf tree: %w", err)
		}
	}

	if rootHash == nil {
		return fmt.Errorf("%w: root hash is nil at level %d for %s/%s", errInvalidTreeState, maxLevel, stage, space)
	}

	pkVal := rootPKInPartition(partition)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: rootHash}); err != nil {
		return fmt.Errorf("store root hash for %s/%s: %w", stage, space, err)
	}

	return nil
}

// getChildHashes retrieves all child hashes for a given parent node.
// Returns all existing child hashes in order.
func (m *Tree) getChildHashes(ctx context.Context, stage, space string, level, parentIndex int) ([][]byte, error) {
	return m.getChildHashesInPartition(ctx, treePartition(stage, space), level, parentIndex)
}

func (m *Tree) getChildHashesInPartition(ctx context.Context, partition lexkey.LexKey, level, parentIndex int) ([][]byte, error) {
	// Reuse childBuf to avoid allocation on every call
	m.childBuf = m.childBuf[:0]
	m.childPKBuf = m.childPKBuf[:0]

	for i := 0; i < m.branching; i++ {
		childIndex := parentIndex*m.branching + i
		m.childPKBuf = append(m.childPKBuf, nodePKInPartition(partition, level, childIndex))
	}

	results, err := m.store.GetBatch(ctx, m.childPKBuf...)
	if err != nil {
		return nil, fmt.Errorf("store get batch: %w", err)
	}

	for _, result := range results {
		if !result.Found || result.Item == nil {
			continue
		}

		childHash, err := m.hashFromStoredValue(level, result.Item.Value)
		if err != nil {
			return nil, err
		}
		if childHash != nil {
			m.childBuf = append(m.childBuf, childHash)
		}
	}

	return m.childBuf, nil
}

// loadLevelNodeHashesInOrder loads every stored hash at the given Merkle level in one
// partition range scan (index order), for bottom-up recomputation without per-parent GetBatch.
func (m *Tree) loadLevelNodeHashesInOrder(ctx context.Context, stage, space string, level, nodeCount int) ([][]byte, error) {
	return m.loadLevelNodeHashesInOrderInPartition(ctx, treePartition(stage, space), level, nodeCount)
}

func (m *Tree) loadLevelNodeHashesInOrderInPartition(ctx context.Context, partition lexkey.LexKey, level, nodeCount int) ([][]byte, error) {
	if nodeCount <= 0 {
		return nil, nil
	}
	args := nodeLevelRangeQueryInPartition(partition, level, 0, nodeCount-1)
	items, err := enumerators.ToSlice(m.store.Enumerate(ctx, args))
	if err != nil {
		return nil, err
	}
	if len(items) != nodeCount {
		return nil, fmt.Errorf("loadLevelNodeHashesInOrder: expected %d nodes at level %d, got %d", nodeCount, level, len(items))
	}
	out := make([][]byte, 0, nodeCount)
	for _, item := range items {
		if item == nil {
			return nil, fmt.Errorf("loadLevelNodeHashesInOrder: nil item at level %d", level)
		}
		h, err := m.hashFromStoredValue(level, item.Value)
		if err != nil {
			return nil, err
		}
		if h == nil {
			return nil, fmt.Errorf("loadLevelNodeHashesInOrder: nil hash at level %d", level)
		}
		out = append(out, h)
	}
	return out, nil
}

func (m *Tree) hashFromStoredValue(level int, value []byte) ([]byte, error) {
	if level != 0 {
		return value, nil
	}

	// Use pooled Leaf to reduce allocations when decoding leaf payloads.
	leaf := leafPool.Get().(*Leaf)
	defer func() {
		*leaf = Leaf{}
		leafPool.Put(leaf)
	}()

	if err := json.Unmarshal(value, leaf); err != nil {
		return nil, fmt.Errorf("unmarshal leaf: %w", err)
	}

	return leaf.Hash, nil
}

// updateRootHash updates the root hash from the topmost internal node.
// The root hash is stored at both node/(maxLevel,0) and the root row key.
// The root row is a read optimization: GetRootHash can retrieve the root without
// loading maxLevel metadata or probing storage. The authoritative root is always
// the hash at (maxLevel, index=0); the root row is a cached copy.
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
	pkVal := rootPK(stage, space)
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
	partition := treePartition(stage, space)

	// INVARIANT: Padding aligns leaf count to branching factor for balanced tree structure
	paddingLeaf := Leaf{Hash: paddingHash()}
	val, err := json.Marshal(paddingLeaf)
	if err != nil {
		return err
	}

	// Batch padding creation for better performance
	batch := make([]*kv.BatchItem, 0, end-start)
	for i := start; i < end; i++ {
		pkVal := nodePKInPartition(partition, 0, i)
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
	return m.recomputePathToRootWithLeafCount(ctx, stage, space, oldCount, newCount)
}

// recomputeAllInternalNodes rebuilds all internal nodes from current leaves.
// Called when tree structure changes (height increase) or after corruption recovery.
// PERFORMANCE: O(N) for full rebuild, but rare - only when adding leaf changes tree height.
func (m *Tree) recomputeAllInternalNodes(ctx context.Context, stage, space string) error {
	partition := treePartition(stage, space)
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
			pkVal := rootPKInPartition(partition)
			if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: leafHash}); err != nil {
				return err
			}
		}
		return nil
	}

	// Recompute each level bottom-up (one range scan per level to load child hashes).
	for level := 1; level <= maxLevel; level++ {
		nodesAtPrevLevel := paddedCount
		for l := 1; l < level; l++ {
			nodesAtPrevLevel = (nodesAtPrevLevel + m.branching - 1) / m.branching
		}

		nodesAtThisLevel := (nodesAtPrevLevel + m.branching - 1) / m.branching
		prevLevel := level - 1
		prevHashes, err := m.loadLevelNodeHashesInOrderInPartition(ctx, partition, prevLevel, nodesAtPrevLevel)
		if err != nil {
			return err
		}

		batch := make([]*kv.BatchItem, 0, nodesAtThisLevel)
		for parentIdx := 0; parentIdx < nodesAtThisLevel; parentIdx++ {
			start := parentIdx * m.branching
			if start >= nodesAtPrevLevel {
				break
			}
			end := min(start+m.branching, nodesAtPrevLevel)
			childHashes := prevHashes[start:end]
			if len(childHashes) == 0 {
				continue
			}
			parentHash := hashByteSlices(childHashes)
			pkVal := nodePKInPartition(partition, level, parentIdx)
			batch = append(batch, &kv.BatchItem{PK: pkVal, Value: parentHash, Op: kv.Put})
		}

		if err := m.commitBatchIfNeeded(ctx, batch); err != nil {
			return err
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

func treePartition(stage, space string) lexkey.LexKey {
	return lexkey.Encode("merkle", stage, space)
}

// treeRangeKey covers every row in the Merkle tree partition for this stage/space.
func treeRangeKey(stage, space string) lexkey.RangeKey {
	return lexkey.NewRangeKeyFull(treePartition(stage, space))
}

// nodeLevelRangeQuery returns a partition-scoped row-key Between query for Merkle nodes at
// the given level with indices in [firstIndex, lastIndexInclusive].
// Requires lexkey ordering of Encode("node", level, index) to follow integer index order.
func nodeLevelRangeQuery(stage, space string, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return nodeLevelRangeQueryInPartition(treePartition(stage, space), level, firstIndex, lastIndexInclusive)
}

func nodeLevelRangeQueryInPartition(partition lexkey.LexKey, level, firstIndex, lastIndexInclusive int) kv.QueryArgs {
	return kv.QueryArgs{
		PartitionKey: partition,
		StartRowKey:  lexkey.Encode("node", level, firstIndex),
		EndRowKey:    lexkey.Encode("node", level, lastIndexInclusive),
		Operator:     kv.Between,
	}
}

func nodePK(stage, space string, level, index int) lexkey.PrimaryKey {
	return nodePKInPartition(treePartition(stage, space), level, index)
}

func nodePKInPartition(partition lexkey.LexKey, level, index int) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		partition,
		lexkey.Encode("node", level, index),
	)
}

func rootPK(stage, space string) lexkey.PrimaryKey {
	return rootPKInPartition(treePartition(stage, space))
}

func rootPKInPartition(partition lexkey.LexKey) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(partition, lexkey.Encode(rootKey))
}

func metaPK(stage, space, metaName string) lexkey.PrimaryKey {
	return metaPKInPartition(treePartition(stage, space), metaName)
}

func metaPKInPartition(partition lexkey.LexKey, metaName string) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		partition,
		lexkey.Encode(metaPrefix, metaName),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
