package merkle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/merkle")

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

type DiffLeaf struct {
	Action string // "added" or "removed"
	Leaf   Leaf
}

// Tree implements a persistent Merkle tree for entity snapshots.
type Tree struct {
	store     kv.KV
	branching int
	batchSize int
}

type NodePosition struct {
	Level int
	Index int
}

var (
	rootRef             = NodePosition{Level: -1, Index: 0}
	ErrEmptyLeaves      = errors.New("no leaves provided")
	ErrInvalidBranching = errors.New("branching factor must be at least 2")
)

const defaultBatchSize = 500_000

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
func NewTree(store kv.KV, opts ...Option) *Tree {
	m := &Tree{
		store:     store,
		branching: 2,
		batchSize: defaultBatchSize,
	}
	for _, o := range opts {
		o(m)
	}
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

	slog.InfoContext(ctx, "starting Merkle tree build", "stage", stage, "space", space)
	if err := m.pruneOldNodes(ctx, stage, space); err != nil {
		slog.ErrorContext(ctx, "failed to prune old nodes", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	currNodes, err := m.persistLeaves(ctx, stage, space, leaves)
	if err != nil {
		slog.ErrorContext(ctx, "failed to persist leaves", "stage", stage, "space", space, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(currNodes) == 0 {
		slog.InfoContext(ctx, "Merkle tree build completed with no leaves", "stage", stage, "space", space)
		span.SetAttributes(attribute.Int("leaves", 0))
		return nil
	}
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
	slog.InfoContext(ctx, "Merkle tree build completed", "stage", stage, "space", space, "leaves", len(currNodes))
	span.SetAttributes(attribute.Int("leaves", len(currNodes)))
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
	slog.InfoContext(ctx, "pruning Merkle tree", "stage", stage, "space", space)
	partition := lexkey.Encode("merkle", stage, space)
	rangeKey := lexkey.NewRangeKeyFull(partition)
	err := m.store.RemoveRange(ctx, rangeKey)
	if err != nil {
		slog.ErrorContext(ctx, "failed to prune Merkle tree", "stage", stage, "space", space, "err", err)
		return err
	}
	slog.InfoContext(ctx, "Merkle tree pruned", "stage", stage, "space", space)
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
		hash := ComputeHash([]byte("padding"))
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
	h := sha256.New()
	for _, n := range nodes {
		h.Write(n.Hash)
	}
	return h.Sum(nil)
}

func (m *Tree) persistRoot(ctx context.Context, stage, space string, nodes []Branch) error {
	if len(nodes) != 1 {
		return fmt.Errorf("persistRoot: expected 1 node, got %d", len(nodes))
	}
	root := nodes[0]
	pkVal := pk(stage, space, "root", "")
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

	// Edge: Both trees empty at root
	if ref == rootRef && prevHash == nil && currHash == nil {
		return enumerators.Empty[Leaf]()
	}

	// Edge: prev is empty, curr is not
	if ref == rootRef && prevHash == nil && currHash != nil {
		return m.scanLeaves(ctx, curr, space)
	}

	// Edge: curr is empty, prev is not (for one-way diff, should emit nothing)
	if ref == rootRef && currHash == nil && prevHash != nil {
		return enumerators.Empty[Leaf]()
	}

	// Early exit if identical hashes
	if prevHash != nil && currHash != nil && string(prevHash) == string(currHash) {
		return enumerators.Empty[Leaf]()
	}

	// Leaf case
	if ref.Level == 0 && currHash != nil && prevHash == nil {
		return decodeLeafEnumerator(currVal)
	}
	if ref.Level == 0 && prevHash != nil && currHash != nil && string(prevHash) != string(currHash) {
		return decodeLeafEnumerator(currVal)
	}
	if prevHash == nil && currHash != nil {
		return m.walkSubtree(ctx, curr, space, ref)
	}
	if currHash == nil && prevHash != nil {
		// Only needed for symmetric diff; for regular diff, nothing to emit
		return enumerators.Empty[Leaf]()
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
		pkVal = pk(stage, space, "root", "")
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

// scanLeaves enumerates all leaves for a given stage/space, used for empty/non-empty diff edge cases.
func (m *Tree) scanLeaves(ctx context.Context, stage, space string) enumerators.Enumerator[Leaf] {
	var leaves []Leaf
	for i := 0; ; i++ {
		pk := pk(stage, space, 0, i)
		item, err := m.store.Get(ctx, pk)
		if err != nil || item == nil {
			break // No more leaves
		}
		var leaf Leaf
		if err := json.Unmarshal(item.Value, &leaf); err == nil && leaf.Ref != "" {
			leaves = append(leaves, leaf)
		}
	}
	return enumerators.Slice(leaves)
}

// --- Storage Helper ---

func (m *Tree) getHash(ctx context.Context, stage, space string, ref NodePosition) ([]byte, []byte, error) {
	var pkVal lexkey.PrimaryKey
	if ref.Level < 0 {
		pkVal = pk(stage, space, "root", "")
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
		var leaf Leaf
		if err := json.Unmarshal(item.Value, &leaf); err != nil {
			return nil, nil, fmt.Errorf("unmarshal leaf: %w", err)
		}
		return leaf.Hash, item.Value, nil
	}
	return item.Value, item.Value, nil
}

// --- Utility ---

func pk(stage, space string, level, index any) lexkey.PrimaryKey {
	return lexkey.NewPrimaryKey(
		lexkey.Encode("merkle", stage, space, level),
		lexkey.Encode(index),
	)
}

func ComputeHash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
