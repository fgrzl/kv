package merkle

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
	"go.opentelemetry.io/otel"
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
