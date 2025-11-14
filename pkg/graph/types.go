package graph

import (
	"context"

	"github.com/fgrzl/enumerators"
)

// Node represents a graph node.
type Node struct {
	ID   string
	Meta []byte
}

// Edge represents a directed edge from From -> To with optional metadata.
type Edge struct {
	From string
	To   string
	Meta []byte
}

// Graph is a simple graph interface backed by a kv.KV store.
type Graph interface {
	AddNode(ctx context.Context, id string, meta []byte) error
	GetNode(ctx context.Context, id string) (*Node, error)
	DeleteNode(ctx context.Context, id string) error

	AddEdge(ctx context.Context, from, to string, meta []byte) error
	RemoveEdge(ctx context.Context, from, to string) error
	IncomingNeighbors(ctx context.Context, to string) ([]Edge, error)
	Neighbors(ctx context.Context, from string) ([]Edge, error)

	// Existence and count helpers
	HasNode(ctx context.Context, id string) (bool, error)
	EdgeExists(ctx context.Context, from, to string) (bool, error)
	// NodeDegree returns incoming and outgoing edge counts for the node (inCount, outCount)
	NodeDegree(ctx context.Context, id string) (int, int, error)

	// Streaming enumerators for neighbors (avoid materializing large slices)
	EnumerateNeighbors(ctx context.Context, from string) enumerators.Enumerator[Edge]
	EnumerateIncomingNeighbors(ctx context.Context, to string) enumerators.Enumerator[Edge]

	// BFS traversal starting from id. visit is called for each visited node.
	BFS(ctx context.Context, start string, visit func(id string) error, limit int) error

	BFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error
	CollectBFSIDs(ctx context.Context, start string, limit int) ([]string, error)

	DFS(ctx context.Context, start string, visit func(id string) error, limit int) error
	DFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error
	CollectDFSIDs(ctx context.Context, start string, limit int) ([]string, error)

	// BatchAdd atomically (when supported) inserts a set of nodes and edges.
	// Edges are written as forward and reverse entries. If the backend does not
	// support large atomic batches the implementation will fall back to chunked
	// commits.
	BatchAdd(ctx context.Context, nodes []Node, edges []Edge) error
}
