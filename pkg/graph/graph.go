package graph

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
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

	// BFS traversal starting from id. visit is called for each visited node.
	BFS(ctx context.Context, start string, visit func(id string) error, limit int) error
}

// graphStore is the default implementation of Graph that stores nodes and edges
// using the project's kv.KV API. It stores small JSON blobs for easy decoding
// when enumerating.
type graphStore struct {
	store kv.KV
	name  string
}

// NewGraph creates a new graph instance using the provided kv store and a name
// that namespaces graph keys in the KV.
func NewGraph(store kv.KV, name string) Graph {
	return &graphStore{store: store, name: name}
}

// Helper storage formats
type storedNode struct {
	ID   string `json:"id"`
	Meta string `json:"meta"` // base64 encoded
}

type storedEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Meta string `json:"meta"`
}

func encodeMeta(b []byte) string          { return base64.StdEncoding.EncodeToString(b) }
func decodeMeta(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

func (g *graphStore) nodePartition() lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "node")
}

func (g *graphStore) edgePartition(from string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "edge", from)
}

func (g *graphStore) inEdgePartition(to string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "inedge", to)
}

func (g *graphStore) AddNode(ctx context.Context, id string, meta []byte) error {
	sn := storedNode{ID: id, Meta: encodeMeta(meta)}
	b, err := json.Marshal(sn)
	if err != nil {
		return err
	}
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	// Use Insert to avoid silently overwriting existing nodes.
	return g.store.Insert(ctx, &kv.Item{PK: pk, Value: b})
}

func (g *graphStore) GetNode(ctx context.Context, id string) (*Node, error) {
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	item, err := g.store.Get(ctx, pk)
	if err != nil || item == nil {
		return nil, err
	}
	var sn storedNode
	if err := json.Unmarshal(item.Value, &sn); err != nil {
		return nil, err
	}
	meta, err := decodeMeta(sn.Meta)
	if err != nil {
		return nil, err
	}
	return &Node{ID: sn.ID, Meta: meta}, nil
}

func (g *graphStore) DeleteNode(ctx context.Context, id string) error {
	// Gather outgoing edges
	outPart := g.edgePartition(id)
	outArgs := kv.QueryArgs{PartitionKey: outPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	outItems, err := g.store.Query(ctx, outArgs, kv.Ascending)
	if err != nil {
		return err
	}

	// Gather incoming edges
	inPart := g.inEdgePartition(id)
	inArgs := kv.QueryArgs{PartitionKey: inPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inItems, err := g.store.Query(ctx, inArgs, kv.Ascending)
	if err != nil {
		return err
	}

	// Build batch deletes: remove node, outgoing forward+reverse, incoming reverse+forward
	var ops []*kv.BatchItem
	// delete node key
	nodePK := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	ops = append(ops, &kv.BatchItem{Op: kv.Delete, PK: nodePK})

	// outgoing edges: for each forward entry (to), delete forward and reverse
	for _, it := range outItems {
		// decode row key via existing storedEdge (value contains from/to)
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			continue
		}
		pkF := lexkey.NewPrimaryKey(g.edgePartition(id), lexkey.Encode(se.To))
		pkR := lexkey.NewPrimaryKey(g.inEdgePartition(se.To), lexkey.Encode(id))
		ops = append(ops, &kv.BatchItem{Op: kv.Delete, PK: pkF})
		ops = append(ops, &kv.BatchItem{Op: kv.Delete, PK: pkR})
	}

	// incoming edges: for each reverse entry (from), delete reverse and forward
	for _, it := range inItems {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			continue
		}
		pkR := lexkey.NewPrimaryKey(g.inEdgePartition(id), lexkey.Encode(se.From))
		pkF := lexkey.NewPrimaryKey(g.edgePartition(se.From), lexkey.Encode(id))
		ops = append(ops, &kv.BatchItem{Op: kv.Delete, PK: pkR})
		ops = append(ops, &kv.BatchItem{Op: kv.Delete, PK: pkF})
	}

	// Attempt an atomic batch commit where supported by the backend.
	if len(ops) == 0 {
		return nil
	}
	// Try one Batch for all ops first (atomic when supported, e.g., Pebble)
	if err := g.store.Batch(ctx, ops); err == nil {
		return nil
	}

	// If the backend doesn't support very large batches or returned an error,
	// fall back to chunked commits for resiliency.
	const chunkSize = 1000
	for i := 0; i < len(ops); i += chunkSize {
		end := i + chunkSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := g.store.Batch(ctx, ops[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (g *graphStore) AddEdge(ctx context.Context, from, to string, meta []byte) error {
	se := storedEdge{From: from, To: to, Meta: encodeMeta(meta)}
	b, err := json.Marshal(se)
	if err != nil {
		return err
	}
	// forward key: edgePartition(from) / to
	pkF := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	// reverse key: inEdgePartition(to) / from
	pkR := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))

	items := []*kv.BatchItem{
		{Op: kv.Put, PK: pkF, Value: b},
		{Op: kv.Put, PK: pkR, Value: b},
	}
	return g.store.Batch(ctx, items)
}

func (g *graphStore) RemoveEdge(ctx context.Context, from, to string) error {
	pkF := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	pkR := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))
	items := []*kv.BatchItem{
		{Op: kv.Delete, PK: pkF},
		{Op: kv.Delete, PK: pkR},
	}
	return g.store.Batch(ctx, items)
}

func (g *graphStore) Neighbors(ctx context.Context, from string) ([]Edge, error) {
	partition := g.edgePartition(from)
	args := kv.QueryArgs{PartitionKey: partition, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := g.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		return nil, err
	}
	var out []Edge
	for _, it := range items {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			// skip malformed edge records
			continue
		}
		meta, _ := decodeMeta(se.Meta)
		out = append(out, Edge{From: se.From, To: se.To, Meta: meta})
	}
	return out, nil
}

// IncomingNeighbors returns edges that point to `to` (i.e., incoming edges).
func (g *graphStore) IncomingNeighbors(ctx context.Context, to string) ([]Edge, error) {
	partition := g.inEdgePartition(to)
	args := kv.QueryArgs{PartitionKey: partition, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := g.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		return nil, err
	}
	var out []Edge
	for _, it := range items {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			continue
		}
		meta, _ := decodeMeta(se.Meta)
		out = append(out, Edge{From: se.From, To: se.To, Meta: meta})
	}
	return out, nil
}

func (g *graphStore) BFS(ctx context.Context, start string, visit func(id string) error, limit int) error {
	// simple queue-based BFS
	queue := []string{start}
	seen := map[string]struct{}{}
	var visited int
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if _, ok := seen[cur]; ok {
			continue
		}
		if err := visit(cur); err != nil {
			return err
		}
		seen[cur] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			return nil
		}
		nbrs, err := g.Neighbors(ctx, cur)
		if err != nil {
			return err
		}
		for _, e := range nbrs {
			if _, ok := seen[e.To]; !ok {
				queue = append(queue, e.To)
			}
		}
	}
	return nil
}
