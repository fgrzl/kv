package graph

import (
	"context"
	"encoding/json"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

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

func (g *graphStore) nodePartition() lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "node")
}

func (g *graphStore) edgePartition(from string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "edge", from)
}

func (g *graphStore) inEdgePartition(to string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "inedge", to)
}

func (g *graphStore) AddEdge(ctx context.Context, from, to string, meta []byte) error {
	se := storedEdge{From: from, To: to, Meta: encodeMeta(meta)}
	b, err := json.Marshal(se)
	if err != nil {
		return err
	}

	// Store forward edge
	edgePK := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	err = g.store.Put(ctx, &kv.Item{PK: edgePK, Value: b})
	if err != nil {
		return err
	}

	// Store reverse edge for efficient incoming neighbor queries
	reversePK := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))
	err = g.store.Put(ctx, &kv.Item{PK: reversePK, Value: b})
	if err != nil {
		return err
	}

	return nil
}

func (g *graphStore) RemoveEdge(ctx context.Context, from, to string) error {
	// Delete forward edge (idempotent: ignore if doesn't exist)
	edgePK := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	_ = g.store.Remove(ctx, edgePK)

	// Delete reverse edge (idempotent: ignore if doesn't exist)
	reversePK := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))
	_ = g.store.Remove(ctx, reversePK)

	return nil
}

func (g *graphStore) EdgeExists(ctx context.Context, from, to string) (bool, error) {
	pk := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

func (g *graphStore) HasNode(ctx context.Context, id string) (bool, error) {
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}
