package graph

import (
	"context"
	"os"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

func TestGraphBasic(t *testing.T) {
	// create a temp pebble DB
	path := "./test-pebble-db"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test")
	ctx := context.Background()

	if err := g.AddNode(ctx, "a", []byte("A-meta")); err != nil {
		t.Fatalf("add node a: %v", err)
	}
	if err := g.AddNode(ctx, "b", []byte("B-meta")); err != nil {
		t.Fatalf("add node b: %v", err)
	}
	if err := g.AddEdge(ctx, "a", "b", []byte("a->b")); err != nil {
		t.Fatalf("add edge a->b: %v", err)
	}

	// check neighbors
	nbrs, err := g.Neighbors(ctx, "a")
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(nbrs) != 1 || nbrs[0].To != "b" {
		t.Fatalf("unexpected neighbors: %#v", nbrs)
	}

	// check incoming neighbors for b
	in, err := g.IncomingNeighbors(ctx, "b")
	if err != nil {
		t.Fatalf("incoming neighbors: %v", err)
	}
	if len(in) != 1 || in[0].From != "a" {
		t.Fatalf("unexpected incoming neighbors: %#v", in)
	}

	// BFS should visit a then b
	var order []string
	if err := g.BFS(ctx, "a", func(id string) error {
		order = append(order, id)
		return nil
	}, 0); err != nil {
		t.Fatalf("bfs: %v", err)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("unexpected bfs order: %v", order)
	}

	// remove the edge and assert both directions removed
	if err := g.RemoveEdge(ctx, "a", "b"); err != nil {
		t.Fatalf("remove edge: %v", err)
	}
	nbrsAfter, err := g.Neighbors(ctx, "a")
	if err != nil {
		t.Fatalf("neighbors after remove: %v", err)
	}
	if len(nbrsAfter) != 0 {
		t.Fatalf("expected no neighbors after remove, got: %#v", nbrsAfter)
	}
	inAfter, err := g.IncomingNeighbors(ctx, "b")
	if err != nil {
		t.Fatalf("incoming after remove: %v", err)
	}
	if len(inAfter) != 0 {
		t.Fatalf("expected no incoming after remove, got: %#v", inAfter)
	}
}

func TestDeleteNodeCascade(t *testing.T) {
	path := "./test-pebble-db-cascade"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-cascade")
	ctx := context.Background()

	_ = g.AddNode(ctx, "a", nil)
	_ = g.AddNode(ctx, "b", nil)
	_ = g.AddNode(ctx, "c", nil)
	_ = g.AddNode(ctx, "d", nil)

	// a->b, c->b (incoming to b), and b->d (outgoing from b)
	_ = g.AddEdge(ctx, "a", "b", nil)
	_ = g.AddEdge(ctx, "c", "b", nil)
	_ = g.AddEdge(ctx, "b", "d", nil)

	// delete node b and ensure edges are removed
	if err := g.DeleteNode(ctx, "b"); err != nil {
		t.Fatalf("delete node b: %v", err)
	}

	// b should be gone
	nb, err := g.GetNode(ctx, "b")
	if err != nil {
		t.Fatalf("get node b: %v", err)
	}
	if nb != nil {
		t.Fatalf("expected node b to be deleted")
	}

	// outgoing from a should not include b
	na, err := g.Neighbors(ctx, "a")
	if err != nil {
		t.Fatalf("neighbors a: %v", err)
	}
	for _, e := range na {
		if e.To == "b" {
			t.Fatalf("expected edge a->b removed")
		}
	}

	// incoming to b should be empty
	in, err := g.IncomingNeighbors(ctx, "b")
	if err != nil {
		t.Fatalf("incoming b: %v", err)
	}
	if len(in) != 0 {
		t.Fatalf("expected no incoming to b after delete, got: %#v", in)
	}

	// outgoing from b should be empty
	outB, err := g.Neighbors(ctx, "b")
	if err != nil {
		t.Fatalf("neighbors b: %v", err)
	}
	if len(outB) != 0 {
		t.Fatalf("expected no outgoing from b after delete, got: %#v", outB)
	}

	// edge b->d removed, but other nodes still exist
	nd, err := g.GetNode(ctx, "d")
	if err != nil || nd == nil {
		t.Fatalf("expected node d to remain: %v", err)
	}
}

func TestAddDuplicateNode(t *testing.T) {
	path := "./test-pebble-db-dup"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-dup")
	ctx := context.Background()

	if err := g.AddNode(ctx, "x", []byte("xmeta")); err != nil {
		t.Fatalf("add node x: %v", err)
	}
	// adding again should return an error (already exists)
	if err := g.AddNode(ctx, "x", []byte("xmeta2")); err == nil {
		t.Fatalf("expected error when adding duplicate node")
	}
}
