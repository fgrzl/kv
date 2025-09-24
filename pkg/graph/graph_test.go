package graph

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/fgrzl/enumerators"

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

func TestExistenceAndDegreeAPIs(t *testing.T) {
	path := "./test-pebble-db-exists"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-exists")
	ctx := context.Background()

	// create nodes and edges:
	// a->b, a->c, d->a
	if err := g.AddNode(ctx, "a", nil); err != nil {
		t.Fatalf("add node a: %v", err)
	}
	if err := g.AddNode(ctx, "b", nil); err != nil {
		t.Fatalf("add node b: %v", err)
	}
	if err := g.AddNode(ctx, "c", nil); err != nil {
		t.Fatalf("add node c: %v", err)
	}
	if err := g.AddNode(ctx, "d", nil); err != nil {
		t.Fatalf("add node d: %v", err)
	}

	if err := g.AddEdge(ctx, "a", "b", nil); err != nil {
		t.Fatalf("add edge a->b: %v", err)
	}
	if err := g.AddEdge(ctx, "a", "c", nil); err != nil {
		t.Fatalf("add edge a->c: %v", err)
	}
	if err := g.AddEdge(ctx, "d", "a", nil); err != nil {
		t.Fatalf("add edge d->a: %v", err)
	}

	// HasNode
	ok, err := g.HasNode(ctx, "a")
	if err != nil || !ok {
		t.Fatalf("expected HasNode(a) true, got %v err %v", ok, err)
	}
	ok, err = g.HasNode(ctx, "z")
	if err != nil {
		t.Fatalf("HasNode error: %v", err)
	}
	if ok {
		t.Fatalf("expected HasNode(z) false")
	}

	// EdgeExists
	ex, err := g.EdgeExists(ctx, "a", "b")
	if err != nil || !ex {
		t.Fatalf("expected EdgeExists a->b true, got %v err %v", ex, err)
	}
	ex, err = g.EdgeExists(ctx, "b", "a")
	if err != nil {
		t.Fatalf("EdgeExists error: %v", err)
	}
	if ex {
		t.Fatalf("expected EdgeExists b->a false")
	}

	// NodeDegree for a: in=1 (d->a), out=2 (a->b,a->c)
	in, out, err := g.NodeDegree(ctx, "a")
	if err != nil {
		t.Fatalf("NodeDegree error: %v", err)
	}
	if in != 1 || out != 2 {
		t.Fatalf("unexpected degrees for a: got in=%d out=%d", in, out)
	}
}

func TestStreamingEnumerators(t *testing.T) {
	path := "./test-pebble-db-stream"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-stream")
	ctx := context.Background()

	// build graph: x->y, x->z, w->x
	_ = g.AddNode(ctx, "x", nil)
	_ = g.AddNode(ctx, "y", nil)
	_ = g.AddNode(ctx, "z", nil)
	_ = g.AddNode(ctx, "w", nil)
	_ = g.AddEdge(ctx, "x", "y", nil)
	_ = g.AddEdge(ctx, "x", "z", nil)
	_ = g.AddEdge(ctx, "w", "x", nil)

	// compare neighbors slice vs enumerator
	sl, err := g.Neighbors(ctx, "x")
	if err != nil {
		t.Fatalf("Neighbors error: %v", err)
	}
	var enumVals []Edge
	enum := g.EnumerateNeighbors(ctx, "x")
	if err := enumerators.ForEach(enum, func(e Edge) error {
		enumVals = append(enumVals, e)
		return nil
	}); err != nil {
		t.Fatalf("NeighborsEnumerate error: %v", err)
	}
	if len(sl) != len(enumVals) {
		t.Fatalf("neighbors mismatch: slice=%v enum=%v", sl, enumVals)
	}

	// compare incoming
	slIn, err := g.IncomingNeighbors(ctx, "x")
	if err != nil {
		t.Fatalf("IncomingNeighbors error: %v", err)
	}
	var enumIn []Edge
	enum2 := g.EnumerateIncomingNeighbors(ctx, "x")
	if err := enumerators.ForEach(enum2, func(e Edge) error {
		enumIn = append(enumIn, e)
		return nil
	}); err != nil {
		t.Fatalf("IncomingNeighborsEnumerate error: %v", err)
	}
	if len(slIn) != len(enumIn) {
		t.Fatalf("incoming neighbors mismatch: slice=%v enum=%v", slIn, enumIn)
	}
}

func TestBFSWithDepthAndCollect(t *testing.T) {
	path := "./test-pebble-db-bfs-depth"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-bfs-depth")
	ctx := context.Background()

	// Construct a small graph: a->b, a->c, b->d, c->e
	_ = g.AddNode(ctx, "a", nil)
	_ = g.AddNode(ctx, "b", nil)
	_ = g.AddNode(ctx, "c", nil)
	_ = g.AddNode(ctx, "d", nil)
	_ = g.AddNode(ctx, "e", nil)
	_ = g.AddEdge(ctx, "a", "b", nil)
	_ = g.AddEdge(ctx, "a", "c", nil)
	_ = g.AddEdge(ctx, "b", "d", nil)
	_ = g.AddEdge(ctx, "c", "e", nil)

	// Test BFSWithDepth collects depth info
	var seen []string
	if err := g.BFSWithDepth(ctx, "a", func(id string, depth int) error {
		seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
		return nil
	}, 0); err != nil {
		t.Fatalf("BFSWithDepth error: %v", err)
	}
	// Expected to see a:0 then b:1 and c:1 then d:2 and e:2 (order among same-level nodes depends on neighbors order)
	if len(seen) != 5 {
		t.Fatalf("unexpected seen list: %v", seen)
	}

	// Test CollectBFSIDs returns IDs
	ids, err := g.CollectBFSIDs(ctx, "a", 0)
	if err != nil {
		t.Fatalf("CollectBFSIDs error: %v", err)
	}
	if len(ids) != 5 {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestDFSWithDepthAndCollect(t *testing.T) {
	path := "./test-pebble-db-dfs-depth"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-dfs-depth")
	ctx := context.Background()

	// Construct a small graph: a->b, a->c, b->d, c->e
	_ = g.AddNode(ctx, "a", nil)
	_ = g.AddNode(ctx, "b", nil)
	_ = g.AddNode(ctx, "c", nil)
	_ = g.AddNode(ctx, "d", nil)
	_ = g.AddNode(ctx, "e", nil)
	_ = g.AddEdge(ctx, "a", "b", nil)
	_ = g.AddEdge(ctx, "a", "c", nil)
	_ = g.AddEdge(ctx, "b", "d", nil)
	_ = g.AddEdge(ctx, "c", "e", nil)

	// Test DFSWithDepth collects depth info
	var seen []string
	if err := g.DFSWithDepth(ctx, "a", func(id string, depth int) error {
		seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
		return nil
	}, 0); err != nil {
		t.Fatalf("DFSWithDepth error: %v", err)
	}
	// DFS will visit a first then go deep. Expect 5 nodes visited.
	if len(seen) != 5 {
		t.Fatalf("unexpected seen list for DFS: %v", seen)
	}
	// Test CollectDFSIDs returns IDs
	ids, err := g.CollectDFSIDs(ctx, "a", 0)
	if err != nil {
		t.Fatalf("CollectDFSIDs error: %v", err)
	}
	if len(ids) != 5 {
		t.Fatalf("unexpected ids for DFS: %v", ids)
	}
}

func TestBatchAddNodesAndEdges(t *testing.T) {
	path := "./test-pebble-db-batch"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		t.Fatalf("failed to open pebble: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(path)
	}()

	g := NewGraph(store, "test-batch")
	ctx := context.Background()

	nodes := []Node{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	edges := []Edge{{From: "n1", To: "n2"}, {From: "n2", To: "n3"}}

	if err := g.BatchAdd(ctx, nodes, edges); err != nil {
		t.Fatalf("BatchAdd failed: %v", err)
	}

	// Assert nodes exist
	for _, n := range nodes {
		ok, err := g.HasNode(ctx, n.ID)
		if err != nil || !ok {
			t.Fatalf("expected node %s to exist: %v", n.ID, err)
		}
	}

	// Assert edges
	out, err := g.Neighbors(ctx, "n1")
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(out) != 1 || out[0].To != "n2" {
		t.Fatalf("unexpected neighbors for n1: %v", out)
	}

	in, err := g.IncomingNeighbors(ctx, "n2")
	if err != nil {
		t.Fatalf("incoming: %v", err)
	}
	if len(in) != 1 || in[0].From != "n1" {
		t.Fatalf("unexpected incoming for n2: %v", in)
	}
}
