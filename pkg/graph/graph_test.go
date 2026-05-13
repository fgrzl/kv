package graph

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"

	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupGraph(t *testing.T) Graph {
	// create a temp pebble DB
	path := "./test-pebble-db"
	_ = os.RemoveAll(path)
	store, err := pebble.NewPebbleStore(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove test db: %v", err)
		}
	})
	return NewGraph(store, "test")
}

func TestShouldAddNodeSuccessfully(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Act
	err := g.AddNode(ctx, "a", []byte("A-meta"))

	// Assert
	assert.NoError(t, err)
}

func TestShouldAddNodeIdempotently(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "x", []byte("xmeta")))

	// Act
	err := g.AddNode(ctx, "x", []byte("xmeta2"))

	// Assert
	assert.NoError(t, err)
	node, err := g.GetNode(ctx, "x")
	assert.NoError(t, err)
	assert.Equal(t, []byte("xmeta2"), node.Meta) // Updated metadata
}

func TestShouldGetNodeSuccessfully(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", []byte("meta")))

	// Act
	node, err := g.GetNode(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "a", node.ID)
	assert.Equal(t, []byte("meta"), node.Meta)
}

func TestShouldReturnNilWhenGettingNonExistentNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Act
	node, err := g.GetNode(ctx, "nonexistent")

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, node)
}

func TestShouldDeleteNodeAndCascadeEdges(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "c", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "a", nil))

	// Act
	err := g.DeleteNode(ctx, "b")

	// Assert
	assert.NoError(t, err)
	node, err := g.GetNode(ctx, "b")
	assert.NoError(t, err)
	assert.Nil(t, node)
	// Edges should be removed
	nbrsA, err := g.Neighbors(ctx, "a")
	assert.NoError(t, err)
	assert.Len(t, nbrsA, 0)
	inC, err := g.IncomingNeighbors(ctx, "c")
	assert.NoError(t, err)
	assert.Len(t, inC, 0)
}

func TestShouldDeleteNodeIdempotently(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))

	// Act
	err := g.DeleteNode(ctx, "a")
	assert.NoError(t, err)
	// Delete again - should not error
	err = g.DeleteNode(ctx, "a")

	// Assert
	assert.NoError(t, err)
}

func TestShouldAddEdgeSuccessfully(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))

	// Act
	err := g.AddEdge(ctx, "a", "b", []byte("edge-meta"))

	// Assert
	assert.NoError(t, err)
}

func TestShouldAddEdgeIdempotently(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))

	// Act
	err := g.AddEdge(ctx, "a", "b", []byte("edge-meta-1"))
	assert.NoError(t, err)
	// Add again with different metadata - should succeed
	err = g.AddEdge(ctx, "a", "b", []byte("edge-meta-2"))

	// Assert
	assert.NoError(t, err)
	nbrs, err := g.Neighbors(ctx, "a")
	assert.NoError(t, err)
	assert.Len(t, nbrs, 1)
	assert.Equal(t, []byte("edge-meta-2"), nbrs[0].Meta) // Updated metadata
}

func TestShouldRemoveEdgeSuccessfully(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))

	// Act
	err := g.RemoveEdge(ctx, "a", "b")

	// Assert
	assert.NoError(t, err)
	nbrs, err := g.Neighbors(ctx, "a")
	assert.NoError(t, err)
	assert.Len(t, nbrs, 0)
	in, err := g.IncomingNeighbors(ctx, "b")
	assert.NoError(t, err)
	assert.Len(t, in, 0)
}

func TestShouldRemoveEdgeIdempotently(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))

	// Act
	err := g.RemoveEdge(ctx, "a", "b")
	assert.NoError(t, err)
	// Remove again - should not error
	err = g.RemoveEdge(ctx, "a", "b")

	// Assert
	assert.NoError(t, err)
}

func TestShouldReturnNeighborsOrIncoming(t *testing.T) {
	cases := []struct {
		name      string
		edges     [][2]string
		query     string
		incoming  bool
		wantField func(Edge) string
	}{
		{
			name:  "outgoing",
			edges: [][2]string{{"a", "b"}, {"a", "c"}},
			query: "a", incoming: false,
			wantField: func(e Edge) string { return e.To },
		},
		{
			name:  "incoming",
			edges: [][2]string{{"b", "a"}, {"c", "a"}},
			query: "a", incoming: true,
			wantField: func(e Edge) string { return e.From },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := setupGraph(t)
			ctx := context.Background()
			for _, id := range []string{"a", "b", "c"} {
				require.NoError(t, g.AddNode(ctx, id, nil))
			}
			for _, e := range tc.edges {
				require.NoError(t, g.AddEdge(ctx, e[0], e[1], nil))
			}
			var edges []Edge
			var err error
			if tc.incoming {
				edges, err = g.IncomingNeighbors(ctx, tc.query)
			} else {
				edges, err = g.Neighbors(ctx, tc.query)
			}
			assert.NoError(t, err)
			assert.Len(t, edges, 2)
			ids := make([]string, len(edges))
			for i, e := range edges {
				ids[i] = tc.wantField(e)
			}
			assert.Contains(t, ids, "b")
			assert.Contains(t, ids, "c")
		})
	}
}

func TestShouldPerformBFSTraversal(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "c", nil))

	// Act
	var order []string
	err := g.BFS(ctx, "a", func(id string) error {
		order = append(order, id)
		return nil
	}, 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, order, 3)
	assert.Equal(t, "a", order[0])
	assert.Contains(t, order[1:], "b")
	assert.Contains(t, order[1:], "c")
}

func TestShouldPerformWalkWithDepth(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T) Graph {
		g := setupGraph(t)
		require.NoError(t, g.AddNode(ctx, "a", nil))
		require.NoError(t, g.AddNode(ctx, "b", nil))
		require.NoError(t, g.AddNode(ctx, "c", nil))
		require.NoError(t, g.AddNode(ctx, "d", nil))
		require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
		require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
		require.NoError(t, g.AddEdge(ctx, "b", "d", nil))
		return g
	}
	cases := []struct {
		name string
		run  func(Graph, context.Context, func(string, int) error) error
	}{
		{"BFS", func(g Graph, ctx context.Context, cb func(string, int) error) error {
			return g.BFSWithDepth(ctx, "a", cb, 0)
		}},
		{"DFS", func(g Graph, ctx context.Context, cb func(string, int) error) error {
			return g.DFSWithDepth(ctx, "a", cb, 0)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := build(t)
			var seen []string
			err := tc.run(g, ctx, func(id string, depth int) error {
				seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
				return nil
			})
			assert.NoError(t, err)
			assert.Len(t, seen, 4)
			assert.Contains(t, seen, "a:0")
			assert.Contains(t, seen, "b:1")
			assert.Contains(t, seen, "c:1")
			assert.Contains(t, seen, "d:2")
		})
	}
}

func TestShouldCollectBFSIDs(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	ids, err := g.CollectBFSIDs(ctx, "a", 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Equal(t, "a", ids[0])
	assert.Contains(t, ids[1:], "b")
	assert.Contains(t, ids[1:], "c")
}

func TestShouldPerformDFSTraversal(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	var order []string
	err := g.DFS(ctx, "a", func(id string) error {
		order = append(order, id)
		return nil
	}, 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, order, 3)
	assert.Equal(t, "a", order[0])
	assert.Contains(t, order, "b")
	assert.Contains(t, order, "c")
}

func TestShouldCollectDFSIDs(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	ids, err := g.CollectDFSIDs(ctx, "a", 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Equal(t, "a", ids[0])
	assert.Contains(t, ids, "b")
	assert.Contains(t, ids, "c")
}

func TestShouldCheckNodeExistence(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))

	// Act & Assert
	exists, err := g.HasNode(ctx, "a")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = g.HasNode(ctx, "nonexistent")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestShouldCheckEdgeExistence(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))

	// Act & Assert
	exists, err := g.EdgeExists(ctx, "a", "b")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = g.EdgeExists(ctx, "b", "a")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestShouldReturnNodeDegree(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddNode(ctx, "d", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
	require.NoError(t, g.AddEdge(ctx, "d", "a", nil))

	// Act
	in, out, err := g.NodeDegree(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, in)
	assert.Equal(t, 2, out)
}

func TestShouldEnumerateNeighborsOrIncoming(t *testing.T) {
	cases := []struct {
		name     string
		edges    [][2]string
		query    string
		incoming bool
		endpoint func(Edge) string
	}{
		{"outgoing", [][2]string{{"a", "b"}, {"a", "c"}}, "a", false, func(e Edge) string { return e.To }},
		{"incoming", [][2]string{{"b", "a"}, {"c", "a"}}, "a", true, func(e Edge) string { return e.From }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := setupGraph(t)
			ctx := context.Background()
			for _, id := range []string{"a", "b", "c"} {
				require.NoError(t, g.AddNode(ctx, id, nil))
			}
			for _, e := range tc.edges {
				require.NoError(t, g.AddEdge(ctx, e[0], e[1], nil))
			}
			var enumVals []Edge
			var enum enumerators.Enumerator[Edge]
			if tc.incoming {
				enum = g.EnumerateIncomingNeighbors(ctx, tc.query)
			} else {
				enum = g.EnumerateNeighbors(ctx, tc.query)
			}
			err := enumerators.ForEach(enum, func(e Edge) error {
				enumVals = append(enumVals, e)
				return nil
			})
			assert.NoError(t, err)
			assert.Len(t, enumVals, 2)
			ids := make([]string, len(enumVals))
			for i, e := range enumVals {
				ids[i] = tc.endpoint(e)
			}
			assert.Contains(t, ids, "b")
			assert.Contains(t, ids, "c")
		})
	}
}

func TestShouldBatchAddNodesAndEdges(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	nodes := []Node{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	edges := []Edge{{From: "n1", To: "n2"}, {From: "n2", To: "n3"}}

	// Act
	err := g.BatchAdd(ctx, nodes, edges)

	// Assert
	assert.NoError(t, err)
	for _, n := range nodes {
		exists, err := g.HasNode(ctx, n.ID)
		assert.NoError(t, err)
		assert.True(t, exists)
	}
	out, err := g.Neighbors(ctx, "n1")
	assert.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, "n2", out[0].To)
	in, err := g.IncomingNeighbors(ctx, "n2")
	assert.NoError(t, err)
	assert.Len(t, in, 1)
	assert.Equal(t, "n1", in[0].From)
}

func TestShouldHandleJSONMarshalErrorInAddNode(t *testing.T) {
	// This test is tricky because json.Marshal rarely fails for our struct.
	// We'll skip this for now as it's hard to trigger without reflection tricks.
	t.Skip("json.Marshal error in AddNode is difficult to test without reflection")
}

func TestShouldHandleJSONUnmarshalErrorInGetNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Manually insert invalid JSON data
	pk := lexkey.NewPrimaryKey(g.(*graphStore).nodePartition(), lexkey.Encode("bad-node"))
	invalidJSON := []byte(`{"id": "bad-node", "meta": "invalid-json"}`) // meta should be base64, but we'll make it invalid JSON
	require.NoError(t, g.(*graphStore).store.Insert(ctx, &kv.Item{PK: pk, Value: invalidJSON}))

	// Act
	node, err := g.GetNode(ctx, "bad-node")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestShouldHandleDecodeMetaErrorInGetNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Manually insert data with invalid base64 meta
	pk := lexkey.NewPrimaryKey(g.(*graphStore).nodePartition(), lexkey.Encode("bad-meta-node"))
	invalidBase64 := []byte(`{"id": "bad-meta-node", "meta": "not-base64!@#"}`)
	require.NoError(t, g.(*graphStore).store.Insert(ctx, &kv.Item{PK: pk, Value: invalidBase64}))

	// Act
	node, err := g.GetNode(ctx, "bad-meta-node")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestShouldHandleMalformedEdgeDataInDeleteNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "test-node", nil))

	// Manually insert invalid JSON in edge partition
	edgePart := g.(*graphStore).edgePartition("test-node")
	pk := lexkey.NewPrimaryKey(edgePart, lexkey.Encode("some-target"))
	invalidJSON := []byte(`invalid json`)
	require.NoError(t, g.(*graphStore).store.Insert(ctx, &kv.Item{PK: pk, Value: invalidJSON}))

	// Act - should not crash, should skip malformed entries
	err := g.DeleteNode(ctx, "test-node")

	// Assert
	assert.NoError(t, err)
}

func TestShouldRespectBFSLimit(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "c", nil))

	// Act
	var order []string
	err := g.BFS(ctx, "a", func(id string) error {
		order = append(order, id)
		return nil
	}, 2) // limit to 2 nodes

	// Assert
	assert.NoError(t, err)
	assert.Len(t, order, 2)
	assert.Equal(t, "a", order[0])
	// Should visit either b or c, but not both due to limit
	assert.Contains(t, []string{"b", "c"}, order[1])
}

func TestShouldRespectWalkWithDepthLimit(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T) Graph {
		g := setupGraph(t)
		require.NoError(t, g.AddNode(ctx, "a", nil))
		require.NoError(t, g.AddNode(ctx, "b", nil))
		require.NoError(t, g.AddNode(ctx, "c", nil))
		require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
		require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
		return g
	}
	cases := []struct {
		name string
		run  func(Graph, context.Context, func(string, int) error) error
	}{
		{"BFS", func(g Graph, ctx context.Context, cb func(string, int) error) error {
			return g.BFSWithDepth(ctx, "a", cb, 2)
		}},
		{"DFS", func(g Graph, ctx context.Context, cb func(string, int) error) error {
			return g.DFSWithDepth(ctx, "a", cb, 2)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := build(t)
			var seen []string
			err := tc.run(g, ctx, func(id string, depth int) error {
				seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
				return nil
			})
			assert.NoError(t, err)
			assert.Len(t, seen, 2)
			assert.Contains(t, seen, "a:0")
			depth1Nodes := make([]string, 0)
			for _, s := range seen {
				if strings.HasSuffix(s, ":1") {
					depth1Nodes = append(depth1Nodes, s)
				}
			}
			assert.Len(t, depth1Nodes, 1)
			assert.Contains(t, []string{"b:1", "c:1"}, depth1Nodes[0])
		})
	}
}

func TestShouldHandleMalformedEdgeDataInNeighborsOrIncoming(t *testing.T) {
	cases := []struct {
		name       string
		incoming   bool
		query      string
		wantFromTo string
	}{
		{"neighbors", false, "a", "b"},
		{"incoming_neighbors", true, "b", "a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := setupGraph(t)
			ctx := context.Background()
			require.NoError(t, g.AddNode(ctx, "a", nil))
			require.NoError(t, g.AddNode(ctx, "b", nil))
			require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
			var pk lexkey.PrimaryKey
			if tc.incoming {
				inEdgePart := g.(*graphStore).inEdgePartition("b")
				pk = lexkey.NewPrimaryKey(inEdgePart, lexkey.Encode("malformed-from"))
			} else {
				edgePart := g.(*graphStore).edgePartition("a")
				pk = lexkey.NewPrimaryKey(edgePart, lexkey.Encode("malformed-target"))
			}
			invalidJSON := []byte(`invalid json`)
			require.NoError(t, g.(*graphStore).store.Insert(ctx, &kv.Item{PK: pk, Value: invalidJSON}))
			if tc.incoming {
				in, err := g.IncomingNeighbors(ctx, tc.query)
				assert.NoError(t, err)
				assert.Len(t, in, 1)
				assert.Equal(t, tc.wantFromTo, in[0].From)
			} else {
				nbrs, err := g.Neighbors(ctx, tc.query)
				assert.NoError(t, err)
				assert.Len(t, nbrs, 1)
				assert.Equal(t, tc.wantFromTo, nbrs[0].To)
			}
		})
	}
}

func TestShouldHandleContextCancellationDuringBFSTraversal(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	var order []string
	err := g.DFS(ctx, "a", func(id string) error {
		order = append(order, id)
		return nil
	}, 2) // limit to 2 nodes

	// Assert
	assert.NoError(t, err)
	assert.Len(t, order, 2)
	assert.Equal(t, "a", order[0])
	// DFS order may vary, but should visit 2 nodes total
	assert.Contains(t, []string{"b", "c"}, order[1])
}

func TestShouldRespectDFSLimit(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "c", nil))

	// Act - cancel context after visiting first node
	visited := make([]string, 0)
	err := g.BFS(ctx, "a", func(id string) error {
		visited = append(visited, id)
		if id == "a" {
			cancel() // Cancel after visiting start node
		}
		return nil
	}, 0)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Contains(t, visited, "a") // Should have visited at least the start node
}

func TestShouldHandleBFSTraversalFromNonExistentNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Act
	visited := make([]string, 0)
	err := g.BFS(ctx, "nonexistent", func(id string) error {
		visited = append(visited, id)
		return nil
	}, 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, visited, 1)
	assert.Equal(t, "nonexistent", visited[0])
}

func TestShouldAllowSelfLoops(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "a", []byte("self-loop")))

	// Act
	nbrs, err := g.Neighbors(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, nbrs, 1)
	assert.Equal(t, "a", nbrs[0].To)
	assert.Equal(t, []byte("self-loop"), nbrs[0].Meta)
}

func TestShouldHandleDuplicateEdges(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", []byte("first")))
	require.NoError(t, g.AddEdge(ctx, "a", "b", []byte("second"))) // Overwrites

	// Act
	nbrs, err := g.Neighbors(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, nbrs, 1)
	assert.Equal(t, "b", nbrs[0].To)
	assert.Equal(t, []byte("second"), nbrs[0].Meta) // Last one wins
}

func TestShouldHandleEmptyBatchAdd(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()

	// Act
	err := g.BatchAdd(ctx, []Node{}, []Edge{})

	// Assert
	assert.NoError(t, err)
}

func TestShouldHandleBatchAddWithOnlyNodes(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	nodes := []Node{{ID: "n1"}, {ID: "n2"}}

	// Act
	err := g.BatchAdd(ctx, nodes, []Edge{})

	// Assert
	assert.NoError(t, err)
	for _, n := range nodes {
		exists, err := g.HasNode(ctx, n.ID)
		assert.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestShouldHandleBatchAddWithOnlyEdges(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	edges := []Edge{{From: "a", To: "b"}}

	// Act
	err := g.BatchAdd(ctx, []Node{}, edges)

	// Assert
	assert.NoError(t, err)
	nbrs, err := g.Neighbors(ctx, "a")
	assert.NoError(t, err)
	assert.Len(t, nbrs, 1)
	assert.Equal(t, "b", nbrs[0].To)
}
