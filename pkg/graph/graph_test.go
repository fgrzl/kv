package graph

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/fgrzl/enumerators"

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
		_ = store.Close()
		_ = os.RemoveAll(path)
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

func TestShouldReturnErrorWhenAddingDuplicateNode(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "x", []byte("xmeta")))

	// Act
	err := g.AddNode(ctx, "x", []byte("xmeta2"))

	// Assert
	assert.Error(t, err)
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

func TestShouldReturnNeighborsCorrectly(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	nbrs, err := g.Neighbors(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, nbrs, 2)
	// Order may vary, check both
	ids := make([]string, len(nbrs))
	for i, e := range nbrs {
		ids[i] = e.To
	}
	assert.Contains(t, ids, "b")
	assert.Contains(t, ids, "c")
}

func TestShouldReturnIncomingNeighborsCorrectly(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "a", nil))
	require.NoError(t, g.AddEdge(ctx, "c", "a", nil))

	// Act
	in, err := g.IncomingNeighbors(ctx, "a")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, in, 2)
	ids := make([]string, len(in))
	for i, e := range in {
		ids[i] = e.From
	}
	assert.Contains(t, ids, "b")
	assert.Contains(t, ids, "c")
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

func TestShouldPerformBFSWithDepth(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddNode(ctx, "d", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "d", nil))

	// Act
	var seen []string
	err := g.BFSWithDepth(ctx, "a", func(id string, depth int) error {
		seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
		return nil
	}, 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, seen, 4)
	assert.Contains(t, seen, "a:0")
	assert.Contains(t, seen, "b:1")
	assert.Contains(t, seen, "c:1")
	assert.Contains(t, seen, "d:2")
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

func TestShouldPerformDFSWithDepth(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddNode(ctx, "d", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "d", nil))

	// Act
	var seen []string
	err := g.DFSWithDepth(ctx, "a", func(id string, depth int) error {
		seen = append(seen, fmt.Sprintf("%s:%d", id, depth))
		return nil
	}, 0)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, seen, 4)
	assert.Contains(t, seen, "a:0")
	assert.Contains(t, seen, "b:1")
	assert.Contains(t, seen, "c:1")
	assert.Contains(t, seen, "d:2")
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

func TestShouldEnumerateNeighbors(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "b", nil))
	require.NoError(t, g.AddEdge(ctx, "a", "c", nil))

	// Act
	var enumVals []Edge
	enum := g.EnumerateNeighbors(ctx, "a")
	err := enumerators.ForEach(enum, func(e Edge) error {
		enumVals = append(enumVals, e)
		return nil
	})

	// Assert
	assert.NoError(t, err)
	assert.Len(t, enumVals, 2)
	ids := make([]string, len(enumVals))
	for i, e := range enumVals {
		ids[i] = e.To
	}
	assert.Contains(t, ids, "b")
	assert.Contains(t, ids, "c")
}

func TestShouldEnumerateIncomingNeighbors(t *testing.T) {
	// Arrange
	g := setupGraph(t)
	ctx := context.Background()
	require.NoError(t, g.AddNode(ctx, "a", nil))
	require.NoError(t, g.AddNode(ctx, "b", nil))
	require.NoError(t, g.AddNode(ctx, "c", nil))
	require.NoError(t, g.AddEdge(ctx, "b", "a", nil))
	require.NoError(t, g.AddEdge(ctx, "c", "a", nil))

	// Act
	var enumVals []Edge
	enum := g.EnumerateIncomingNeighbors(ctx, "a")
	err := enumerators.ForEach(enum, func(e Edge) error {
		enumVals = append(enumVals, e)
		return nil
	})

	// Assert
	assert.NoError(t, err)
	assert.Len(t, enumVals, 2)
	ids := make([]string, len(enumVals))
	for i, e := range enumVals {
		ids[i] = e.From
	}
	assert.Contains(t, ids, "b")
	assert.Contains(t, ids, "c")
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
