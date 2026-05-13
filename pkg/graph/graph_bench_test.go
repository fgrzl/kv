package graph

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv/pkg/storage/pebble"
)

// setupBenchGraph creates a graph for benchmarks.
func setupBenchGraph(b *testing.B) Graph {
	path := filepath.Join(b.TempDir(), "bench-graph")
	store, err := pebble.NewPebbleStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close store: %v", err)
		}
	})
	return NewGraph(store, "bench")
}

// BenchmarkAddNode measures node addition performance.
func BenchmarkAddNode(b *testing.B) {
	g := setupBenchGraph(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.AddNode(ctx, fmt.Sprintf("node-%d", i), []byte("meta")); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBFS measures BFS traversal performance on a small graph.
func BenchmarkBFS(b *testing.B) {
	g := setupBenchGraph(b)
	ctx := context.Background()

	// Build a small graph: 10 nodes in a line
	for i := 0; i < 10; i++ {
		if err := g.AddNode(ctx, fmt.Sprintf("n%d", i), nil); err != nil {
			b.Fatal(err)
		}
		if i > 0 {
			if err := g.AddEdge(ctx, fmt.Sprintf("n%d", i-1), fmt.Sprintf("n%d", i), nil); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		if err := g.BFS(ctx, "n0", func(id string) error {
			count++
			return nil
		}, 0); err != nil {
			b.Fatal(err)
		}
		if count != 10 {
			b.Fatalf("Expected 10 nodes, got %d", count)
		}
	}
}
