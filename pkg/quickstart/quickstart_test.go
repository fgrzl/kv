package quickstart

import (
	"context"
	"testing"

	"github.com/fgrzl/enumerators"
	kvstore "github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/merkle"
	"github.com/fgrzl/lexkey"
)

func TestNewPebbleKV(t *testing.T) {
	kv, err := NewPebbleKV("")
	if err != nil {
		t.Fatalf("NewPebbleKV failed: %v", err)
	}
	defer kv.Close()

	// Test basic operations
	pk := lexkey.NewPrimaryKey(lexkey.Encode("test"), lexkey.Encode("key"))
	item := &kvstore.Item{PK: pk, Value: []byte("value")}

	err = kv.Put(context.Background(), item)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := kv.Get(context.Background(), pk)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved == nil || string(retrieved.Value) != "value" {
		t.Fatalf("Retrieved item mismatch: %+v", retrieved)
	}
}

func TestNewGraph(t *testing.T) {
	graph, err := NewGraph("testgraph", "")
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}

	// Test basic graph operations
	err = graph.AddNode(context.Background(), "node1", []byte("data"))
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	node, err := graph.GetNode(context.Background(), "node1")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}

	if node == nil || node.ID != "node1" {
		t.Fatalf("Retrieved node mismatch: %+v", node)
	}
}

func TestNewTimeSeries(t *testing.T) {
	ts, err := NewTimeSeries("testts", "")
	if err != nil {
		t.Fatalf("NewTimeSeries failed: %v", err)
	}

	// Test basic time series operations
	err = ts.Append(context.Background(), "metric1", 1234567890, []byte("42.5"))
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	samples, err := ts.QueryRange(context.Background(), "metric1", 1234567800, 1234567900)
	if err != nil {
		t.Fatalf("QueryRange failed: %v", err)
	}

	if len(samples) != 1 || samples[0].Series != "metric1" {
		t.Fatalf("Retrieved samples mismatch: %+v", samples)
	}
}

func TestNewMerkleTree(t *testing.T) {
	tree, err := NewMerkleTree("")
	if err != nil {
		t.Fatalf("NewMerkleTree failed: %v", err)
	}

	// Test basic merkle tree operations
	leaves := []merkle.Leaf{
		{Ref: "ref1", Hash: []byte("hash1")},
		{Ref: "ref2", Hash: []byte("hash2")},
	}

	err = tree.Build(context.Background(), "test", "space", enumerators.Slice(leaves))
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
}
