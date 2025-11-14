package quickstart

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	kvstore "github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pkg/graph"
	"github.com/fgrzl/kv/pkg/merkle"
	"github.com/fgrzl/kv/pkg/storage/azure"
	"github.com/fgrzl/kv/pkg/storage/pebble"
	"github.com/fgrzl/kv/pkg/storage/redis"
	"github.com/fgrzl/kv/pkg/timeseries"
)

// QuickStart provides dead-simple setup functions for common KV configurations.
// These functions use sensible defaults and minimal configuration to get you up and running quickly.

// NewPebbleKV creates a new Pebble-backed KV store with optimized defaults.
// Uses the provided path, or creates a temporary directory if path is empty.
func NewPebbleKV(path string) (kvstore.KV, error) {
	if path == "" {
		// Create temp directory for pebble
		tempDir, err := os.MkdirTemp("", "kv-pebble-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp directory: %w", err)
		}
		path = filepath.Join(tempDir, "db")
		slog.Info("using temporary pebble database", "path", path)
	}

	store, err := pebble.NewPebbleStore(path,
		pebble.WithMemTableSize(64<<20), // 64MB
		pebble.WithMaxOpenFiles(1000),
		pebble.WithMaxConcurrentCompactions(func() int { return 4 }),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble store: %w", err)
	}

	slog.Info("pebble KV store created", "path", path)
	return kvstore.NewInstrumentedKV(store, "pebble"), nil
}

// NewRedisKV creates a new Redis-backed KV store with optimized defaults.
// Uses localhost:6379 if addr is empty.
func NewRedisKV(addr string) (kvstore.KV, error) {
	if addr == "" {
		addr = "localhost:6379"
	}

	store, err := redis.NewRedisStore(
		redis.WithAddress(addr),
		redis.WithDatabase(0),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis store: %w", err)
	}

	slog.Info("redis KV store created", "addr", addr)
	return kvstore.NewInstrumentedKV(store, "redis"), nil
}

// NewAzureKV creates a new Azure Table-backed KV store.
// Uses environment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY.
// Table name defaults to "kvstore" if empty.
func NewAzureKV(tableName string) (kvstore.KV, error) {
	if tableName == "" {
		tableName = "kvstore"
	}

	account := os.Getenv("AZURE_STORAGE_ACCOUNT")
	key := os.Getenv("AZURE_STORAGE_KEY")

	if account == "" || key == "" {
		return nil, fmt.Errorf("AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY environment variables must be set")
	}

	cred, err := azure.NewSharedKeyCredential(account, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure credential: %w", err)
	}

	store, err := azure.NewAzureStore(
		azure.WithTable(tableName),
		azure.WithSharedKey(cred),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure store: %w", err)
	}

	slog.Info("azure KV store created", "account", account, "table", tableName)
	return kvstore.NewInstrumentedKV(store, "azure"), nil
}

// NewGraph creates a complete graph setup with Pebble backend.
// Uses a temporary database if path is empty.
func NewGraph(name, pebblePath string) (graph.Graph, error) {
	kv, err := NewPebbleKV(pebblePath)
	if err != nil {
		return nil, err
	}

	g := graph.NewGraph(kv, name)
	slog.Info("graph overlay created", "name", name)
	return g, nil
}

// NewTimeSeries creates a complete timeseries setup with Pebble backend.
// Uses a temporary database if path is empty.
func NewTimeSeries(name, pebblePath string) (*timeseries.TimeSeries, error) {
	kv, err := NewPebbleKV(pebblePath)
	if err != nil {
		return nil, err
	}

	ts := timeseries.New(kv, name)
	slog.Info("timeseries overlay created", "name", name)
	return ts, nil
}

// NewMerkleTree creates a complete merkle tree setup with Pebble backend.
// Uses a temporary database if path is empty.
func NewMerkleTree(pebblePath string) (*merkle.Tree, error) {
	kv, err := NewPebbleKV(pebblePath)
	if err != nil {
		return nil, err
	}

	tree := merkle.NewTree(kv)
	slog.Info("merkle tree overlay created")
	return tree, nil
}

// QuickStartExamples demonstrates common usage patterns.
func QuickStartExamples() {
	// Example 1: Simple KV store
	_, _ = NewPebbleKV("") // temporary database
	// Use kv for basic operations...

	// Example 2: Graph database
	_, _ = NewGraph("mygraph", "")
	// Use graph for node/edge operations...

	// Example 3: Time series
	_, _ = NewTimeSeries("metrics", "")
	// Use ts for timestamped data...

	// Example 4: Merkle tree
	_, _ = NewMerkleTree("")
	// Use tree for verifiable data structures...
}
