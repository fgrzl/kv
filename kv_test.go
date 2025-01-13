package kv_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/kv/pebble"
	"github.com/fgrzl/kv/sqlite"
	"github.com/google/uuid"
)

var implementations = []string{"pebble", "sqlite"}

func getKVDB(t *testing.T, dbType string) kv.KV {
	var db kv.KV
	var err error
	var dbPath string

	// Create a temporary directory for the test database
	tempDir := os.TempDir()

	// Determine the database type and initialize the appropriate DB
	switch dbType {
	case "sqlite":
		// Use a temporary file for SQLite DB
		dbPath = filepath.Join(tempDir, fmt.Sprintf("db_%v.sqlite", uuid.NewString()))
		// Initialize SQLite implementation
		db, err = sqlite.NewSQLiteStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to initialize SQLite DB: %v", err)
		}
	case "pebble":
		// Use a temporary directory for Pebble DB
		dbPath = filepath.Join(tempDir, fmt.Sprintf("db_%v.pebble", uuid.NewString()))
		// Initialize Pebble implementation
		db, err = pebble.NewPebbleStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to initialize Pebble DB: %v", err)
		}
	default:
		t.Fatalf("Unknown database type: %s", dbType)
	}

	// Register the cleanup function to remove the database files after tests
	t.Cleanup(func() {
		// Cleanup logic based on the database type
		if dbType == "sqlite" {
			// Remove the SQLite test database file
			err := os.Remove(dbPath)
			if err != nil {
				t.Errorf("Failed to remove SQLite test database: %v", err)
			}
		} else if dbType == "pebble" {
			// Remove the Pebble test database directory
			err := os.RemoveAll(dbPath)
			if err != nil {
				t.Errorf("Failed to remove Pebble test database: %v", err)
			}
		}
	})

	return db
}
