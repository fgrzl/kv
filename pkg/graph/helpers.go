package graph

import (
	"encoding/base64"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("github.com/fgrzl/kv/graph")

// storedNode represents a node in storage format with base64-encoded metadata.
type storedNode struct {
	ID   string `json:"id"`
	Meta string `json:"meta"` // base64 encoded
}

// storedEdge represents an edge in storage format with base64-encoded metadata.
type storedEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Meta string `json:"meta"`
}

// encodeMeta encodes metadata bytes to base64 string for storage.
func encodeMeta(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// decodeMeta decodes base64 string back to metadata bytes.
func decodeMeta(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }
