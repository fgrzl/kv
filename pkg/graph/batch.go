package graph

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Batch operations

// BatchAdd inserts nodes and edges into the store. It writes node entries
// and for each edge writes forward and reverse entries. It attempts a single
// atomic batch when supported and falls back to chunked commits.
func (g *graphStore) BatchAdd(ctx context.Context, nodes []Node, edges []Edge) error {
	slog.DebugContext(ctx, "starting batch add", "nodes", len(nodes), "edges", len(edges), "graph", g.name)
	// Pre-allocate with estimated capacity
	ops := make([]*kv.BatchItem, 0, len(nodes)+2*len(edges))

	// nodes
	for _, n := range nodes {
		sn := storedNode{ID: n.ID, Meta: encodeMeta(n.Meta)}
		b, err := json.Marshal(sn)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal node in batch", "graph", g.name, "id", n.ID, "err", err)
			return err
		}
		pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(n.ID))
		ops = append(ops, &kv.BatchItem{Op: kv.Put, PK: pk, Value: b})
	}

	// edges: for each edge, write forward and reverse
	for _, e := range edges {
		se := storedEdge{From: e.From, To: e.To, Meta: encodeMeta(e.Meta)}
		b, err := json.Marshal(se)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal edge in batch", "graph", g.name, "from", e.From, "to", e.To, "err", err)
			return err
		}
		pkF := lexkey.NewPrimaryKey(g.edgePartition(e.From), lexkey.Encode(e.To))
		pkR := lexkey.NewPrimaryKey(g.inEdgePartition(e.To), lexkey.Encode(e.From))
		ops = append(ops, &kv.BatchItem{Op: kv.Put, PK: pkF, Value: b})
		ops = append(ops, &kv.BatchItem{Op: kv.Put, PK: pkR, Value: b})
	}

	if len(ops) == 0 {
		slog.DebugContext(ctx, "batch add empty", "graph", g.name)
		return nil
	}

	// Chunked commits.
	// Azure Tables transactions allow up to 100 operations per submit, and the
	// Azure backend enforces this limit in kv.Batch.
	const chunkSize = 100
	for i := 0; i < len(ops); i += chunkSize {
		end := i + chunkSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := g.store.Batch(ctx, ops[i:end]); err != nil {
			slog.ErrorContext(ctx, "failed to commit batch chunk", "graph", g.name, "chunk_start", i, "chunk_end", end, "err", err)
			return err
		}
	}
	slog.DebugContext(ctx, "batch add completed", "operations", len(ops), "graph", g.name)
	return nil
}
