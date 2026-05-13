package graph

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Node operations

func (g *graphStore) AddNode(ctx context.Context, id string, meta []byte) error {
	ctx, span := tracer.Start(ctx, "graph.AddNode",
		trace.WithAttributes(
			attribute.String("graph", g.name),
			attribute.String("node_id", id),
			attribute.Int("meta_size", len(meta)),
		))
	defer span.End()

	sn := storedNode{ID: id, Meta: encodeMeta(meta)}
	b, err := json.Marshal(sn)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal node", "id", id, "graph", g.name, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	// Use Put to make this operation idempotent (succeeds even if node already exists).
	err = g.store.Put(ctx, &kv.Item{PK: pk, Value: b})
	if err != nil {
		slog.ErrorContext(ctx, "failed to add node", "id", id, "graph", g.name, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	slog.DebugContext(ctx, "node added", "id", id, "graph", g.name)
	return nil
}

func (g *graphStore) GetNode(ctx context.Context, id string) (*Node, error) {
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get node", "id", id, "graph", g.name, "err", err)
		return nil, err
	}
	if item == nil {
		slog.DebugContext(ctx, "node not found", "id", id, "graph", g.name)
		return nil, nil
	}
	var sn storedNode
	if err := json.Unmarshal(item.Value, &sn); err != nil {
		slog.ErrorContext(ctx, "failed to unmarshal node", "id", id, "graph", g.name, "err", err)
		return nil, err
	}
	meta, err := decodeMeta(sn.Meta)
	if err != nil {
		slog.ErrorContext(ctx, "failed to decode node meta", "id", id, "graph", g.name, "err", err)
		return nil, err
	}
	return &Node{ID: sn.ID, Meta: meta}, nil
}

func (g *graphStore) DeleteNode(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "graph.DeleteNode",
		trace.WithAttributes(
			attribute.String("graph", g.name),
			attribute.String("node_id", id),
		))
	defer span.End()

	// We'll stream deletes using enumerators to avoid materializing large slices.
	// Azure Tables transactions allow up to 100 operations per submit.
	const chunkSize = 100

	// Pre-allocate batch with reasonable capacity
	batch := make([]*kv.BatchItem, 0, chunkSize)

	// helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := g.store.Batch(ctx, batch); err != nil {
			slog.ErrorContext(ctx, "failed to flush delete batch", "node", id, "graph", g.name, "batch_size", len(batch), "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		batch = batch[:0]
		return nil
	}

	// delete the node entry itself
	nodePK := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: nodePK})

	if err := g.forEachEdgeDeleteBatch(ctx, id, true, &batch, chunkSize, flush); err != nil {
		slog.ErrorContext(ctx, "failed to enumerate outgoing edges", "node", id, "graph", g.name, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := g.forEachEdgeDeleteBatch(ctx, id, false, &batch, chunkSize, flush); err != nil {
		slog.ErrorContext(ctx, "failed to enumerate incoming edges", "node", id, "graph", g.name, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := flush(); err != nil {
		return err
	}
	slog.DebugContext(ctx, "node deleted", "id", id, "graph", g.name)
	return nil
}

func (g *graphStore) forEachEdgeDeleteBatch(ctx context.Context, id string, outgoing bool, batch *[]*kv.BatchItem, chunkSize int, flush func() error) error {
	part := g.inEdgePartition(id)
	if outgoing {
		part = g.edgePartition(id)
	}
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	enum := g.store.Enumerate(ctx, args)
	return enumerators.ForEach(enum, func(it *kv.Item) error {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			kind := "incoming"
			if outgoing {
				kind = "outgoing"
			}
			slog.WarnContext(ctx, "skipping malformed edge during delete", "kind", kind, "node", id, "graph", g.name, "err", err)
			return nil
		}
		var first, second lexkey.PrimaryKey
		if outgoing {
			first = lexkey.NewPrimaryKey(g.edgePartition(id), lexkey.Encode(se.To))
			second = lexkey.NewPrimaryKey(g.inEdgePartition(se.To), lexkey.Encode(id))
		} else {
			first = lexkey.NewPrimaryKey(g.inEdgePartition(id), lexkey.Encode(se.From))
			second = lexkey.NewPrimaryKey(g.edgePartition(se.From), lexkey.Encode(id))
		}
		*batch = append(*batch, &kv.BatchItem{Op: kv.Delete, PK: first}, &kv.BatchItem{Op: kv.Delete, PK: second})
		if len(*batch) >= chunkSize {
			return flush()
		}
		return nil
	})
}
