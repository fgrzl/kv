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
		slog.ErrorContext(ctx, "failed to marshal node", "id", id, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	// Use Put to make this operation idempotent (succeeds even if node already exists).
	err = g.store.Put(ctx, &kv.Item{PK: pk, Value: b})
	if err != nil {
		slog.WarnContext(ctx, "failed to add node", "id", id, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	slog.InfoContext(ctx, "node added", "id", id, "graph", g.name)
	return nil
}

func (g *graphStore) GetNode(ctx context.Context, id string) (*Node, error) {
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get node", "id", id, "err", err)
		return nil, err
	}
	if item == nil {
		slog.DebugContext(ctx, "node not found", "id", id)
		return nil, nil
	}
	var sn storedNode
	if err := json.Unmarshal(item.Value, &sn); err != nil {
		slog.ErrorContext(ctx, "failed to unmarshal node", "id", id, "err", err)
		return nil, err
	}
	meta, err := decodeMeta(sn.Meta)
	if err != nil {
		slog.ErrorContext(ctx, "failed to decode node meta", "id", id, "err", err)
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
			slog.ErrorContext(ctx, "failed to flush delete batch", "node", id, "batch_size", len(batch), "err", err)
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

	// process outgoing edges (forward entries) streamingly
	outPart := g.edgePartition(id)
	outArgs := kv.QueryArgs{PartitionKey: outPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	outEnum := g.store.Enumerate(ctx, outArgs)
	if err := enumerators.ForEach(outEnum, func(it *kv.Item) error {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			// skip malformed entries
			slog.WarnContext(ctx, "skipping malformed edge during delete", "node", id, "err", err)
			return nil
		}
		pkF := lexkey.NewPrimaryKey(g.edgePartition(id), lexkey.Encode(se.To))
		pkR := lexkey.NewPrimaryKey(g.inEdgePartition(se.To), lexkey.Encode(id))
		batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: pkF})
		batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: pkR})
		if len(batch) >= chunkSize {
			return flush()
		}
		return nil
	}); err != nil {
		slog.ErrorContext(ctx, "failed to enumerate outgoing edges", "node", id, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// process incoming edges (reverse entries) streamingly
	inPart := g.inEdgePartition(id)
	inArgs := kv.QueryArgs{PartitionKey: inPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inEnum := g.store.Enumerate(ctx, inArgs)
	if err := enumerators.ForEach(inEnum, func(it *kv.Item) error {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal incoming edge", "node", id, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil
		}
		pkR := lexkey.NewPrimaryKey(g.inEdgePartition(id), lexkey.Encode(se.From))
		pkF := lexkey.NewPrimaryKey(g.edgePartition(se.From), lexkey.Encode(id))
		batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: pkR})
		batch = append(batch, &kv.BatchItem{Op: kv.Delete, PK: pkF})
		if len(batch) >= chunkSize {
			return flush()
		}
		return nil
	}); err != nil {
		slog.ErrorContext(ctx, "failed to enumerate incoming edges", "node", id, "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := flush(); err != nil {
		return err
	}
	slog.InfoContext(ctx, "node deleted", "id", id, "graph", g.name)
	return nil
}
