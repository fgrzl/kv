package graph

import (
	"context"
	"encoding/json"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Query operations

func (g *graphStore) Neighbors(ctx context.Context, from string) ([]Edge, error) {
	partition := g.edgePartition(from)
	args := kv.QueryArgs{PartitionKey: partition, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := g.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		return nil, err
	}
	out := make([]Edge, 0, len(items)) // pre-allocate with known capacity
	for _, it := range items {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			// skip malformed edge records
			continue
		}
		meta, _ := decodeMeta(se.Meta)
		out = append(out, Edge{From: se.From, To: se.To, Meta: meta})
	}
	return out, nil
}

// IncomingNeighbors returns edges that point to `to` (i.e., incoming edges).
func (g *graphStore) IncomingNeighbors(ctx context.Context, to string) ([]Edge, error) {
	partition := g.inEdgePartition(to)
	args := kv.QueryArgs{PartitionKey: partition, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	items, err := g.store.Query(ctx, args, kv.Ascending)
	if err != nil {
		return nil, err
	}
	out := make([]Edge, 0, len(items)) // pre-allocate
	for _, it := range items {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			continue
		}
		meta, _ := decodeMeta(se.Meta)
		out = append(out, Edge{From: se.From, To: se.To, Meta: meta})
	}
	return out, nil
}

// NodeDegree returns the incoming and outgoing edge counts for a node.
func (g *graphStore) NodeDegree(ctx context.Context, id string) (int, int, error) {
	var outCount int
	outPart := g.edgePartition(id)
	outArgs := kv.QueryArgs{PartitionKey: outPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	outEnum := g.store.Enumerate(ctx, outArgs)
	defer outEnum.Dispose()
	if err := enumerators.ForEach(outEnum, func(_ *kv.Item) error {
		outCount++
		return nil
	}); err != nil {
		return 0, 0, err
	}

	var inCount int
	inPart := g.inEdgePartition(id)
	inArgs := kv.QueryArgs{PartitionKey: inPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inEnum := g.store.Enumerate(ctx, inArgs)
	defer inEnum.Dispose()
	if err := enumerators.ForEach(inEnum, func(_ *kv.Item) error {
		inCount++
		return nil
	}); err != nil {
		return 0, 0, err
	}

	return inCount, outCount, nil
}

// EnumerateNeighbors streams outgoing neighbors for `from`.
func (g *graphStore) EnumerateNeighbors(ctx context.Context, from string) enumerators.Enumerator[Edge] {
	part := g.edgePartition(from)
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inner := g.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (Edge, bool, error) {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			return Edge{}, false, nil
		}
		meta, _ := decodeMeta(se.Meta)
		return Edge{From: se.From, To: se.To, Meta: meta}, true, nil
	})
}

// EnumerateIncomingNeighbors streams incoming neighbors for `to`.
func (g *graphStore) EnumerateIncomingNeighbors(ctx context.Context, to string) enumerators.Enumerator[Edge] {
	part := g.inEdgePartition(to)
	args := kv.QueryArgs{PartitionKey: part, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inner := g.store.Enumerate(ctx, args)
	return enumerators.FilterMap(inner, func(it *kv.Item) (Edge, bool, error) {
		var se storedEdge
		if err := json.Unmarshal(it.Value, &se); err != nil {
			return Edge{}, false, nil
		}
		meta, _ := decodeMeta(se.Meta)
		return Edge{From: se.From, To: se.To, Meta: meta}, true, nil
	})
}
