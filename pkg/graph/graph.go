package graph

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/kv"
	"github.com/fgrzl/lexkey"
)

// Node represents a graph node.
type Node struct {
	ID   string
	Meta []byte
}

// Edge represents a directed edge from From -> To with optional metadata.
type Edge struct {
	From string
	To   string
	Meta []byte
}

// Graph is a simple graph interface backed by a kv.KV store.
type Graph interface {
	AddNode(ctx context.Context, id string, meta []byte) error
	GetNode(ctx context.Context, id string) (*Node, error)
	DeleteNode(ctx context.Context, id string) error

	AddEdge(ctx context.Context, from, to string, meta []byte) error
	RemoveEdge(ctx context.Context, from, to string) error
	IncomingNeighbors(ctx context.Context, to string) ([]Edge, error)
	Neighbors(ctx context.Context, from string) ([]Edge, error)

	// Existence and count helpers
	HasNode(ctx context.Context, id string) (bool, error)
	EdgeExists(ctx context.Context, from, to string) (bool, error)
	// NodeDegree returns incoming and outgoing edge counts for the node (inCount, outCount)
	NodeDegree(ctx context.Context, id string) (int, int, error)

	// Streaming enumerators for neighbors (avoid materializing large slices)
	EnumerateNeighbors(ctx context.Context, from string) enumerators.Enumerator[Edge]
	EnumerateIncomingNeighbors(ctx context.Context, to string) enumerators.Enumerator[Edge]

	// BFS traversal starting from id. visit is called for each visited node.
	BFS(ctx context.Context, start string, visit func(id string) error, limit int) error

	BFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error
	CollectBFSIDs(ctx context.Context, start string, limit int) ([]string, error)

	DFS(ctx context.Context, start string, visit func(id string) error, limit int) error
	DFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error
	CollectDFSIDs(ctx context.Context, start string, limit int) ([]string, error)

	// BatchAdd atomically (when supported) inserts a set of nodes and edges.
	// Edges are written as forward and reverse entries. If the backend does not
	// support large atomic batches the implementation will fall back to chunked
	// commits.
	BatchAdd(ctx context.Context, nodes []Node, edges []Edge) error
}

// graphStore is the default implementation of Graph that stores nodes and edges
// using the project's kv.KV API. It stores small JSON blobs for easy decoding
// when enumerating.
type graphStore struct {
	store kv.KV
	name  string
}

// NewGraph creates a new graph instance using the provided kv store and a name
// that namespaces graph keys in the KV.
func NewGraph(store kv.KV, name string) Graph {
	return &graphStore{store: store, name: name}
}

// Helper storage formats
type storedNode struct {
	ID   string `json:"id"`
	Meta string `json:"meta"` // base64 encoded
}

type storedEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Meta string `json:"meta"`
}

func encodeMeta(b []byte) string          { return base64.StdEncoding.EncodeToString(b) }
func decodeMeta(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

func (g *graphStore) nodePartition() lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "node")
}

func (g *graphStore) edgePartition(from string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "edge", from)
}

func (g *graphStore) inEdgePartition(to string) lexkey.LexKey {
	return lexkey.Encode("graph", g.name, "inedge", to)
}

func (g *graphStore) AddNode(ctx context.Context, id string, meta []byte) error {
	sn := storedNode{ID: id, Meta: encodeMeta(meta)}
	b, err := json.Marshal(sn)
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal node", "id", id, "err", err)
		return err
	}
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	// Use Insert to avoid silently overwriting existing nodes.
	err = g.store.Insert(ctx, &kv.Item{PK: pk, Value: b})
	if err != nil {
		slog.WarnContext(ctx, "failed to add node", "id", id, "err", err)
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
	// We'll stream deletes using enumerators to avoid materializing large slices.
	const chunkSize = 1000

	// Pre-allocate batch with reasonable capacity
	batch := make([]*kv.BatchItem, 0, chunkSize)

	// helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := g.store.Batch(ctx, batch); err != nil {
			slog.ErrorContext(ctx, "failed to flush delete batch", "node", id, "batch_size", len(batch), "err", err)
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
		return err
	}

	if err := flush(); err != nil {
		return err
	}
	slog.InfoContext(ctx, "node deleted", "id", id, "graph", g.name)
	return nil
}

func (g *graphStore) AddEdge(ctx context.Context, from, to string, meta []byte) error {
	se := storedEdge{From: from, To: to, Meta: encodeMeta(meta)}
	b, err := json.Marshal(se)
	if err != nil {
		return err
	}
	// forward key: edgePartition(from) / to
	pkF := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	// reverse key: inEdgePartition(to) / from
	pkR := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))

	items := []*kv.BatchItem{
		{Op: kv.Put, PK: pkF, Value: b},
		{Op: kv.Put, PK: pkR, Value: b},
	}
	return g.store.Batch(ctx, items)
}

func (g *graphStore) RemoveEdge(ctx context.Context, from, to string) error {
	pkF := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	pkR := lexkey.NewPrimaryKey(g.inEdgePartition(to), lexkey.Encode(from))
	items := []*kv.BatchItem{
		{Op: kv.Delete, PK: pkF},
		{Op: kv.Delete, PK: pkR},
	}
	return g.store.Batch(ctx, items)
}

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

func (g *graphStore) BFS(ctx context.Context, start string, visit func(id string) error, limit int) error {
	slog.DebugContext(ctx, "starting BFS traversal", "start", start, "limit", limit, "graph", g.name)
	// simple queue-based BFS with pre-allocation optimizations
	queue := make([]string, 0, 100) // pre-allocate
	queue = append(queue, start)
	seen := make(map[string]struct{}, 100) // pre-allocate
	var visited int

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			slog.DebugContext(ctx, "BFS cancelled", "visited", visited)
			return ctx.Err()
		default:
		}
		cur := queue[0]
		queue = queue[1:]
		if _, ok := seen[cur]; ok {
			continue
		}
		if err := visit(cur); err != nil {
			slog.ErrorContext(ctx, "BFS visit failed", "node", cur, "err", err)
			return err
		}
		seen[cur] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			slog.DebugContext(ctx, "BFS limit reached", "visited", visited, "limit", limit)
			return nil
		}
		nbrs, err := g.Neighbors(ctx, cur)
		if err != nil {
			slog.ErrorContext(ctx, "failed to get neighbors", "node", cur, "err", err)
			return err
		}
		for _, e := range nbrs {
			if _, ok := seen[e.To]; !ok {
				queue = append(queue, e.To)
			}
		}
	}
	slog.DebugContext(ctx, "BFS completed", "visited", visited)
	return nil
}

// BFSWithDepth delegates to the package-level BFSWithDepth helper.
func (g *graphStore) BFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error {
	type qItem struct {
		id    string
		depth int
	}
	queue := make([]qItem, 0, 100) // pre-allocate
	queue = append(queue, qItem{start, 0})
	seen := make(map[string]struct{}, 100) // pre-allocate
	var visited int

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cur := queue[0]
		queue = queue[1:]
		if _, ok := seen[cur.id]; ok {
			continue
		}
		if err := visit(cur.id, cur.depth); err != nil {
			return err
		}
		seen[cur.id] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			return nil
		}

		// Use streaming enumeration for better performance
		enumerator := g.EnumerateNeighbors(ctx, cur.id)
		if err := enumerators.ForEach(enumerator, func(edge Edge) error {
			if _, ok := seen[edge.To]; !ok {
				queue = append(queue, qItem{edge.To, cur.depth + 1})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// CollectBFSIDs delegates to the package-level CollectBFSIDs helper.
func (g *graphStore) CollectBFSIDs(ctx context.Context, start string, limit int) ([]string, error) {
	ids := make([]string, 0, 100) // pre-allocate
	type qItem struct {
		id    string
		depth int
	}
	queue := make([]qItem, 0, 100) // pre-allocate
	queue = append(queue, qItem{start, 0})
	seen := make(map[string]struct{}, 100) // pre-allocate
	var visited int

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return ids, ctx.Err()
		default:
		}
		cur := queue[0]
		queue = queue[1:]
		if _, ok := seen[cur.id]; ok {
			continue
		}
		ids = append(ids, cur.id)
		seen[cur.id] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			return ids, nil
		}

		// Use streaming enumeration
		enumerator := g.EnumerateNeighbors(ctx, cur.id)
		if err := enumerators.ForEach(enumerator, func(edge Edge) error {
			if _, ok := seen[edge.To]; !ok {
				queue = append(queue, qItem{edge.To, cur.depth + 1})
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// DFSWithDepth delegates to the package-level DFSWithDepth helper.
func (g *graphStore) DFSWithDepth(ctx context.Context, start string, visit func(id string, depth int) error, limit int) error {
	type sItem struct {
		id    string
		depth int
	}

	stack := []sItem{{start, 0}}
	seen := map[string]struct{}{}
	var visited int

	for len(stack) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// pop
		n := len(stack) - 1
		cur := stack[n]
		stack = stack[:n]
		if _, ok := seen[cur.id]; ok {
			continue
		}
		if err := visit(cur.id, cur.depth); err != nil {
			return err
		}
		seen[cur.id] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			return nil
		}
		// push neighbors onto stack. To preserve a predictable order that
		// resembles recursive DFS, we append neighbors in the order returned
		// so that the first neighbor will be processed last (LIFO).
		nbrs, err := g.Neighbors(ctx, cur.id)
		if err != nil {
			return err
		}
		for _, e := range nbrs {
			if _, ok := seen[e.To]; !ok {
				stack = append(stack, sItem{e.To, cur.depth + 1})
			}
		}
	}
	return nil
}

// DFS performs depth-first traversal calling visit(id) for each visited node.
func (g *graphStore) DFS(ctx context.Context, start string, visit func(id string) error, limit int) error {
	return g.DFSWithDepth(ctx, start, func(id string, depth int) error {
		return visit(id)
	}, limit)
}

// CollectDFSIDs delegates to the package-level CollectDFSIDs helper.
func (g *graphStore) CollectDFSIDs(ctx context.Context, start string, limit int) ([]string, error) {
	var ids []string
	type sItem struct {
		id    string
		depth int
	}

	stack := []sItem{{start, 0}}
	seen := map[string]struct{}{}
	var visited int

	for len(stack) > 0 {
		select {
		case <-ctx.Done():
			return ids, ctx.Err()
		default:
		}
		// pop
		n := len(stack) - 1
		cur := stack[n]
		stack = stack[:n]
		if _, ok := seen[cur.id]; ok {
			continue
		}
		ids = append(ids, cur.id)
		seen[cur.id] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			return ids, nil
		}
		nbrs, err := g.Neighbors(ctx, cur.id)
		if err != nil {
			return nil, err
		}
		for _, e := range nbrs {
			if _, ok := seen[e.To]; !ok {
				stack = append(stack, sItem{e.To, cur.depth + 1})
			}
		}
	}
	return ids, nil
}

// BatchAdd inserts nodes and edges into the store. It writes node entries
// and for each edge writes forward and reverse entries. It attempts a single
// atomic batch when supported and falls back to chunked commits.
func (g *graphStore) BatchAdd(ctx context.Context, nodes []Node, edges []Edge) error {
	slog.InfoContext(ctx, "starting batch add", "nodes", len(nodes), "edges", len(edges), "graph", g.name)
	// Pre-allocate with estimated capacity
	ops := make([]*kv.BatchItem, 0, len(nodes)+2*len(edges))

	// nodes
	for _, n := range nodes {
		sn := storedNode{ID: n.ID, Meta: encodeMeta(n.Meta)}
		b, err := json.Marshal(sn)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal node in batch", "id", n.ID, "err", err)
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
			slog.ErrorContext(ctx, "failed to marshal edge in batch", "from", e.From, "to", e.To, "err", err)
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

	// Try one Batch commit first.
	if err := g.store.Batch(ctx, ops); err == nil {
		slog.InfoContext(ctx, "batch add completed", "operations", len(ops), "graph", g.name)
		return nil
	}

	// Fallback to chunked commits
	slog.WarnContext(ctx, "falling back to chunked batch commits", "operations", len(ops))
	const chunkSize = 1000
	for i := 0; i < len(ops); i += chunkSize {
		end := i + chunkSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := g.store.Batch(ctx, ops[i:end]); err != nil {
			slog.ErrorContext(ctx, "chunked batch failed", "chunk_start", i, "chunk_end", end, "err", err)
			return err
		}
	}
	slog.InfoContext(ctx, "chunked batch add completed", "operations", len(ops), "graph", g.name)
	return nil
}

// HasNode returns true if a node with the given id exists.
func (g *graphStore) HasNode(ctx context.Context, id string) (bool, error) {
	pk := lexkey.NewPrimaryKey(g.nodePartition(), lexkey.Encode(id))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

// EdgeExists returns true if an edge from->to exists.
func (g *graphStore) EdgeExists(ctx context.Context, from, to string) (bool, error) {
	pk := lexkey.NewPrimaryKey(g.edgePartition(from), lexkey.Encode(to))
	item, err := g.store.Get(ctx, pk)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

// NodeDegree returns the incoming and outgoing edge counts for a node.
func (g *graphStore) NodeDegree(ctx context.Context, id string) (int, int, error) {
	// Count outgoing
	outPart := g.edgePartition(id)
	outArgs := kv.QueryArgs{PartitionKey: outPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	outItems, err := g.store.Query(ctx, outArgs, kv.Ascending)
	if err != nil {
		return 0, 0, err
	}

	// Count incoming
	inPart := g.inEdgePartition(id)
	inArgs := kv.QueryArgs{PartitionKey: inPart, StartRowKey: lexkey.Empty, EndRowKey: lexkey.Empty, Operator: kv.Scan}
	inItems, err := g.store.Query(ctx, inArgs, kv.Ascending)
	if err != nil {
		return 0, 0, err
	}
	return len(inItems), len(outItems), nil
}

// NeighborsEnumerate streams outgoing neighbors for `from`.
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

// IncomingNeighborsEnumerate streams incoming neighbors for `to`.
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
