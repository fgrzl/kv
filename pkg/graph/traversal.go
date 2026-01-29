package graph

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fgrzl/enumerators"
)

// Traversal algorithms

func (g *graphStore) BFS(ctx context.Context, start string, visit func(id string) error, limit int) error {
	ctx, span := tracer.Start(ctx, "graph.BFS",
		trace.WithAttributes(
			attribute.String("graph", g.name),
			attribute.String("start_node", start),
			attribute.Int("limit", limit),
		))
	defer span.End()

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
			span.SetAttributes(attribute.Int("visited", visited))
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
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Int("visited", visited))
			return err
		}
		seen[cur] = struct{}{}
		visited++
		if limit > 0 && visited >= limit {
			slog.DebugContext(ctx, "BFS limit reached", "visited", visited, "limit", limit)
			span.SetAttributes(attribute.Int("visited", visited))
			return nil
		}
		enumerator := g.EnumerateNeighbors(ctx, cur)
		if err := enumerators.ForEach(enumerator, func(edge Edge) error {
			if _, ok := seen[edge.To]; !ok {
				queue = append(queue, edge.To)
			}
			return nil
		}); err != nil {
			slog.ErrorContext(ctx, "failed to enumerate neighbors", "node", cur, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Int("visited", visited))
			return err
		}
	}
	slog.DebugContext(ctx, "BFS completed", "visited", visited)
	span.SetAttributes(attribute.Int("visited", visited))
	return nil
}

// BFSWithDepth performs breadth-first traversal calling visit(id, depth) for each visited node.
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

// CollectBFSIDs performs BFS traversal and collects all visited node IDs.
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

// DFSWithDepth performs depth-first traversal calling visit(id, depth) for each visited node.
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
		enumerator := g.EnumerateNeighbors(ctx, cur.id)
		if err := enumerators.ForEach(enumerator, func(edge Edge) error {
			if _, ok := seen[edge.To]; !ok {
				stack = append(stack, sItem{edge.To, cur.depth + 1})
			}
			return nil
		}); err != nil {
			return err
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

// CollectDFSIDs performs DFS traversal and collects all visited node IDs.
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
		enumerator := g.EnumerateNeighbors(ctx, cur.id)
		if err := enumerators.ForEach(enumerator, func(edge Edge) error {
			if _, ok := seen[edge.To]; !ok {
				stack = append(stack, sItem{edge.To, cur.depth + 1})
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
