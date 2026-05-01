package merkle

import "context"

// MutationSession buffers mutation batches for a single stage/space and flushes them in one apply.
//
// The initial implementation is intentionally narrow: it supports append-only workloads so callers
// such as initial projectors can amortize tree recomputation and metadata writes across many small
// batches without introducing a long-lived in-memory tree cache.
type MutationSession struct {
	tree          *Tree
	stage         string
	space         string
	nextLeafIndex int
	pending       []LeafMutation
	closed        bool
}

// BeginMutationSession creates a mutation session for one Merkle tree identified by stage/space.
func (m *Tree) BeginMutationSession(ctx context.Context, stage, space string) (*MutationSession, error) {
	if err := validateStageSpace(stage, space); err != nil {
		return nil, err
	}

	leafCount, err := m.GetLeafCount(ctx, stage, space)
	if err != nil {
		return nil, err
	}

	return &MutationSession{
		tree:          m,
		stage:         stage,
		space:         space,
		nextLeafIndex: leafCount,
		pending:       make([]LeafMutation, 0, 128),
	}, nil
}

// QueueLeafMutations stages append-only mutations and returns the indexes they will occupy when flushed.
func (s *MutationSession) QueueLeafMutations(mutations []LeafMutation) ([]int, error) {
	if s.closed {
		return nil, errInvalidTreeState
	}
	if len(mutations) == 0 {
		return []int{}, nil
	}
	if !isAppendOnlyBatch(mutations) {
		return nil, ErrInvalidLeaf
	}

	indexes := make([]int, len(mutations))
	for i, mutation := range mutations {
		indexes[i] = s.nextLeafIndex + i
		s.pending = append(s.pending, mutation)
	}
	s.nextLeafIndex += len(mutations)
	return indexes, nil
}

// Flush persists all queued mutations in one underlying ApplyLeafMutations call.
func (s *MutationSession) Flush(ctx context.Context) error {
	if s.closed {
		return errInvalidTreeState
	}
	if len(s.pending) == 0 {
		return nil
	}

	if _, err := s.tree.ApplyLeafMutations(ctx, s.stage, s.space, s.pending); err != nil {
		return err
	}

	s.pending = s.pending[:0]
	return nil
}

// Close marks the session unusable for further queuing or flushing.
func (s *MutationSession) Close() {
	s.closed = true
	s.pending = nil
}
