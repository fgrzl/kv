package merkle

import (
	"context"
	"fmt"

	"github.com/fgrzl/kv"
)

// setLeafCountMetadata stores the actual leaf count (excluding padding).
func (m *Tree) setLeafCountMetadata(ctx context.Context, stage, space string, count int) error {
	pkVal := metaPK(stage, space, leafCountKey)
	val := []byte(fmt.Sprintf("%d", count))
	return m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val})
}

// getLeafCountMetadata retrieves the actual leaf count from metadata.
func (m *Tree) getLeafCountMetadata(ctx context.Context, stage, space string) (int, error) {
	pkVal := metaPK(stage, space, leafCountKey)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return 0, fmt.Errorf("get leaf count metadata for %s/%s: %w", stage, space, err)
	}
	if item == nil {
		return 0, nil // No metadata means empty tree
	}
	var count int
	if _, err := fmt.Sscanf(string(item.Value), "%d", &count); err != nil {
		return 0, fmt.Errorf("parse leaf count metadata for %s/%s: %w", stage, space, err)
	}
	if count < 0 {
		return 0, fmt.Errorf("%w: negative leaf count %d for %s/%s", errCorruptedMetadata, count, stage, space)
	}
	return count, nil
}

// getMaxLevelMetadata retrieves the maximum tree level from metadata.
// The boolean return reports whether metadata was present.
func (m *Tree) getMaxLevelMetadata(ctx context.Context, stage, space string) (level int, ok bool, err error) {
	pkVal := metaPK(stage, space, maxLevelKey)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return 0, false, fmt.Errorf("get max level metadata for %s/%s: %w", stage, space, err)
	}
	if item == nil {
		return 0, false, nil
	}
	if _, err := fmt.Sscanf(string(item.Value), "%d", &level); err != nil {
		return 0, false, fmt.Errorf("parse max level metadata for %s/%s: %w", stage, space, err)
	}
	if level < -1 {
		return 0, false, fmt.Errorf("%w: negative max level %d for %s/%s", errCorruptedMetadata, level, stage, space)
	}
	return level, true, nil
}

// setMaxLevelMetadata stores the maximum tree level.
func (m *Tree) setMaxLevelMetadata(ctx context.Context, stage, space string, level int) error {
	pkVal := metaPK(stage, space, maxLevelKey)
	val := []byte(fmt.Sprintf("%d", level))
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val}); err != nil {
		return fmt.Errorf("set max level metadata for %s/%s: %w", stage, space, err)
	}
	return nil
}
