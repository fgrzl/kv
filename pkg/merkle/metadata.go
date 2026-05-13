package merkle

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/fgrzl/kv"
)

// getLeafCountMetadata retrieves the actual leaf count from metadata.
func (m *Tree) getLeafCountMetadata(ctx context.Context, stage, space string) (int, error) {
	pkVal := metaPK(stage, space, leafCountKey)
	item, err := m.store.Get(ctx, pkVal)
	if err != nil {
		return 0, fmt.Errorf("get leaf count metadata for %s/%s: %w", stage, space, err)
	}
	if item == nil {
		return 0, nil
	}
	count, err := strconv.ParseUint(string(item.Value), 10, 63)
	if err != nil {
		return 0, fmt.Errorf("%w: parse leaf count for %s/%s: %w", errCorruptedMetadata, stage, space, err)
	}
	return int(count), nil
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
	if string(item.Value) == "-1" {
		return -1, true, nil
	}
	parsed, err := strconv.ParseUint(string(item.Value), 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("%w: parse max level for %s/%s: %w", errCorruptedMetadata, stage, space, err)
	}
	if parsed > uint64(math.MaxInt32) {
		return 0, false, fmt.Errorf("%w: max level overflow for %s/%s", errCorruptedMetadata, stage, space)
	}
	return int(parsed), true, nil
}

// setMaxLevelMetadata stores the maximum tree level.
func (m *Tree) setMaxLevelMetadata(ctx context.Context, stage, space string, level int) error {
	pkVal := metaPK(stage, space, maxLevelKey)
	val := strconv.AppendInt(nil, int64(level), 10)
	if err := m.store.Put(ctx, &kv.Item{PK: pkVal, Value: val}); err != nil {
		return fmt.Errorf("set max level metadata for %s/%s: %w", stage, space, err)
	}
	return nil
}
