package kv_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/fgrzl/kv"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeKeyBehavior tests that encoded keys are lexicographically sortable.
func TestEncodeKeyBehavior(t *testing.T) {
	tests := []struct {
		name  string
		input []any
	}{
		{"string keys", []any{"a", "b"}},
		{"integer keys", []any{1, 2}},
		{"mixed types", []any{"abc", 42, true}},
		{"boolean order", []any{false, true}},
		{"floating point order", []any{1.1, 2.2, 3.3}},
		{"timestamp order", []any{time.Unix(1000, 0), time.Unix(2000, 0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1, err := kv.NewEncodedKey(tt.input...)
			require.NoError(t, err, "EncodeKey should not return an error")

			key2, err := kv.NewEncodedKey(append(tt.input, "extra")...)
			require.NoError(t, err, "EncodeKey should not return an error")

			assert.Less(t, bytes.Compare(key1, key2), 0, "EncodeKey should produce lexicographically ordered keys")
		})
	}
}

// TestKeyRange ensures keys are correctly ordered for range queries.
func TestKeyRange(t *testing.T) {
	keys := [][]any{
		{"a"},
		{"b"},
		{"c"},
		{1},
		{2},
		{3},
		{time.Unix(1000, 0)},
		{time.Unix(2000, 0)},
	}

	var encodedKeys []kv.EncodedKey
	for _, key := range keys {
		encodedKey, err := kv.NewEncodedKey(key...)
		require.NoError(t, err, "EncodeKey should not return an error")
		encodedKeys = append(encodedKeys, encodedKey)
	}

	for i := 0; i < len(encodedKeys)-1; i++ {
		assert.Less(t, bytes.Compare(encodedKeys[i], encodedKeys[i+1]), 0, "Keys should be lexicographically ordered")
	}
}

func TestUUIDEncoding(t *testing.T) {
	id := uuid.New()
	encodedKey, err := kv.NewEncodedKey(id)
	require.NoError(t, err, "EncodeKey should not return an error")
	assert.Len(t, encodedKey, 16, "UUID should be encoded as 16 bytes")
}
