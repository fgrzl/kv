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
		name   string
		prefix string
		input  []any
	}{
		{"string keys", "test", []any{"a", "b"}},
		{"integer keys", "test", []any{1, 2}},
		{"mixed types", "test", []any{"abc", 42, true}},
		{"boolean order", "test", []any{false, true}},
		{"floating point order", "test", []any{1.1, 2.2, 3.3}},
		{"timestamp order", "test", []any{time.Unix(1000, 0), time.Unix(2000, 0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1, err := kv.EncodeKey(tt.prefix, tt.input...)
			require.NoError(t, err, "EncodeKey should not return an error")
			key2, err := kv.EncodeKey(tt.prefix, append(tt.input, "extra")...)
			require.NoError(t, err, "EncodeKey should not return an error")

			assert.Less(t, bytes.Compare(key1, key2), 0, "EncodeKey should produce lexicographically ordered keys")
		})
	}
}

// TestFirstKey ensures FirstKey is always smaller than any encoded key.
func TestFirstKey(t *testing.T) {
	prefix := "test"
	first, err := kv.FirstKey(prefix)
	require.NoError(t, err, "EncodeKey should not return an error")
	regular, err := kv.EncodeKey(prefix, "somekey")
	require.NoError(t, err, "EncodeKey should not return an error")

	assert.Less(t, bytes.Compare(first, regular), 0, "FirstKey should be lexicographically smaller than any encoded key")
}

// TestLastKey ensures LastKey is always greater than any encoded key.
func TestLastKey(t *testing.T) {
	prefix := "test"
	last, err := kv.LastKey(prefix)
	require.NoError(t, err, "EncodeKey should not return an error")
	regular, err := kv.EncodeKey(prefix, "somekey")
	require.NoError(t, err, "EncodeKey should not return an error")

	assert.Greater(t, bytes.Compare(last, regular), 0, "LastKey should be lexicographically larger than any encoded key")
}

// TestKeyRange ensures keys are correctly ordered for range queries.
func TestKeyRange(t *testing.T) {
	prefix := "test"
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
		encodedKey, err := kv.EncodeKey(prefix, key...)
		require.NoError(t, err, "EncodeKey should not return an error")
		encodedKeys = append(encodedKeys, encodedKey)
	}

	for i := 0; i < len(encodedKeys)-1; i++ {
		assert.Less(t, bytes.Compare(encodedKeys[i], encodedKeys[i+1]), 0, "Keys should be lexicographically ordered")
	}
}

func TestUUIDEncoding(t *testing.T) {
	id := uuid.New()
	encoded, err := kv.EncodeKey("test", id)
	require.NoError(t, err, "EncodeKey should not return an error")
	assert.Len(t, encoded, 22, "UUID should be encoded as 16 bytes, 4 bytes for prefix, and 2 bytes for seperators")
}
