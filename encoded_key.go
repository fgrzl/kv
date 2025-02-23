package kv

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func EncodeKey(parts ...any) EncodedKey {
	encodedKey, _ := NewEncodedKey(parts...)
	return encodedKey
}

// BuildKey constructs an EncodedKey with a prefix and a list of sortable values.
func NewEncodedKey(parts ...any) (EncodedKey, error) {

	var result []byte
	for i, part := range parts {
		encoded, err := encodeToBytes(part)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded...)

		if i < len(parts)-1 {
			result = append(result, 0x00) // Separator
		}
	}

	return result, nil
}

type EncodedKey []byte

// IsEmpty checks if the EncodedKey is empty (no prefix and no key).
func (e EncodedKey) IsEmpty() bool {
	return len(e) == 0
}

// Helper function to encode different value types directly into bytes.
func encodeToBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return []byte(v), nil
	case uuid.UUID:
		return v[:], nil
	case []byte:
		return v, nil
	case int, int64, int32, uint64, uint32, uint16, uint8:
		return encodeNumber(v)
	case float64, float32:
		return encodeFloat(v)
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case time.Time:
		return encodeInt64Bytes(v.UTC().UnixNano()), nil
	case time.Duration:
		return encodeInt64Bytes(int64(v)), nil
	case nil:
		return []byte{0x00}, nil // Minimal in sorting
	case struct{}:
		return []byte{0xFF}, nil // Max value for sorting
	default:
		return nil, fmt.Errorf("unsupported key type: %v", reflect.TypeOf(v))
	}
}

// Encode numbers (int64, uint64) in a lexicographically sortable manner.
func encodeNumber(v any) ([]byte, error) {
	buf := make([]byte, 8)
	switch value := v.(type) {
	case int:
		binary.BigEndian.PutUint64(buf, uint64(value)^1<<63)
	case int64:
		binary.BigEndian.PutUint64(buf, uint64(value)^1<<63)
	case int32:
		buf = buf[:4]
		binary.BigEndian.PutUint32(buf, uint32(value)^1<<31)
	case uint64:
		binary.BigEndian.PutUint64(buf, value)
	case uint32:
		buf = buf[:4]
		binary.BigEndian.PutUint32(buf, value)
	case uint16:
		buf = buf[:2]
		binary.BigEndian.PutUint16(buf, value)
	case uint8:
		buf = buf[:1]
		buf[0] = value
	default:
		return nil, fmt.Errorf("unsupported number type: %v", reflect.TypeOf(v))
	}
	return buf, nil
}

// Encode float values with proper lexicographic sorting.
func encodeFloat(v any) ([]byte, error) {
	buf := make([]byte, 8)
	switch value := v.(type) {
	case float64:
		bits := math.Float64bits(value)
		if value < 0 {
			bits ^= 0xffffffffffffffff
		} else {
			bits ^= 1 << 63
		}
		binary.BigEndian.PutUint64(buf, bits)
	case float32:
		buf = buf[:4]
		bits := math.Float32bits(value)
		if value < 0 {
			bits ^= 0xffffffff
		} else {
			bits ^= 1 << 31
		}
		binary.BigEndian.PutUint32(buf, bits)
	default:
		return nil, fmt.Errorf("unsupported float type: %v", reflect.TypeOf(v))
	}
	return buf, nil
}

// Helper to encode int64 values directly.
func encodeInt64Bytes(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value)^1<<63)
	return buf
}

// EncodeFirst returns the first lexicographically sortable key.
func (e EncodedKey) EncodeFirst() []byte {
	return append(e, 0x00)
}

// EncodeLast returns the last lexicographically sortable key.
func (e EncodedKey) EncodeLast() []byte {
	return append(e, 0xFF)
}
