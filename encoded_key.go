package kv

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type EncodedKey struct {
	Prefix string
	Key    []byte
}

func EncodeKey(prefix string, parts ...any) EncodedKey {
	encodedKey, _ := NewEncodedKey(prefix, parts...)
	return encodedKey
}

// BuildKey constructs an EncodedKey with a prefix and a list of sortable values.
func NewEncodedKey(prefix string, parts ...any) (EncodedKey, error) {
	var encodedKey EncodedKey
	totalLen := 0
	encodedParts := make([][]byte, len(parts))

	// Precompute total length and encode each part
	for i, part := range parts {
		encoded, err := encodeToBytes(part)
		if err != nil {
			return encodedKey, err
		}
		encodedParts[i] = encoded
		totalLen += len(encoded)
		if i < len(parts)-1 {
			totalLen++ // Separator space
		}
	}

	// Allocate and copy data
	buf := make([]byte, totalLen)
	offset := 0
	for i, part := range encodedParts {
		copy(buf[offset:], part)
		offset += len(part)
		if i < len(parts)-1 {
			buf[offset] = 0x00 // Separator
			offset++
		}
	}

	encodedKey.Prefix = prefix
	encodedKey.Key = buf

	return encodedKey, nil
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

// Encode encodes the prefix and key into a byte slice for storage.
func (e *EncodedKey) Encode() []byte {
	prefixLen := len(e.Prefix)
	keyLen := len(e.Key)
	buf := make([]byte, 4+prefixLen+1+keyLen)

	// Encode prefix length and data
	binary.BigEndian.PutUint32(buf[:4], uint32(prefixLen))
	copy(buf[4:], e.Prefix)
	buf[4+prefixLen] = 0x00
	copy(buf[5+prefixLen:], e.Key)

	return buf
}

// EncodeFirst returns the first lexicographically sortable key.
func (e *EncodedKey) EncodeFirst() []byte {
	return append(e.Encode(), 0x00)
}

// EncodeLast returns the last lexicographically sortable key.
func (e *EncodedKey) EncodeLast() []byte {
	return append(e.Encode(), 0xFF)
}

// EncodeFirstInPrefix returns the first key within the prefix range.
func (e *EncodedKey) EncodeFirstInPrefix() []byte {
	buf := make([]byte, 4+len(e.Prefix)+1)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(e.Prefix)))
	copy(buf[4:], e.Prefix)
	buf[4+len(e.Prefix)] = 0x00
	return buf
}

// EncodeLastInPrefix returns the last key within the prefix range.
func (e *EncodedKey) EncodeLastInPrefix() []byte {
	buf := make([]byte, 4+len(e.Prefix)+1)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(e.Prefix)))
	copy(buf[4:], e.Prefix)
	buf[4+len(e.Prefix)] = 0xFF
	return buf
}

// Decode parses an encoded byte slice back into an EncodedKey.
// It expects the format: [4-byte prefix length][prefix][0x00 separator][key]
func (e *EncodedKey) Decode(encoded []byte) {
	if len(encoded) < 5 { // Minimum valid size: 4 bytes for length + 1 byte separator
		e.Prefix = ""
		e.Key = nil
		return
	}

	// Extract prefix length from the first 4 bytes
	prefixLen := int(binary.BigEndian.Uint32(encoded[:4]))

	// Validate the total length for the prefix and the separator
	if len(encoded) < 4+prefixLen+1 {
		e.Prefix = ""
		e.Key = nil
		return
	}

	// Extract the prefix
	e.Prefix = string(encoded[4 : 4+prefixLen])

	// Verify the separator exists
	if encoded[4+prefixLen] != 0x00 {
		e.Prefix = ""
		e.Key = nil
		return
	}

	// Extract the key (after the null-byte separator)
	e.Key = append([]byte{}, encoded[5+prefixLen:]...) // Ensure a copy of the key
}

// IsEmpty checks if the EncodedKey is empty (no prefix and no key).
func (e *EncodedKey) IsEmpty() bool {
	return e.Prefix == "" && len(e.Key) == 0
}
