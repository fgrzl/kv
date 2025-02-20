package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func NewEncodedKey(prefix string, key ...any) (EncodedKey, error) {
	var encodedKey EncodedKey

	// Precompute total length for a single allocation
	totalLen := 0
	encodedParts := make([][]byte, len(key))

	for i, k := range key {
		part, err := encodeDirect(k)
		if err != nil {
			return encodedKey, err
		}
		encodedParts[i] = part
		totalLen += len(part)
		if i < len(key)-1 {
			totalLen++ // Add separator space
		}
	}

	// Allocate the buffer once
	buf := make([]byte, totalLen)
	offset := 0

	// Copy data and separators
	for i, part := range encodedParts {
		copy(buf[offset:], part)
		offset += len(part)
		if i < len(encodedParts)-1 {
			buf[offset] = 0x00 // Separator
			offset++
		}
	}

	encodedKey.Prefix = prefix
	encodedKey.Key = buf

	return encodedKey, nil
}

// get an encoded key, returns an empty key if an error occurs.
func EncodeKey(prefix string, key ...any) EncodedKey {
	ek, _ := NewEncodedKey(prefix, key...)
	return ek
}

func LastKey(prefix string) EncodedKey {
	return EncodedKey{Prefix: prefix}
}

// EncodedKey represents a structured key with a prefix and a sortable key.
type EncodedKey struct {
	Prefix string
	Key    []byte
}

func (e EncodedKey) IsEmpty() bool {
	return e.Prefix == "" && len(e.Key) == 0
}

// Encode returns a fully encoded key with prefix and key, maintaining lexicographic order.
func (e *EncodedKey) Encode() []byte {
	prefixLen := len(e.Prefix)
	keyLen := len(e.Key)

	// Allocate buffer (4 bytes for length + prefix + 1 null byte + key)
	buf := make([]byte, 4+prefixLen+1+keyLen)

	// Store the prefix length in the first 4 bytes (big-endian)
	binary.BigEndian.PutUint32(buf[:4], uint32(prefixLen))

	// Copy prefix
	copy(buf[4:], e.Prefix)

	// Store null-byte separator
	buf[4+prefixLen] = 0x00

	// Copy key after the null byte
	copy(buf[5+prefixLen:], e.Key)

	return buf
}

// EncodeFirst returns the logical first key.
func (e *EncodedKey) EncodeFirst() []byte {
	// Append a null byte (0x00) to ensure this is the first key lexicographically
	return append(e.Encode(), 0x00)
}

// EncodeLast returns the logical last key.
func (e *EncodedKey) EncodeLast() []byte {
	// Append a max byte (0xFF) to ensure this is the last key lexicographically
	return append(e.Encode(), 0xFF)
}

// EncodeFirstInPrefix returns the first key lexicographically within the prefix range.
func (e *EncodedKey) EncodeFirstInPrefix() []byte {
	// Encode only the prefix, ensuring this is the first key in that range
	buf := make([]byte, 4+len(e.Prefix)+1)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(e.Prefix))) // Store prefix length
	copy(buf[4:], e.Prefix)                                    // Copy prefix
	buf[4+len(e.Prefix)] = 0x00                                // Smallest possible value to mark the start

	return buf
}

// EncodeLastInPrefix returns the last key lexicographically within the prefix range.
func (e *EncodedKey) EncodeLastInPrefix() []byte {
	// Append the max byte (0xFF) directly to the prefix, ensuring it stays within the prefix range
	buf := make([]byte, 4+len(e.Prefix)+1)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(e.Prefix))) // Store prefix length
	copy(buf[4:], e.Prefix)                                    // Copy prefix
	buf[4+len(e.Prefix)] = 0xFF                                // Max byte to ensure upper bound

	return buf
}

func (e *EncodedKey) Decode(encoded []byte) {
	if len(encoded) < 5 { // Minimum valid size: 4-byte length + 1-byte null separator
		return
	}

	// Extract prefix length from the first 4 bytes
	prefixLen := int(binary.BigEndian.Uint32(encoded[:4]))

	// Validate that the encoded data is long enough for the prefix
	if len(encoded) < 4+prefixLen+1 {
		return
	}

	// Extract prefix as a string
	prefix := string(encoded[4 : 4+prefixLen])

	// Ensure the separator exists
	if encoded[4+prefixLen] != 0x00 {
		return
	}

	// Extract the key (after the null-byte separator)
	key := encoded[5+prefixLen:]

	// Return parsed EncodedKey
	e.Prefix = prefix
	e.Key = append([]byte{}, key...)
}

func encodeValue(buf *bytes.Buffer, v any) error {
	switch v := v.(type) {
	case string:
		buf.WriteString(v)
	case uuid.UUID:
		buf.Write(v[:])
	case []byte:
		buf.Write(v)
	case int:
		encodeInt64(buf, int64(v))
	case int64:
		encodeInt64(buf, v)
	case int32:
		encodeInt32(buf, v)
	case uint64:
		encodeUint64(buf, v)
	case uint32:
		encodeUint32(buf, v)
	case uint16:
		encodeUint16(buf, v)
	case uint8:
		buf.WriteByte(v)
	case float64:
		encodeFloat64(buf, v)
	case float32:
		encodeFloat32(buf, v)
	case bool:
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case time.Time:
		encodeTime(buf, v)
	case time.Duration:
		encodeInt64(buf, int64(v)) // Store as int64 nanoseconds
	case nil:
		buf.WriteByte(0x00) // Ensures nil values are minimal in sorting
	case struct{}:
		buf.WriteByte(0xFF) // Ensures max values are at the top
	default:
		return fmt.Errorf("unsupported key type: %v", reflect.TypeOf(v))
	}

	return nil
}

// encodeInt64 ensures lexicographic sorting of signed int64.
func encodeInt64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v)^1<<63) // Convert signed to lexicographically sortable
	buf.Write(b[:])
}

// encodeInt32 ensures lexicographic sorting of signed int32.
func encodeInt32(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v)^1<<31)
	buf.Write(b[:])
}

// encodeUint64 encodes unsigned 64-bit integers.
func encodeUint64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

// encodeUint32 encodes unsigned 32-bit integers.
func encodeUint32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

// encodeUint16 encodes unsigned 16-bit integers.
func encodeUint16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

// encodeFloat64 ensures lexicographic sorting of float64.
func encodeFloat64(buf *bytes.Buffer, f float64) {
	var b [8]byte
	bits := math.Float64bits(f)
	if f < 0 {
		bits ^= 0xffffffffffffffff // Flip bits for proper lexicographic sorting
	} else {
		bits ^= 1 << 63 // Offset to maintain order
	}
	binary.BigEndian.PutUint64(b[:], bits)
	buf.Write(b[:])
}

// encodeFloat32 ensures lexicographic sorting of float32.
func encodeFloat32(buf *bytes.Buffer, f float32) {
	var b [4]byte
	bits := math.Float32bits(f)
	if f < 0 {
		bits ^= 0xffffffff
	} else {
		bits ^= 1 << 31
	}
	binary.BigEndian.PutUint32(b[:], bits)
	buf.Write(b[:])
}

// encodeTime encodes a timestamp in UTC as a lexicographically sortable value.
func encodeTime(buf *bytes.Buffer, t time.Time) {
	encodeInt64(buf, t.UTC().UnixNano()) // Store as nanoseconds since epoch
}

func encodeDirect(v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return []byte(v), nil
	case uuid.UUID:
		return v[:], nil
	case []byte:
		return v, nil
	case int:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v)^1<<63)
		return b, nil
	case int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v)^1<<63)
		return b, nil
	case int32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v)^1<<31)
		return b, nil
	case uint64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, v)
		return b, nil
	case uint32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, v)
		return b, nil
	case uint16:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, v)
		return b, nil
	case uint8:
		return []byte{v}, nil
	case float64:
		b := make([]byte, 8)
		bits := math.Float64bits(v)
		if v < 0 {
			bits ^= 0xffffffffffffffff // Flip bits for negative numbers
		} else {
			bits ^= 1 << 63 // Offset to maintain order
		}
		binary.BigEndian.PutUint64(b, bits)
		return b, nil
	case float32:
		b := make([]byte, 4)
		bits := math.Float32bits(v)
		if v < 0 {
			bits ^= 0xffffffff
		} else {
			bits ^= 1 << 31
		}
		binary.BigEndian.PutUint32(b, bits)
		return b, nil
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case time.Time:
		return encodeTimeDirect(v), nil
	case time.Duration:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.Nanoseconds())^1<<63)
		return b, nil
	case nil:
		return []byte{0x00}, nil // Minimal in sorting
	case struct{}:
		return []byte{0xFF}, nil // Max value for sorting
	default:
		return nil, fmt.Errorf("unsupported key type: %v", reflect.TypeOf(v))
	}
}

// encodeTimeDirect encodes time.Time as a sortable byte slice
func encodeTimeDirect(t time.Time) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UTC().UnixNano())^1<<63)
	return b
}
