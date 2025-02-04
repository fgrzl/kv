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

type EncodedKey []byte

// EncodeKey constructs a lexicographically sortable multipart key.
func EncodeKey(prefix string, key ...any) (EncodedKey, error) {
	var buf bytes.Buffer

	// Add prefix with a null separator to prevent conflicts
	buf.WriteString(prefix)
	buf.WriteByte(0) // Null byte separator ensures prefix remains distinct

	for _, k := range key {
		err := encodeValue(&buf, k)
		if err != nil {
			return nil, err
		}
		buf.WriteByte(0) // Separator to ensure proper ordering
	}

	return buf.Bytes(), nil
}

// FirstKey returns the lexicographically smallest possible key for a given prefix.
func FirstKey(prefix string) (EncodedKey, error) {
	return EncodeKey(prefix, nil) // The smallest value
}

// LastKey returns the lexicographically largest possible key for a given prefix.
func LastKey(prefix string) (EncodedKey, error) {
	return EncodeKey(prefix, maxValue{}) // A value that sorts at the highest position
}

// maxValue is a placeholder for the highest sortable value.
type maxValue struct{}

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
	case maxValue:
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
