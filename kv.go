package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/google/uuid"
)

// EncodedKey represents an encoded key as a byte slice, optimized for lexicographic sorting.
type EncodedKey []byte

// NewEncodedKey constructs an EncodedKey from a list of parts, separated by null bytes.
func NewEncodedKey(parts ...any) (EncodedKey, error) {
	var result []byte
	for i, part := range parts {
		encoded, err := encodeToBytes(part)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded...)
		if i < len(parts)-1 {
			result = append(result, 0x00)
		}
	}
	return result, nil
}

// EncodeKey is a convenience function to create an EncodedKey, discarding any error.
func EncodeKey(parts ...any) EncodedKey {
	key, _ := NewEncodedKey(parts...)
	return key
}

// IsEmpty checks if the EncodedKey is empty.
func (e EncodedKey) IsEmpty() bool {
	return len(e) == 0
}

// ToHexString converts the EncodedKey to a hexadecimal string using a pooled buffer.
func (e EncodedKey) ToHexString() string {
	if len(e) == 0 {
		return ""
	}
	return hex.EncodeToString(e)
}

// FromHexString decodes a hexadecimal string back into an EncodedKey.
func (e *EncodedKey) FromHexString(hexstr string) error {
	bytes, err := hex.DecodeString(hexstr)
	if err != nil {
		return err
	}
	*e = bytes // Properly assign the decoded bytes to the dereferenced EncodedKey
	return nil
}

// MarshalJSON encodes EncodedKey as a hex string for JSON.
func (e EncodedKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.ToHexString())
}

// UnmarshalJSON decodes a hex string from JSON into an EncodedKey.
func (e *EncodedKey) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return fmt.Errorf("failed to unmarshal EncodedKey hex string: %w", err)
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return fmt.Errorf("failed to decode hex string: %w", err)
	}
	*e = decoded
	return nil
}

// EncodeFirst returns the first lexicographically sortable key by appending a null byte.
func (e EncodedKey) EncodeFirst() []byte {
	return append(e, 0x00)
}

// EncodeLast returns the last lexicographically sortable key by appending a max byte.
func (e EncodedKey) EncodeLast() []byte {
	return append(e, 0xFF)
}

// encodeToBytes converts a value to a lexicographically sortable byte representation.
func encodeToBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return []byte(v), nil
	case uuid.UUID:
		return v[:], nil
	case []byte:
		return v, nil
	case int:
		return encodeInt64(int64(v)), nil
	case int64:
		return encodeInt64(v), nil
	case int32:
		return encodeInt32(v), nil
	case uint64:
		return encodeUint64(v), nil
	case uint32:
		return encodeUint32(v), nil
	case uint16:
		return encodeUint16(v), nil
	case uint8:
		return []byte{v}, nil
	case float64:
		return encodeFloat64(v), nil
	case float32:
		return encodeFloat32(v), nil
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case time.Time:
		return encodeInt64(v.UTC().UnixNano()), nil
	case time.Duration:
		return encodeInt64(int64(v)), nil
	case nil:
		return []byte{0x00}, nil
	case struct{}:
		return []byte{0xFF}, nil
	default:
		return nil, fmt.Errorf("unsupported key type: %v", reflect.TypeOf(v))
	}
}

// encodeInt64 encodes an int64 with sign bit flipped for sorting.
func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v)^1<<63)
	return buf
}

// encodeInt32 encodes an int32 with sign bit flipped for sorting.
func encodeInt32(v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v)^1<<31)
	return buf
}

// encodeUint64 encodes a uint64 directly.
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// encodeUint32 encodes a uint32 directly.
func encodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

// encodeUint16 encodes a uint16 directly.
func encodeUint16(v uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, v)
	return buf
}

// encodeFloat64 encodes a float64 with proper sorting.
func encodeFloat64(v float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(v)
	if v < 0 {
		bits ^= 0xffffffffffffffff
	} else {
		bits ^= 1 << 63
	}
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

// encodeFloat32 encodes a float32 with proper sorting.
func encodeFloat32(v float32) []byte {
	buf := make([]byte, 4)
	bits := math.Float32bits(v)
	if v < 0 {
		bits ^= 0xffffffff
	} else {
		bits ^= 1 << 31
	}
	binary.BigEndian.PutUint32(buf, bits)
	return buf
}

// PrimaryKey represents a composite key for key-value storage.
type PrimaryKey struct {
	PartitionKey EncodedKey
	RowKey       EncodedKey
}

// NewPrimaryKey creates a new PrimaryKey from partition and row keys.
func NewPrimaryKey(partitionKey, rowKey EncodedKey) PrimaryKey {
	return PrimaryKey{
		PartitionKey: partitionKey,
		RowKey:       rowKey,
	}
}

// Encode concatenates PartitionKey and RowKey with a null byte separator.
func (pk *PrimaryKey) Encode() EncodedKey {
	if pk == nil {
		return nil
	}
	result := make([]byte, len(pk.PartitionKey)+len(pk.RowKey)+1)
	n := copy(result, pk.PartitionKey)
	result[n] = 0x00
	copy(result[n+1:], pk.RowKey)
	return result
}

// ToHexStrings converts the PartitionKey and RowKey to hexadecimal strings.
func (pk *PrimaryKey) ToHexStrings() (partitionKey, rowKey string) {
	if pk == nil {
		return "", ""
	}
	return pk.PartitionKey.ToHexString(), pk.RowKey.ToHexString()
}

// Item represents a key-value pair stored in the KV store.
type Item struct {
	PK    PrimaryKey
	Value []byte
}

// BatchItem represents an operation to perform in a batch.
type BatchItem struct {
	Op    BatchOp
	PK    PrimaryKey
	Value []byte
}

// BatchOp defines the type of batch operation.
type BatchOp int

const (
	NoOp BatchOp = iota
	Put
	Delete
)

// RangeKey defines a range query over keys.
type RangeKey struct {
	PartitionKey EncodedKey
	StartRowKey  EncodedKey
	EndRowKey    EncodedKey
}

// Encode encodes the range boundaries with an option to include/exclude the partition key.
// Returns lower and upper bounds for range queries.
func (rk *RangeKey) Encode(withPartitionKey bool) (lower, upper EncodedKey) {
	if rk == nil {
		return nil, nil
	}
	lower = encodeBoundary(rk.PartitionKey, rk.StartRowKey, false, withPartitionKey) // Lower bound ends with 0x00
	upper = encodeBoundary(rk.PartitionKey, rk.EndRowKey, true, withPartitionKey)    // Upper bound ends with 0xFF
	return lower, upper
}

// encodeBoundary encodes the key boundaries, optionally including the partition key.
func encodeBoundary(partitionKey, rowKey []byte, isUpper, withPartitionKey bool) EncodedKey {
	additionalBytes := 1 // For separator or terminator
	if len(rowKey) > 0 {
		additionalBytes++ // Additional byte for the end marker if rowKey exists
	}

	// Calculate total length considering whether to include the partition key
	partitionLength := 0
	if withPartitionKey {
		partitionLength = len(partitionKey)
	}

	result := make([]byte, 0, partitionLength+len(rowKey)+additionalBytes)

	if withPartitionKey {
		result = append(result, partitionKey...)
	}

	if len(rowKey) == 0 {
		// If rowKey is empty, append end-of-range marker directly
		if isUpper {
			result = append(result, 0xFF)
		} else {
			result = append(result, 0x00)
		}
		return result
	}

	if withPartitionKey {
		result = append(result, 0x00) // Separator if partition key is included
	}
	result = append(result, rowKey...)

	// Append end marker for upper boundary
	if isUpper {
		result = append(result, 0xFF)
	}

	return result
}

// QueryArgs defines parameters for querying the KV store.
type QueryArgs struct {
	PartitionKey EncodedKey
	StartRowKey  EncodedKey
	EndRowKey    EncodedKey
	Operator     QueryOperator
	Limit        int
}

// QueryOperator defines the type of query operation.
type QueryOperator int

const (
	Scan QueryOperator = iota
	Equal
	GreaterThan
	GreaterThanOrEqual
	LessThan
	LessThanOrEqual
	Between
	StartsWith
)

// SortDirection defines the sorting order for query results.
type SortDirection int

const (
	Ascending SortDirection = iota
	Descending
)

// ReverseItems reverses a slice of Item pointers in place.
func ReverseItems(items []*Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

// SortItems sorts []*kv.Item by PartitionKey and RowKey in ascending or descending order.
func SortItems(items []*Item, direction SortDirection) {
	sort.Slice(items, func(i, j int) bool {
		// Compare PartitionKey first
		if cmp := bytes.Compare(items[i].PK.PartitionKey, items[j].PK.PartitionKey); cmp != 0 {
			if direction == Ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		// If PartitionKeys are equal, compare RowKey
		if direction == Ascending {
			return bytes.Compare(items[i].PK.RowKey, items[j].PK.RowKey) < 0
		}
		return bytes.Compare(items[i].PK.RowKey, items[j].PK.RowKey) > 0
	})
}

// KV defines the interface for a key-value store.
type KV interface {
	Get(pk PrimaryKey) (*Item, error)
	GetBatch(keys ...PrimaryKey) ([]*Item, error)
	Put(item *Item) error
	Remove(pk PrimaryKey) error
	RemoveBatch(keys ...PrimaryKey) error
	RemoveRange(rangeKey RangeKey) error
	Query(queryArgs QueryArgs, sort SortDirection) ([]*Item, error)
	Enumerate(queryArgs QueryArgs) enumerators.Enumerator[*Item]
	Batch(items []*BatchItem) error
	BatchChunks(items enumerators.Enumerator[*BatchItem], chunkSize int) error
	Close() error
}
