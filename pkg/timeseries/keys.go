package timeseries

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/fgrzl/lexkey"
)

// rowKeyTimestampWidth is the fixed width of the timestamp portion of a row
// key. lexkey encodes int64 and uint64 as 8 big-endian bytes.
const rowKeyTimestampWidth = 8

// errMalformedRowKey is returned by decodeRowKey when the row key does not
// begin with a well-formed timestamp part.
var errMalformedRowKey = errors.New("timeseries: malformed row key")

// orderedTimestamp returns the timestamp value as it should appear as the
// first row-key part, honoring the configured ordering. The return type is
// the concrete numeric type lexkey will encode (int64 ascending, uint64 for
// the bit-complement descending trick).
func (ts *TimeSeries) orderedTimestamp(tsVal int64) any {
	if !ts.descending {
		return tsVal
	}
	// lexkey's int64 encoding is BE(uint64(v) ^ 0x8000...), which preserves
	// int64 ordering in uint64 lexicographic space. To get descending order
	// across the full int64 domain (including negatives) we complement that
	// uint64. Equivalently: ^uint64(tsVal) ^ 0x8000... .
	return ^uint64(tsVal) ^ 0x8000000000000000
}

// encodeRowKey builds the row key for a sample with the given timestamp and
// optional id suffix. An empty id produces a row key with just the ordered
// timestamp (preserving overwrite-on-duplicate behavior); a non-empty id is
// appended so multiple events at the same timestamp coexist.
func (ts *TimeSeries) encodeRowKey(tsVal int64, id string) lexkey.LexKey {
	if id == "" {
		return lexkey.Encode(ts.orderedTimestamp(tsVal))
	}
	return lexkey.Encode(ts.orderedTimestamp(tsVal), id)
}

// decodeRowKey reverses encodeRowKey, recovering the timestamp and optional
// id from a row key's bytes. It is the hot-path counterpart to encodeRowKey
// and allocates only the id string.
func (ts *TimeSeries) decodeRowKey(rk []byte) (int64, string, error) {
	if len(rk) < rowKeyTimestampWidth {
		return 0, "", errMalformedRowKey
	}
	raw := binary.BigEndian.Uint64(rk[:rowKeyTimestampWidth])
	// Both ascending and descending encodings reduce to a single XOR on
	// recovery: ascending stored uint64(v)^0x8000..., descending stored
	// ^uint64(v) ^ 0x8000.... Complementing in the descending case gives
	// back uint64(v)^0x8000... — the same form as ascending.
	if ts.descending {
		raw = ^raw
	}
	tsVal := int64(raw ^ 0x8000000000000000)
	switch {
	case len(rk) == rowKeyTimestampWidth:
		return tsVal, "", nil
	case rk[rowKeyTimestampWidth] != lexkey.Seperator:
		return 0, "", errMalformedRowKey
	default:
		id := string(rk[rowKeyTimestampWidth+1:])
		return tsVal, id, nil
	}
}

// extractRowKey recovers the true row-key bytes for an item given the known
// partition key that was used to issue the query. This is necessary because
// some backends (notably the pebble store) store the combined
// partitionKey||0x00||rowKey blob and recover the split by scanning for the
// first 0x00, which returns the wrong split point when the partition key
// itself contains separator bytes (as ours does: lexkey.Encode("timeseries",
// name, series)). The Azure backend stores the two keys separately so this
// is a no-op there.
func extractRowKey(knownPartition lexkey.LexKey, pk lexkey.PrimaryKey) ([]byte, bool) {
	skip := len(knownPartition) - len(pk.PartitionKey)
	if skip < 0 || skip > len(pk.RowKey) {
		return nil, false
	}
	return pk.RowKey[skip:], true
}

// encodeRangeForBounds produces the start/end row-key bounds for a [from, to)
// range scan in the series' natural order. Returns ok=false when the range
// is empty or would underflow the int64 domain under the descending encoding.
//
// Range queries use an inclusive Between operator. The end bound is padded
// with lexkey.EncodeLast so entries carrying an id suffix at the boundary
// timestamp are included in the scan, and the start bound uses Encode (no
// padding) so the same entries are not excluded at the other end.
func (ts *TimeSeries) encodeRangeForBounds(from, to int64) (lexkey.LexKey, lexkey.LexKey, bool) {
	if from >= to {
		return nil, nil, false
	}
	if !ts.descending {
		startRK := lexkey.Encode(ts.orderedTimestamp(from))
		endRK := lexkey.EncodeLast(ts.orderedTimestamp(to - 1))
		return startRK, endRK, true
	}
	// descending: smallest encoded value corresponds to largest timestamp.
	if to <= math.MinInt64+1 {
		return nil, nil, false
	}
	startRK := lexkey.Encode(ts.orderedTimestamp(to - 1))
	endRK := lexkey.EncodeLast(ts.orderedTimestamp(from))
	return startRK, endRK, true
}
