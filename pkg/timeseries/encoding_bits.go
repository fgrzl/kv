package timeseries

import (
	"encoding/binary"
)

// int64LEBytes returns the little-endian byte representation of tsVal (two's complement).
func int64LEBytes(tsVal int64) [8]byte {
	var b [8]byte
	x := tsVal
	for i := range b {
		b[i] = byte(x & 0xFF)
		x >>= 8
	}
	return b
}

// int64BitsToDescendingKeyUint maps an int64 timestamp to the uint64 row-key
// prefix used for descending series order, without lossy int↔uint conversions.
func int64BitsToDescendingKeyUint(tsVal int64) uint64 {
	buf := int64LEBytes(tsVal)
	u := binary.LittleEndian.Uint64(buf[:])
	return ^u ^ 0x8000000000000000
}

// descendingKeyUintToInt64 recovers the int64 timestamp from the ordered uint64
// prefix (after optional complement for descending storage).
func descendingKeyUintToInt64(raw uint64) int64 {
	x := raw ^ 0x8000000000000000
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], x)
	var v int64
	for i := 0; i < 7; i++ {
		v |= int64(b[i]) << (8 * i)
	}
	top := int64(b[7])
	if top >= 128 {
		top -= 256
	}
	v |= top << 56
	return v
}
