package azure

import (
	"bytes"

	"github.com/fgrzl/lexkey"
)

// Azure Table Storage rejects empty RowKey values (InvalidUri / OutOfRangeInput).
// When callers use an empty RowKey (e.g. merkle root hash), we transparently swap
// it for a single-byte sentinel before writing and reverse it when reading back.

var emptyRowKeySentinel = lexkey.LexKey([]byte{0})

func rowKeyToStore(rk lexkey.LexKey) lexkey.LexKey {
	if len(rk) == 0 {
		return emptyRowKeySentinel
	}
	return rk
}

func rowKeyFromStore(rk lexkey.LexKey) lexkey.LexKey {
	if bytes.Equal(rk, emptyRowKeySentinel) {
		return nil
	}
	return rk
}

func pkToStore(pk lexkey.PrimaryKey) lexkey.PrimaryKey {
	if len(pk.RowKey) == 0 {
		return lexkey.PrimaryKey{PartitionKey: pk.PartitionKey, RowKey: emptyRowKeySentinel}
	}
	return pk
}

func pkFromStore(pk lexkey.PrimaryKey) lexkey.PrimaryKey {
	return lexkey.PrimaryKey{
		PartitionKey: pk.PartitionKey,
		RowKey:       rowKeyFromStore(pk.RowKey),
	}
}
