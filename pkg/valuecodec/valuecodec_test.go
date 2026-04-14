package valuecodec

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldRoundTripCompressedValue(t *testing.T) {
	codec := New(testConfig())
	value := bytes.Repeat([]byte("compressible-value-"), 64)

	encoded, err := codec.Encode(value)
	require.NoError(t, err)
	assert.True(t, IsCompressed(encoded))

	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, value, decoded)
}

func TestShouldLeaveLegacyValueUnchanged(t *testing.T) {
	codec := New(DisabledConfig())
	value := []byte("legacy-raw")

	decoded, err := codec.Decode(value)
	require.NoError(t, err)
	assert.Equal(t, value, decoded)
	assert.False(t, IsCompressed(value))
}

func TestShouldLeaveLegacyValueWithOldMagicPrefixUnchanged(t *testing.T) {
	codec := New(DefaultConfig())
	legacyMagicPrefix := []byte{0x8b, 0x4b, 0x56, 0xd1, 0xa6, 0x39, 0xc2, 0x10}
	value := append(append([]byte{}, legacyMagicPrefix...), []byte("legacy-raw")...)

	decoded, err := codec.Decode(value)
	require.NoError(t, err)
	assert.Equal(t, value, decoded)
	assert.False(t, IsCompressed(value))
}

func TestShouldSkipCompressionWhenValueTooSmall(t *testing.T) {
	codec := New(DefaultConfig())
	value := []byte("small")

	encoded, err := codec.Encode(value)
	require.NoError(t, err)
	assert.Equal(t, value, encoded)
	assert.False(t, IsCompressed(encoded))
}

func TestShouldFailOnInvalidEnvelope(t *testing.T) {
	codec := New(DefaultConfig())
	value := append(append([]byte{}, magic[:]...), formatVersion)

	decoded, err := codec.Decode(value)
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, ErrInvalidEnvelope)
}

func TestShouldFailOnCorruptedCompressedPayload(t *testing.T) {
	codec := New(testConfig())
	value := bytes.Repeat([]byte("compressible-value-"), 64)

	encoded, err := codec.Encode(value)
	require.NoError(t, err)
	encoded[len(encoded)-1] ^= 0xff

	decoded, err := codec.Decode(encoded)
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, ErrIntegrityCheckFailed)
}

func TestShouldFailOnMalformedCompressedPayload(t *testing.T) {
	codec := New(DefaultConfig())
	value := makeFramedValue([]byte("not-zstd"), uint32(len("legacy-raw")))

	decoded, err := codec.Decode(value)
	assert.Nil(t, decoded)
	assert.ErrorContains(t, err, "decompress value")
}

func makeFramedValue(payload []byte, decodedSize uint32) []byte {
	framed := make([]byte, headerSize+len(payload))
	copy(framed, magic[:])
	framed[versionOffset] = formatVersion
	framed[algorithmOffset] = byte(AlgorithmZstd)
	binary.BigEndian.PutUint32(framed[decodedSizeOffset:payloadChecksumOffset], decodedSize)
	binary.BigEndian.PutUint32(framed[payloadChecksumOffset:headerSize], crc32.ChecksumIEEE(payload))
	copy(framed[headerSize:], payload)
	return framed
}

func testConfig() Config {
	config := DefaultConfig()
	config.MinInputSize = 1
	config.MinSavingsBytes = 1
	config.MaxEncodedRatio = 0.99
	return config
}
