package valuecodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

const (
	formatVersion = 1
	magicSize     = 16

	versionOffset         = magicSize
	algorithmOffset       = versionOffset + 1
	decodedSizeOffset     = algorithmOffset + 1
	payloadChecksumOffset = decodedSizeOffset + 4
	headerSize            = payloadChecksumOffset + 4

	defaultMinInputSize    = 256
	defaultMinSavingsBytes = 64
	defaultMaxEncodedRatio = 0.90
)

var (
	magic = [magicSize]byte{0x8b, 0x4b, 0x56, 0xd1, 0xa6, 0x39, 0xc2, 0x10, 0x31, 0x72, 0x5c, 0x93, 0xd8, 0x44, 0xaf, 0x6e}

	ErrInvalidEnvelope      = errors.New("invalid compressed value envelope")
	ErrIntegrityCheckFailed = errors.New("compressed value integrity check failed")
	ErrUnsupportedVersion   = errors.New("unsupported compression format version")
	ErrUnknownAlgorithm     = errors.New("unknown compression algorithm")
)

type AlgorithmID byte

const (
	AlgorithmZstd AlgorithmID = 1
)

type Algorithm interface {
	ID() AlgorithmID
	Name() string
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

type Config struct {
	Enabled         bool
	Algorithm       Algorithm
	MinInputSize    int
	MinSavingsBytes int
	MaxEncodedRatio float64
}

func DefaultConfig() Config {
	return Config{
		Enabled:         true,
		Algorithm:       NewZstd(),
		MinInputSize:    defaultMinInputSize,
		MinSavingsBytes: defaultMinSavingsBytes,
		MaxEncodedRatio: defaultMaxEncodedRatio,
	}
}

func DisabledConfig() Config {
	cfg := DefaultConfig()
	cfg.Enabled = false
	return cfg
}

type Codec struct {
	config     Config
	algorithms map[AlgorithmID]Algorithm
}

func New(config Config) *Codec {
	config = normalizeConfig(config)

	algorithms := map[AlgorithmID]Algorithm{
		AlgorithmZstd: NewZstd(),
	}
	algorithms[config.Algorithm.ID()] = config.Algorithm

	return &Codec{
		config:     config,
		algorithms: algorithms,
	}
}

func (c *Codec) Encode(value []byte) ([]byte, error) {
	if len(value) == 0 || !c.config.Enabled || len(value) < c.config.MinInputSize {
		return value, nil
	}

	encoded, err := c.config.Algorithm.Compress(value)
	if err != nil {
		return nil, fmt.Errorf("compress value: %w", err)
	}

	wireSize := headerSize + len(encoded)
	savings := len(value) - wireSize
	if savings < c.config.MinSavingsBytes {
		return value, nil
	}
	if float64(wireSize)/float64(len(value)) > c.config.MaxEncodedRatio {
		return value, nil
	}

	framed := make([]byte, wireSize)
	copy(framed, magic[:])
	framed[versionOffset] = formatVersion
	framed[algorithmOffset] = byte(c.config.Algorithm.ID())
	binary.BigEndian.PutUint32(framed[decodedSizeOffset:payloadChecksumOffset], uint32(len(value)))
	binary.BigEndian.PutUint32(framed[payloadChecksumOffset:headerSize], crc32.ChecksumIEEE(encoded))
	copy(framed[headerSize:], encoded)

	return framed, nil
}

func (c *Codec) Decode(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value) < len(magic) || !bytes.Equal(value[:len(magic)], magic[:]) {
		return value, nil
	}
	if len(value) < headerSize {
		return nil, ErrInvalidEnvelope
	}

	version := value[versionOffset]
	if version != formatVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	algorithmID := AlgorithmID(value[algorithmOffset])
	algorithm, ok := c.algorithms[algorithmID]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownAlgorithm, algorithmID)
	}

	payload := value[headerSize:]
	expectedChecksum := binary.BigEndian.Uint32(value[payloadChecksumOffset:headerSize])
	if crc32.ChecksumIEEE(payload) != expectedChecksum {
		return nil, ErrIntegrityCheckFailed
	}

	decoded, err := algorithm.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("decompress value: %w", err)
	}

	expectedDecodedSize := binary.BigEndian.Uint32(value[decodedSizeOffset:payloadChecksumOffset])
	if uint32(len(decoded)) != expectedDecodedSize {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrIntegrityCheckFailed, expectedDecodedSize, len(decoded))
	}

	return decoded, nil
}

func IsCompressed(value []byte) bool {
	return len(value) >= len(magic) && bytes.Equal(value[:len(magic)], magic[:])
}

func normalizeConfig(config Config) Config {
	defaults := DefaultConfig()
	if config.Algorithm == nil {
		config.Algorithm = defaults.Algorithm
	}
	if config.MinInputSize <= 0 {
		config.MinInputSize = defaults.MinInputSize
	}
	if config.MinSavingsBytes <= 0 {
		config.MinSavingsBytes = defaults.MinSavingsBytes
	}
	if config.MaxEncodedRatio <= 0 || config.MaxEncodedRatio >= 1 {
		config.MaxEncodedRatio = defaults.MaxEncodedRatio
	}
	return config
}

type Zstd struct{}

func NewZstd() *Zstd {
	return &Zstd{}
}

func (z *Zstd) ID() AlgorithmID {
	return AlgorithmZstd
}

func (z *Zstd) Name() string {
	return "zstd"
}

func (z *Zstd) Compress(src []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()

	return encoder.EncodeAll(src, nil), nil
}

func (z *Zstd) Decompress(src []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	return decoder.DecodeAll(src, nil)
}
