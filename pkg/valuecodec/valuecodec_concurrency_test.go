package valuecodec

import (
	"fmt"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestShouldRoundTripConcurrentlyWhenSharedZstdReused(t *testing.T) {
	// Arrange
	codec := New(testConfig())
	zstdAlgo := NewZstd()

	const goroutines = 64
	const iterations = 200

	// Act
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				value := []byte(fmt.Sprintf("goroutine-%d-iteration-%d-", id, i))
				for len(value) < 512 {
					value = append(value, value...)
				}

				encoded, err := codec.Encode(value)
				if err != nil {
					errCh <- fmt.Errorf("encode: %w", err)
					return
				}
				decoded, err := codec.Decode(encoded)
				if err != nil {
					errCh <- fmt.Errorf("decode: %w", err)
					return
				}
				if string(decoded) != string(value) {
					errCh <- fmt.Errorf("round trip mismatch for goroutine %d iteration %d", id, i)
					return
				}

				// Exercise the algorithm layer directly, bypassing the codec
				// thresholds, so the shared encoder/decoder see raw concurrent use.
				compressed, err := zstdAlgo.Compress(value)
				if err != nil {
					errCh <- fmt.Errorf("compress: %w", err)
					return
				}
				restored, err := zstdAlgo.Decompress(compressed)
				if err != nil {
					errCh <- fmt.Errorf("decompress: %w", err)
					return
				}
				if string(restored) != string(value) {
					errCh <- fmt.Errorf("direct round trip mismatch for goroutine %d iteration %d", id, i)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	// Assert
	for err := range errCh {
		require.NoError(t, err)
	}
}

func benchmarkPayload() []byte {
	payload := []byte("compressible-benchmark-payload-")
	for len(payload) < 4096 {
		payload = append(payload, payload...)
	}
	return payload
}

func BenchmarkZstdCompressShared(b *testing.B) {
	z := NewZstd()
	payload := benchmarkPayload()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := z.Compress(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZstdDecompressShared(b *testing.B) {
	z := NewZstd()
	payload := benchmarkPayload()
	compressed, err := z.Compress(payload)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := z.Decompress(compressed); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZstdCompressPerCall reproduces the OLD per-call construction path
// locally (it does not touch production code) so the allocs/op reduction of the
// shared encoder is demonstrable side by side.
func BenchmarkZstdCompressPerCall(b *testing.B) {
	payload := benchmarkPayload()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			b.Fatal(err)
		}
		_ = encoder.EncodeAll(payload, nil)
		if err := encoder.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
