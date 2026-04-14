package pebble

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/kv/pkg/valuecodec"
)

// Options is an opaque wrapper around pebble.Options.
type Options struct {
	inner      *pebble.Options
	valueCodec *valuecodec.Codec
}

// Option defines a functional option for configuring Options.
type Option func(*Options)

// NewOptions applies functional options and returns a configured pebble.Options.
func NewOptions(opts ...Option) *pebble.Options {
	return newStoreOptions(opts...).inner
}

func newStoreOptions(opts ...Option) *Options {
	wrapped := &Options{inner: &pebble.Options{}, valueCodec: valuecodec.New(valuecodec.DefaultConfig())}
	for _, opt := range opts {
		opt(wrapped)
	}
	return wrapped
}

func WithTableCacheShards(n int) Option {
	return func(o *Options) {
		o.inner.Experimental.FileCacheShards = n
	}
}

func WithMemTableSize(n uint64) Option {
	return func(o *Options) {
		o.inner.MemTableSize = n
	}
}

func WithMaxConcurrentCompactions(get func() int) Option {
	return func(o *Options) {
		o.inner.CompactionConcurrencyRange = func() (int, int) { return 1, get() }
	}
}

func WithMaxOpenFiles(n int) Option {
	return func(o *Options) {
		o.inner.MaxOpenFiles = n
	}
}

func WithDisableWAL() Option {
	return func(o *Options) {
		o.inner.DisableWAL = true
	}
}

func WithLogger(logger pebble.Logger) Option {
	return func(o *Options) {
		o.inner.Logger = logger
	}
}

func WithLevelCompaction() Option {
	return func(o *Options) {
		o.inner.L0CompactionThreshold = 1
		o.inner.L0StopWritesThreshold = 12
	}
}

func WithBytesPerSync(n int) Option {
	return func(o *Options) {
		o.inner.BytesPerSync = n
	}
}

func WithCache(cache *pebble.Cache) Option {
	return func(o *Options) {
		o.inner.Cache = cache
	}
}

func WithDefaultValueCompression() Option {
	return WithValueCompression(valuecodec.DefaultConfig())
}

func WithValueCompression(config valuecodec.Config) Option {
	return func(o *Options) {
		o.valueCodec = valuecodec.New(config)
	}
}

func WithoutValueCompression() Option {
	return func(o *Options) {
		o.valueCodec = valuecodec.New(valuecodec.DisabledConfig())
	}
}
