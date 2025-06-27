package pebble

import (
	"github.com/cockroachdb/pebble/v2"
)

// Option defines a functional option for configuring Pebble options.
type Option func(*pebble.Options)

// WithTableCacheShards sets the number of table cache shards.
func WithTableCacheShards(n int) Option {
	return func(opts *pebble.Options) {
		opts.Experimental.TableCacheShards = n
	}
}

// WithMemTableSize sets the size of the memtable in bytes.
func WithMemTableSize(n uint64) Option {
	return func(opts *pebble.Options) {
		opts.MemTableSize = n
	}
}

// WithMaxConcurrentCompactions sets the maximum number of concurrent compactions using a deferred getter.
func WithMaxConcurrentCompactions(get func() int) Option {
	return func(opts *pebble.Options) {
		opts.MaxConcurrentCompactions = get
	}
}

// WithMaxOpenFiles sets the maximum number of open files.
func WithMaxOpenFiles(n int) Option {
	return func(opts *pebble.Options) {
		opts.MaxOpenFiles = n
	}
}

// WithDisableWAL disables the write-ahead log for performance-sensitive workloads.
func WithDisableWAL() Option {
	return func(opts *pebble.Options) {
		opts.DisableWAL = true
	}
}

// WithLogger injects a custom Pebble-compatible logger.
func WithLogger(logger pebble.Logger) Option {
	return func(opts *pebble.Options) {
		opts.Logger = logger
	}
}

// WithLevelCompaction enables level-based compaction.
func WithLevelCompaction() Option {
	return func(opts *pebble.Options) {
		opts.L0CompactionThreshold = 1
		opts.L0StopWritesThreshold = 12
	}
}

// WithBytesPerSync controls how frequently Pebble fsyncs data to disk.
func WithBytesPerSync(n int) Option {
	return func(opts *pebble.Options) {
		opts.BytesPerSync = n
	}
}

// WithCache allows setting a shared block cache across multiple Pebble stores.
func WithCache(cache *pebble.Cache) Option {
	return func(opts *pebble.Options) {
		opts.Cache = cache
	}
}
