package redis

// Option defines a functional option for configuring RedisOptions.
type Option func(*RedisOptions)

// WithAddress sets the Redis server address.
func WithAddress(addr string) Option {
	return func(o *RedisOptions) {
		o.Addr = addr
	}
}

// WithPassword sets the Redis authentication password.
func WithPassword(pw string) Option {
	return func(o *RedisOptions) {
		o.Password = pw
	}
}

// WithDatabase selects the Redis database number.
func WithDatabase(db int) Option {
	return func(o *RedisOptions) {
		o.DB = db
	}
}

// WithPrefix sets the key prefix.
func WithPrefix(prefix string) Option {
	return func(o *RedisOptions) {
		o.Prefix = prefix
	}
}

// RedisOptions holds configuration options for the Redis provider.
type RedisOptions struct {
	Addr     string
	Password string
	DB       int
	Prefix   string
}

// applyOptions builds a RedisOptions from given Option funcs.
func applyOptions(opts ...Option) *RedisOptions {
	cfg := &RedisOptions{
		Addr: "localhost:6379",
		DB:   0,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
