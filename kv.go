package kv

// KV represents the interface for key-value database operations.
type KV interface {
	Put(item *Item) error
	PutItems(items []*Item) error
	Remove(key string) error
	RemoveItems(keys ...string) error
	GetItem(id string) (*Item, error)
	Query(queryArgs *QueryArgs) ([]*Item, error)
	Close() error
}

// Item represents a key-value pair.
type Item struct {
	Key   string `json:"id"`
	Value []byte `json:"data"`
}

// QueryArgs defines the parameters for querying items.
type QueryArgs struct {
	StartKey  string
	EndKey    string
	Operator  QueryOperator
	Direction SortDirection
	Limit     int
	KeyMatch  func(key string) bool
}

// QueryOperator defines the type of query operation.
type QueryOperator int

const (
	None QueryOperator = iota
	Equal
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
	Between
	NotEqual
	StartsWith
)

// SortDirection defines the sorting direction for queries.
type SortDirection int

const (
	NoneDirection SortDirection = iota
	Ascending
	Descending
)
