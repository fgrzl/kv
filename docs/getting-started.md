# Getting started

## Install

```bash
go get github.com/fgrzl/kv
```

Requires Go 1.25+ and Docker for full integration tests.

## Pebble (embedded)

```go
import (
    "context"

    "github.com/fgrzl/kv"
    "github.com/fgrzl/kv/pkg/storage/pebble"
    "github.com/fgrzl/lexkey"
)

store, err := pebble.NewPebbleStore("./data", pebble.WithTableCacheShards(1))
if err != nil {
    log.Fatal(err)
}
defer store.Close()

pk := lexkey.NewPrimaryKey(
    lexkey.Encode("users"),
    lexkey.Encode("user-1"),
)

item := &kv.Item{PK: pk, Value: []byte(`{"name":"Ada"}`)}
_ = store.Put(context.Background(), item)

retrieved, _ := store.Get(context.Background(), pk)
```

## Azure Tables

```go
import (
    "github.com/fgrzl/azkit/credentials"
    "github.com/fgrzl/kv/pkg/storage/azure"
)

cred, _ := credentials.NewSharedKeyCredential(accountName, accountKey)
store, _ := azure.NewAzureStore(
    azure.WithTable("mytable"),
    azure.WithEndpoint(endpoint),
    azure.WithSharedKey(cred),
)
```

## Redis

```go
import "github.com/fgrzl/kv/pkg/storage/redis"

store, _ := redis.NewRedisStore(
    redis.WithAddress("localhost:6379"),
    redis.WithPrefix("myapp:"),
)
```

## Instrumentation

```go
store = kv.NewInstrumentedKV(store, "pebble")
```

See [Observability](observability.md).

## Development

Run backend services via Docker as described in the root [README](../README.md#development-setup), then:

```bash
go test ./...
```
