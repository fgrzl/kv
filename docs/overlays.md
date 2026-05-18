# Overlays

Higher-level structures built on top of `kv.KV`.

| Package | Import | Purpose |
|---------|--------|---------|
| Graph | `github.com/fgrzl/kv/pkg/graph` | Nodes, edges, BFS traversal |
| Merkle | `github.com/fgrzl/kv/pkg/merkle` | Verifiable trees over stored leaves |
| Timeseries | `github.com/fgrzl/kv/pkg/timeseries` | Append and range query by time |
| Search | `github.com/fgrzl/kv/pkg/search` | Indexing and search helpers |

## Usage pattern

Construct an overlay with a base `KV` store:

```go
import "github.com/fgrzl/kv/pkg/graph"

g := graph.NewGraph(store, "my-graph")
```

Overlays add semantic operations while persisting through the same backend. Performance characteristics are documented in the root README benchmarks.

## When to use overlays

- **Graph** — relationship traversal, dependency graphs
- **Merkle** — integrity proofs, sync checkpoints
- **Timeseries** — metrics, audit timelines keyed by time
- **Search** — secondary lookup patterns over KV data

For API details, see package godoc and tests in each `pkg/*` directory.
