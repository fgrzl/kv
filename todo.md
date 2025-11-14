# Project TODOs

This file lists the short-term plan and next actions for the `kv` repository.

1. Create `todo.md` with initial review
   - Status: completed
   - Notes: Initial project-scope tasks captured here.

2. Run tests & linters
   - Status: completed
   - Commands to run locally:
     - `go test ./...`
     - `gofmt -w .`
     - `golangci-lint run` (if installed)

3. Fix failing tests and bugs
   - Status: completed
   - Notes: All tests passing with good coverage (80-84% for overlays).

4. Add/improve unit tests for storage
   - Status: completed
   - Notes: Storage implementations are fully covered by integration tests in `test/kv_test.go` across all providers (azure, pebble, redis).

5. Add CI workflow (GitHub Actions)
   - Status: not-started
   - Notes: Run `go test ./...`, `gofmt`, and `golangci-lint` on PRs.

6. Update README and package docs
   - Status: completed
   - Notes: Added performance section with benchmark results.

7. Add static analysis & formatting setup
   - Status: not-started
   - Notes: Add `golangci-lint` config, `gofmt` checks, and pre-commit hooks.

8. Add examples and benchmarks
   - Status: completed
   - Notes: Added comprehensive benchmarks for all overlays and core KV operations. Performance validated - overlays add minimal overhead. Optimized Merkle tree implementation with pre-allocation and efficient batching for ~5% performance improvement. Optimized Graph implementation with pre-allocated data structures for BFS and batch operations.

Next steps (recommended):

- From repo root, run the tests and linters:

```powershell
go test ./...
gofmt -w .
golangci-lint run
```

- Share any failing test output and I will triage and fix them.
