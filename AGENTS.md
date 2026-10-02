# Raftra - AI Agent Guidelines

This document provides explicit guidelines for AI coding agents working on the Raftra repository. 
Raftra is a fault-tolerant distributed key-value store built in Go, using a from-scratch implementation of the Raft consensus algorithm.

**IMPORTANT:** These guidelines are based on the *actual* implemented state of the repository (last verified 2026-10-03). Do not assume the existence of features unless they are explicitly present in the codebase. If this file and the code disagree, trust the code and update this file.

## Overview
Raftra solves the problem of single-server failure by replicating key-value pairs across a cluster of nodes using the Raft consensus protocol. Writes are linearizable and survive as long as a majority of nodes remain healthy. Reads are served from a node's local state machine and are **not** linearizable (see Architecture).

## Repository Structure
- `cmd/raftra-server/`: The node binary entry point (`main.go`): flag parsing, wiring, graceful shutdown.
- `cmd/raftra-cli/`: Interactive REPL and one-shot HTTP client (`status`, `set`, `get`, `delete`). It follows HTTP 307 leader redirects.
- `internal/raft/`: Core Raft consensus engine: `raft.go` (node and event loop), `election.go`, `replication.go`, `state.go`, `config.go`, and their tests.
- `internal/kvstore/`: The replicated state machine (in-memory map) and the `gob`-encoded `Command` type.
- `internal/storage/`: `StorageBackend` interface, the bbolt implementation (`bbolt_store.go`, with a `NoSync` option), and an in-memory implementation (`memory_store.go`).
- `internal/transport/`: gRPC server, client and handlers (`RaftService` and `KVService`), plus the HTTP REST gateway (`http_server.go`), which also serves `/status` and `/metrics`.
- `internal/metrics/`: Prometheus collectors (12 metrics). Every method is nil-safe.
- `proto/`: Protobuf definitions (`raft.proto`) and generated Go code.
- `test/chaos/`: The in-memory `ChaosNetwork` and the `TestCluster` harness (real bbolt on disk), plus 6 fault scenarios.
- `benchmark/`: Go microbenchmarks (`bench_test.go`), the failover timing test (`failover_test.go`), and the HTTP load generator (`loadgen/`).
- `deployments/`: Multi-stage `Dockerfile` and a 3-node `docker-compose.yml`.
- `benchmark_results.md`: Recorded load-generator results. `DEMO_WALKTHROUGH.md`: hands-on demo. `plan.md`: the original phase plan.
- `Makefile`: Build, test, benchmark, protobuf and Docker targets.

## Architecture
- **Raft Nodes:** Nodes operate as Followers, Candidates, or Leaders. Only the Leader accepts writes and replicates them to followers.
- **Timing (`raft.DefaultConfig`):** The election timeout is randomized between 150 and 300 ms, and the heartbeat interval is 50 ms. The chaos harness uses a 30 ms heartbeat. Outbound gRPC RPCs time out after 100 ms. A client proposal times out after 5 s with `ErrCommitTimeout`.
- **Log Replication:** Done with `AppendEntries` RPCs, each of which carries at most 256 entries (`maxEntriesPerAppend`) starting at `nextIndex`; a follower that is far behind catches up over several RPCs. A follower advances `commitIndex` only up to `min(leaderCommit, prevLogIndex + len(entries))`. On a log mismatch the leader decrements `nextIndex` by 1 and retries. An entry commits once a majority stores it, and the leader only counts entries from its current term (§5.4.2).
- **Write Path:** `ProposeCommand` appends the entry to bbolt (one transaction per entry) **while holding `rn.mu`**. It then broadcasts `AppendEntries`, releases the lock, and waits on a `pendingCommits` channel. If the node steps down, pending proposals receive `ErrNotLeader`.
- **State Machine / KV Store:** An in-memory map (`internal/kvstore/kvstore.go`) protected by a read-write mutex. Committed entries are applied to it in log index order.
- **Reads:** HTTP `GET` reads the local KV map on *any* node, so followers can return stale values. gRPC `Get` requires the Leader role but performs no ReadIndex or lease check.
- **Persistence:** bbolt stores `currentTerm` and `votedFor` in the `meta` bucket and the Raft log in the `log` bucket, keyed by big-endian index, with a sentinel entry at index 0. These are written before RPC responses. `commitIndex` and `lastApplied` are volatile: after a restart, a node rebuilds its KV map as `leaderCommit` arrives. The `-nosync` flag disables fsync, which is for benchmarks only and is not crash-safe.
- **Network Layer:** Node-to-node Raft traffic uses gRPC. Clients can use gRPC `KVService`, where a follower returns `leader_hint` = the leader's node ID, or the HTTP gateway, where a follower answers `PUT`/`POST`/`DELETE` with a **307 redirect** to the leader's HTTP address from `-http-peers`, or `503` if no leader is known. When playground limits are enabled, HTTP writes can also return `413`/`429`/`507` (enforced by the node that proposes, i.e. the leader) and `GET /status` can carry `Access-Control-Allow-Origin` (`-cors-origin`).
- **Chaos Testing:** `ChaosNetwork` is an in-memory router. It can block individual links, isolate nodes, partition groups, and heal. It has a `delays` map, but **packet delay is NOT applied** by `TestTransport`, so don't rely on it.
- **Deployment:** `deployments/docker-compose.yml` runs node1 to node3 with fsync on. Host ports are HTTP `8001`–`8003` and gRPC `50051`–`50053`. Redirect `Location` headers use container hostnames such as `http://node2:8001`. `raftra-cli` and `raftra-loadgen` rewrite these to `localhost:800x`, but plain `curl -L` cannot follow them.
- **Observability:** Every node serves Prometheus metrics at `GET /metrics` on its HTTP port (see Observability).
- **Not implemented:** Snapshots or log compaction, dynamic membership, linearizable reads, write batching or group commit of disk syncs (AppendEntries RPCs are capped at 256 entries, but each entry is still its own fsynced bbolt transaction), TLS or auth, and Kubernetes manifests.

## Development Workflow
Use the `Makefile` for standard workflows:
- **Build binaries:** `make build` builds `bin/raftra-server`, `bin/raftra-cli` and `bin/raftra-loadgen`.
- **Run all tests:** `make test`, which runs `go test -v -race ./...`. This includes the unit, persistence and chaos tests **and** `benchmark/TestFailoverTimeMeasurement`, which builds 10 clusters (about 16 s wall time for the full suite on an M2).
- **Microbenchmarks:** `make bench` runs `go test -v -bench=. -benchmem -run=^$ ./benchmark/...`.
- **Failover benchmark:** `make bench-failover` runs 10 trials and fails if the average exceeds 2 s.
- **Load generator:** `./bin/raftra-loadgen -ops=10000 -concurrency=100 -ratio=80:20 -addr=http://localhost:8001`. The ratio is **SET:GET**.
- **Generate Protobufs:** `make proto` (requires `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc`).
- **Docker cluster:** `make docker-build`, `make docker-up` and `make docker-down` (`docker-down` also deletes volumes).
- **Run a node locally:** `./bin/raftra-server -id node1 -port 50051 -http-port 8001 -data-dir data1 -peers node2:localhost:50052,node3:localhost:50053 -http-peers node2:http://localhost:8002,node3:http://localhost:8003`. The full 3-node commands are in `DEMO_WALKTHROUGH.md`. Server flags are `-id`, `-host`, `-port`, `-http-port`, `-peers`, `-http-peers`, `-data-dir`, `-nosync`, plus the playground limits `-max-key-bytes`, `-max-value-bytes`, `-max-keys`, `-write-rate`, `-write-burst`, `-trust-proxy` and `-cors-origin` (all off by default; implemented in `internal/transport/limits.go` and `ratelimit.go`).
- **Race Detection:** Automatically included in `make test`.
- *Note: There is no lint target and no golangci-lint config. `go vet ./...` and `gofmt -l .` work but are not wired into the Makefile.*

## Code Conventions
- **Go Formatting:** Standard `gofmt` conventions apply.
- **Concurrency:** Extensive use of `sync.Mutex` and `sync.RWMutex` to protect shared state (`RaftNode`, `KVStore`, `ChaosNetwork`, `TestCluster`). Functions with the `...Locked` suffix and `checkTerm` / `becomeLeader` require the caller to hold `rn.mu`.
- **Event Loop:** The Raft engine uses a dedicated background goroutine (`run()`) that reacts to `time.Timer` channels (election and heartbeat) and `stopCh`. Outbound RPCs run in their own goroutines and re-acquire `rn.mu` to handle responses.
- **Logging:** Uses Go's structured `log/slog`. The node logger carries `node_id`.
- **Metrics:** Call metrics through the `*metrics.Metrics` helper methods, which are nil-safe so tests can leave metrics unset. Use `metrics.NewMetrics(prometheus.NewRegistry())` in tests instead of the process-wide `metrics.Default()` singleton.
- **Interfaces:** `Transport` (in `internal/raft`) and `StorageBackend` (in `internal/storage`) are abstracted to support the in-memory chaos harness.
- **Package Organization:** Strict separation of concerns following standard Go `internal/` directory layouts.

## Distributed Systems Invariants
Modifications must strictly uphold the following Raft safety properties:
- **Election Safety:** At most one leader can be elected in a given term.
- **Leader Append-Only:** A leader never overwrites or deletes its own log entries.
- **Log Matching:** If two logs have an entry with the same index and term, all preceding entries are identical. Followers must aggressively truncate divergent logs.
- **Leader Completeness:** Voters deny candidates whose log is less up-to-date (§5.4.1). Leaders only commit current-term entries by counting replicas (§5.4.2).
- **State Machine Safety:** Commands are applied to the `KVStore` *strictly* in log index order, and *only* after being committed by a majority.
- **Persistence First:** Nodes must sync state (Term, Vote, Log) to bbolt before acknowledging RPCs to ensure crash-recovery safety. *Known gap:* `SaveTerm` / `SaveVotedFor` errors are currently ignored (`_ =`) in `election.go`.
- **Partition Behavior:** Minority partitions cannot commit new data. They will time out and keep holding elections, but cannot break safety.

## Testing Guidelines
- Distributed tests use the `test/chaos` harness. Create a `chaos.NewTestCluster(t, n)`, then call `Start()` and `WaitForLeader()`/`WaitForNewLeader()`. Inject faults with `Crash(id)`, `Restart(id)`, `Partition(id)`, `PartitionGroup(a, b)` and `Heal()`. Assert with `Propose`, `ProposeOnNode`, `GetKV`, `GetLeaders` and `AssertAllKVConsistent`.
- `TestCluster` uses real bbolt files in `t.TempDir()`. `Crash` closes the DB and isolates the node, and `Restart` reopens the DB from disk.
- Lower-level tests in `internal/raft` (election, replication, persistence) and `internal/storage` use their own helpers in their `_test.go` files. Look at those before adding new ones.
- All concurrent code modifications must pass `make test`, which includes the `-race` flag.
- Do not remove or weaken tests to bypass failures.

## Performance Guidelines
- Recorded results are in `benchmark_results.md` and the README (3 nodes on one MacBook, HTTP load generator, 100 workers, 80% SET):
  - `-nosync`: **5,842 ops/s**, SET p50 20.84 ms / p99 40.23 ms, 0 errors over 1M ops.
  - Durable (fsync): **93.7 ops/s**, SET p50 1,323.76 ms, 0 errors over 10k ops.
- The durable-mode bottleneck is one fsynced bbolt transaction per proposal, taken while `rn.mu` is held. On macOS, `File.Sync` issues `F_FULLFSYNC`. Group commit or batching is the main lever if performance work is requested.
- Go microbenchmarks (recorded 2026-10-03, Apple M2, `-count=3`, in-process cluster with fsync): SetOperation **14.83 ms/op**, GetOperation **29.36 ns/op** (0 allocs), MixedWorkload **2.69 ms/op**, ReplicationLatency **15.58 ms** per quorum commit.
- Failover (`make bench-failover`, 20 trials): **322 ms average**, 273 ms median, 191–611 ms range. The target from `plan.md` is an average under 2 s, enforced by `TestFailoverTimeMeasurement`. The test targets the highest-term leader once the warm-up write has reached every node. Don't revert that, or it will crash already-deposed leaders and report impossible sub-150 ms failovers.
- When re-measuring, record the hardware, OS and Go version alongside the numbers. Never invent figures.

## Observability
- **Logging:** Inspect structured `slog` output to trace election cycles, RPC handling, commits and state machine applies.
- **Metrics:** `GET /metrics` on every node's HTTP port. Raft metrics: `raft_current_term`, `raft_node_role` (0=Follower, 1=Candidate, 2=Leader), `raft_leader_elections_total`, `raft_commit_index`, `raft_last_applied`, `raft_log_entries_total`, `raft_replication_latency_seconds` (histogram), `raft_append_entries_total`, `raft_request_vote_total`. KV metrics: `kv_requests_total{type}`, `kv_request_duration_seconds{type}` (histogram), `kv_store_size`.
- **Status:** `GET /status` returns JSON with `role`, `term`, `is_leader`, `leader_id`, `commit_index` and `last_applied`.

## Generated Files
- The files `proto/raft.pb.go` and `proto/raft_grpc.pb.go` are generated code.
- **NEVER** edit these files directly. Modify `proto/raft.proto` and run `make proto`.

## Common Pitfalls
- **Inventing Infrastructure:** Assuming things that don't exist, such as Kubernetes manifests, a lint target, snapshotting, packet-delay injection, or recorded microbenchmark numbers.
- **Bypassing the Log:** Directly calling `kvStore.Apply()` or modifying data without going through the Raft proposal process.
- **Concurrency Bugs:** Failing to acquire locks (`rn.mu.Lock()`) when accessing or modifying Raft state or the KV dictionary, or calling a `...Locked` function without holding `rn.mu`.
- **Shared Metrics in Tests:** `metrics.Default()` is a process-wide singleton, so every in-process node using it would overwrite the same gauges. Use `metrics.NewMetrics(prometheus.NewRegistry())` per node, or leave metrics nil. Registering the same collectors twice on one registry panics.
- **Pointer Sharing in Tests:** When simulating RPCs in tests, failing to `proto.Clone()` messages causes nodes to share memory pointers, hiding serialization bugs. (The `TestTransport` already handles this).
- **Treating reads as linearizable:** Follower `GET`s can be stale. Don't write tests or docs that assume otherwise.

## Agent Rules
- **Inspect before modifying:** Always view existing implementations before writing code.
- **Trust the codebase:** Do not invent configuration, commands, or architecture that does not exist. If a prompt mentions a feature that is missing from the code, trust the code.
- **Prefer minimal, localized changes.**
- **Preserve Raft safety and correctness:** Do not rewrite the working consensus layer without explicit, unavoidable reasons.
- **Never bypass consensus:** Replicated state must always flow through the Raft log.
- **No manual protobuf edits:** Do not modify `.pb.go` files directly.
- **Test coverage:** Add or update tests when altering behavior. Do not weaken tests just to make them pass.
- **Run tests:** `make test` (which includes race detection) must pass after modifying concurrent code. Per the rule below, give the user the command to run.
- **Do not introduce new dependencies** unless strictly necessary.
- **Do not change public APIs or wire formats** unnecessarily.
- **Keep docs in sync:** When behavior, flags, metrics or benchmark results change, update `README.md` (and this file) in the same change.
- **Interactive User Testing Mode:** When it comes to testing, DO NOT run test/run commands directly in the background. Always provide the command to the user, explain what it is for, and provide the expected results so the user can run it in their terminal and compare the output themselves to maximize hands-on learning.
