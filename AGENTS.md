# Raftra - AI Agent Guidelines

This document provides explicit guidelines for AI coding agents working on the Raftra repository. 
Raftra is a fault-tolerant distributed key-value store built in Go, using a from-scratch implementation of the Raft consensus algorithm.

**IMPORTANT:** These guidelines are based on the *actual* implemented state of the repository. Do not assume the existence of features (like Docker or Prometheus metrics) unless they are explicitly present in the codebase.

## Overview
Raftra solves the problem of single-server failure by replicating key-value pairs across a cluster of nodes using the Raft consensus protocol. It guarantees strong consistency and high availability as long as a majority of nodes remain healthy.

## Repository Structure
- `cmd/raftra-server/`: The entry point for the node binary (`main.go`).
- `internal/raft/`: Core Raft consensus engine (election, replication, state management).
- `internal/kvstore/`: The replicated state machine (in-memory key-value map).
- `internal/storage/`: Bbolt-backed durable persistence for Raft state and logs.
- `internal/transport/`: Network communication layer (gRPC for Node-to-Node and Client RPC, plus an HTTP REST gateway).
- `proto/`: Protobuf definitions (`raft.proto`) and generated Go code.
- `test/chaos/`: In-memory virtual network harness for testing partitions, delays, and node isolation.
- `Makefile`: Commands for building, testing, and generating protobufs.

## Architecture
- **Raft Nodes:** Nodes operate as Followers, Candidates, or Leaders. Only the Leader handles client writes, replicating them to followers.
- **Log Replication:** Handled via `AppendEntries` gRPC calls. Entries are committed only when safely stored by a majority quorum.
- **State Machine / KV Store:** An in-memory map (`internal/kvstore/kvstore.go`) protected by a read-write mutex. It only applies entries after they are committed by Raft.
- **Persistence:** Bbolt is used for durable storage of the `currentTerm`, `votedFor`, and the append-only Raft log. State is persisted *before* responding to RPCs.
- **Network Layer:** gRPC is used for Raft consensus communication. Clients can use gRPC or the HTTP REST gateway (which issues HTTP 307 redirects to the leader if a follower is contacted).
- **Chaos Testing:** The project features a robust custom `ChaosNetwork` router in memory that allows tests to cut virtual cables, delay packets, and isolate nodes to prove fault tolerance.
- **Docker / Prometheus:** *Note: While planned in the roadmap or dependencies, Docker Compose files and Prometheus metrics integrations are NOT currently implemented in the codebase.*

## Development Workflow
Use the `Makefile` for standard workflows:
- **Build binary:** `make build` (Outputs to `bin/raftra-server`)
- **Run Unit/Integration Tests:** `make test` (Executes `go test -v -race ./...`)
- **Generate Protobufs:** `make proto`
- **Run a Node:** Execute the binary directly (e.g., `./bin/raftra-server -id node1 -port 50051 -http-port 8001 -data-dir data1`).
- **Race Detection:** Automatically included in the `make test` command.
- *Note: There are no automated cluster startup scripts (like docker-compose), format/lint commands, or benchmarks currently implemented.*

## Code Conventions
- **Go Formatting:** Standard `gofmt` conventions apply.
- **Concurrency:** Extensive use of `sync.Mutex` and `sync.RWMutex` to protect shared state (`RaftNode`, `KVStore`, `ChaosNetwork`).
- **Event Loop:** The Raft engine uses a dedicated background goroutine (`run()`) that reacts to `time.Timer` channels (election/heartbeat) and coordination channels.
- **Logging:** Uses Go's structured `log/slog` package.
- **Interfaces:** Systems like `Transport` and `StorageBackend` are abstracted behind interfaces to facilitate the in-memory chaos testing harness.
- **Package Organization:** Strict separation of concerns following standard Go `internal/` directory layouts.

## Distributed Systems Invariants
Modifications must strictly uphold the following Raft safety properties:
- **Election Safety:** At most one leader can be elected in a given term.
- **Leader Append-Only:** A leader never overwrites or deletes its own log entries.
- **Log Matching:** If two logs have an entry with the same index and term, all preceding entries are identical. Followers must aggressively truncate divergent logs.
- **State Machine Safety:** Commands are applied to the `KVStore` *strictly* in log index order, and *only* after being committed by a majority.
- **Persistence First:** Nodes must sync state (Term, Vote, Log) to Bbolt disk before acknowledging RPCs to ensure crash-recovery safety.
- **Partition Behavior:** Minority partitions cannot commit new data. They will time out and spin elections, but cannot break safety.

## Testing Guidelines
- tests are deeply integrated with the `ChaosNetwork` harness (`test/chaos`).
- To test distributed scenarios, write tests that instantiate multiple nodes attached to a `ChaosNetwork`, manipulate the network (e.g., `network.Partition()`, `network.Isolate()`), and assert on cluster state.
- All concurrent code modifications must pass `make test` which includes the `-race` flag.
- Do not remove or weaken tests to bypass failures.

## Performance Guidelines
- There are currently no measured performance targets or benchmarks implemented in the repository.

## Observability
- **Logging:** Developers should inspect structured `slog` output to trace election cycles, RPC handling, and state transitions.
- **Metrics:** Prometheus metrics are not currently implemented.

## Generated Files
- The files `proto/raft.pb.go` and `proto/raft_grpc.pb.go` are generated code.
- **NEVER** edit these files directly. Modify `proto/raft.proto` and run `make proto`.

## Common Pitfalls
- **Inventing Infrastructure:** Trying to use `docker-compose` or query Prometheus metrics that do not exist.
- **Bypassing the Log:** Directly calling `kvStore.Apply()` or modifying data without going through the Raft proposal process.
- **Concurrency Bugs:** Failing to acquire locks (`rn.mu.Lock()`) when accessing or modifying Raft state or the KV dictionary.
- **Pointer Sharing in Tests:** When simulating RPCs in tests, failing to `proto.Clone()` messages causes nodes to share memory pointers, hiding serialization bugs. (The `TestTransport` already handles this).

## Agent Rules
- **Inspect before modifying:** Always view existing implementations before writing code.
- **Trust the codebase:** Do not invent configuration, commands, or architecture that does not exist. If a prompt mentions a feature that is missing from the code, trust the code.
- **Prefer minimal, localized changes.**
- **Preserve Raft safety and correctness:** Do not rewrite the working consensus layer without explicit, unavoidable reasons.
- **Never bypass consensus:** Replicated state must always flow through the Raft log.
- **No manual protobuf edits:** Do not modify `.pb.go` files directly.
- **Test coverage:** Add or update tests when altering behavior. Do not weaken tests just to make them pass.
- **Run tests:** Always run `make test` (which includes race detection) after modifying concurrent code.
- **Do not introduce new dependencies** unless strictly necessary.
- **Do not change public APIs or wire formats** unnecessarily.
