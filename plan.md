# Raftra — Fault-Tolerant Distributed Key-Value Store

## Implementation Plan (10-Day, Phase-Wise)

> [!IMPORTANT]
> **Priority Order**: Correctness > Fault Tolerance > Testing > Observability > Performance > Extra Features.
> Every phase ends with verification. No phase starts until the previous phase is verified correct.

---

## Project Structure

```
raftra/
├── cmd/
│   ├── raftra-server/          # Node binary entry point
│   │   └── main.go
│   └── raftra-cli/             # CLI client for SET/GET/DELETE
│       └── main.go
├── proto/
│   └── raft.proto              # gRPC + Protobuf definitions (Raft RPCs + Client API)
├── internal/
│   ├── raft/                   # Core Raft consensus engine
│   │   ├── raft.go             # Main Raft state machine & event loop
│   │   ├── state.go            # Node state types (Follower/Candidate/Leader)
│   │   ├── log.go              # In-memory Raft log structure
│   │   ├── election.go         # Election logic (start, vote, transition)
│   │   ├── replication.go      # Log replication (AppendEntries send/receive)
│   │   ├── commitment.go       # Commit index advancement & apply logic
│   │   └── config.go           # Raft configuration (timeouts, cluster peers)
│   ├── transport/              # gRPC transport layer
│   │   ├── server.go           # gRPC server setup (Raft + Client services)
│   │   ├── client.go           # gRPC client for inter-node Raft RPCs
│   │   └── handler.go          # gRPC handler implementations
│   ├── storage/                # Persistence layer
│   │   ├── persistence.go      # Interface definitions (LogStore, StableStore)
│   │   ├── bbolt_store.go      # bbolt-backed implementation
│   │   └── memory_store.go     # In-memory implementation (for testing)
│   ├── kvstore/                # KV state machine
│   │   ├── kvstore.go          # In-memory KV map + Apply() method
│   │   └── command.go          # Command types (SET, GET, DELETE)
│   └── metrics/                # Observability
│       └── metrics.go          # Prometheus metrics registration
├── test/
│   ├── integration/            # Integration tests
│   │   ├── cluster_test.go     # Multi-node cluster test harness
│   │   ├── election_test.go    # Leader election integration tests
│   │   ├── replication_test.go # Log replication integration tests
│   │   ├── failure_test.go     # Leader/follower failure tests
│   │   └── partition_test.go   # Network partition tests
│   └── chaos/                  # Chaos/fault-injection test harness
│       ├── harness.go          # Programmatic cluster controller
│       └── scenarios_test.go   # Automated failure scenarios
├── benchmark/
│   ├── bench_test.go           # Go benchmark tests
│   └── loadgen/                # Custom load generator
│       └── main.go
├── deployments/
│   ├── Dockerfile              # Multi-stage Go Dockerfile
│   └── docker-compose.yml      # 3-node cluster compose file
├── docs/
│   ├── architecture.md         # Architecture diagram + explanation
│   ├── raft-protocol.md        # Raft implementation decisions
│   └── demo.md                 # Demo walkthrough
├── Makefile                    # Build, test, proto-gen, docker commands
├── go.mod
├── go.sum
├── plan.md                     # This plan (also saved to project root)
└── README.md                   # Project overview + quick start
```

---

## Phase 1 — Foundation & Node Architecture (Days 1–2)

### Objective
Set up the project skeleton, define all gRPC/Protobuf interfaces, implement the Raft state machine shell, and establish the node lifecycle. By end of Phase 1, three nodes can start, communicate via gRPC, and each node runs its Raft event loop (though elections don't happen yet).

---

### Phase 1A — Project Initialization (Day 1, Morning)

#### 1. Go Module & Dependencies

- `go mod init github.com/shantanusingh/raftra`
- Dependencies:
  - `google.golang.org/grpc` — gRPC framework
  - `google.golang.org/protobuf` — Protobuf runtime
  - `go.etcd.io/bbolt` — Persistent storage for Raft state/log
  - `github.com/prometheus/client_golang` — Metrics
  - `log/slog` — Structured logging (stdlib, Go 1.21+)

#### 2. Protobuf Definitions — [`proto/raft.proto`](file:///Users/shantanusingh/Desktop/raftra/proto/raft.proto)

Define two gRPC services:

```protobuf
syntax = "proto3";
package raft;
option go_package = "github.com/shantanusingh/raftra/proto";

// === Raft Internal RPC Service ===
service RaftService {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
}

// === Client-Facing API Service ===
service KVService {
  rpc Set(SetRequest) returns (SetResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

// --- Raft Messages (per Figure 2 of Raft paper) ---
message LogEntry {
  uint64 index = 1;
  uint64 term = 2;
  bytes command = 3;  // Serialized KV command
}

message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}

message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
}

// --- Client Messages ---
message SetRequest {
  string key = 1;
  string value = 2;
}
message SetResponse {
  bool success = 1;
  string error = 2;
  string leader_hint = 3;  // Redirect client to leader
}

message GetRequest {
  string key = 1;
}
message GetResponse {
  string value = 1;
  bool found = 2;
  string error = 3;
}

message DeleteRequest {
  string key = 1;
}
message DeleteResponse {
  bool success = 1;
  string error = 2;
  string leader_hint = 3;
}
```

#### 3. Makefile

```makefile
.PHONY: proto build test run docker

proto:
	protoc --go_out=. --go-grpc_out=. proto/raft.proto

build:
	go build -o bin/raftra-server ./cmd/raftra-server
	go build -o bin/raftra-cli ./cmd/raftra-cli

test:
	go test -race -v ./...

test-integration:
	go test -race -v -tags=integration ./test/...

docker-build:
	docker build -t raftra .

docker-up:
	docker compose -f deployments/docker-compose.yml up --build

docker-down:
	docker compose -f deployments/docker-compose.yml down
```

#### 4. Generate Protobuf Code
- Run `make proto` to generate Go stubs.

---

### Phase 1B — Raft State Machine Shell (Day 1, Afternoon)

#### 5. Raft Configuration — [`internal/raft/config.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/config.go)

```go
type Config struct {
    NodeID             string
    Peers              []PeerConfig   // {ID, Address} of all cluster nodes
    ElectionTimeoutMin time.Duration  // e.g., 300ms
    ElectionTimeoutMax time.Duration  // e.g., 500ms
    HeartbeatInterval  time.Duration  // e.g., 100ms
    DataDir            string         // Persistence directory
}
```

> [!NOTE]
> Election timeout is randomized between Min and Max per Raft spec to avoid split votes.

#### 6. Node State Types — [`internal/raft/state.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/state.go)

```go
type NodeRole int
const (
    Follower  NodeRole = iota
    Candidate
    Leader
)

// Persistent state (survives restarts — per Figure 2)
type PersistentState struct {
    CurrentTerm uint64
    VotedFor    string   // "" means no vote cast this term
    Log         []LogEntry
}

// Volatile state (all servers)
type VolatileState struct {
    CommitIndex uint64
    LastApplied uint64
}

// Volatile state (leader only — reinitialized after election)
type LeaderState struct {
    NextIndex  map[string]uint64  // peer → next log index to send
    MatchIndex map[string]uint64  // peer → highest replicated index
}
```

#### 7. Main Raft Node — [`internal/raft/raft.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/raft.go)

Core struct:

```go
type RaftNode struct {
    mu sync.Mutex

    // Identity
    config Config
    role   NodeRole

    // State (Figure 2)
    persistent PersistentState
    volatile   VolatileState
    leader     *LeaderState  // nil when not leader

    // Components
    transport  Transport       // Interface for sending RPCs
    storage    StorageBackend  // Interface for persistence
    kvStore    *kvstore.KVStore

    // Channels
    applyCh    chan LogEntry   // Committed entries to apply
    stopCh     chan struct{}

    // Timers
    electionTimer  *time.Timer
    heartbeatTimer *time.Timer

    // Logger
    logger *slog.Logger
}
```

Methods to stub out (implementation in later phases):

```go
func NewRaftNode(config Config, transport Transport, storage StorageBackend, kv *kvstore.KVStore) *RaftNode
func (rn *RaftNode) Start() error           // Start event loop
func (rn *RaftNode) Stop()                  // Graceful shutdown
func (rn *RaftNode) run()                   // Main event loop (select on timers + channels)

// RPC Handlers (called by transport layer)
func (rn *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
func (rn *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse

// Client operations
func (rn *RaftNode) ProposeCommand(cmd []byte) error  // Submit to leader
```

#### 8. Transport Interface — [`internal/transport/`](file:///Users/shantanusingh/Desktop/raftra/internal/transport/)

```go
type Transport interface {
    SendRequestVote(peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
    SendAppendEntries(peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}
```

Implement `GRPCTransport` that holds gRPC client connections to each peer.

#### 9. Storage Interface — [`internal/storage/persistence.go`](file:///Users/shantanusingh/Desktop/raftra/internal/storage/persistence.go)

```go
type StorageBackend interface {
    // Stable store (metadata)
    SaveTerm(term uint64) error
    LoadTerm() (uint64, error)
    SaveVotedFor(candidateID string) error
    LoadVotedFor() (string, error)

    // Log store
    AppendEntries(entries []LogEntry) error
    GetEntry(index uint64) (*LogEntry, error)
    GetEntriesFrom(startIndex uint64) ([]LogEntry, error)
    TruncateFrom(index uint64) error  // Delete entries from index onward
    LastIndex() (uint64, error)
    LastEntry() (*LogEntry, error)
}
```

Implement `MemoryStore` first for testing, `BboltStore` in Phase 4.

---

### Phase 1C — Node Entry Point & Basic gRPC Server (Day 2)

#### 10. Server Entry Point — [`cmd/raftra-server/main.go`](file:///Users/shantanusingh/Desktop/raftra/cmd/raftra-server/main.go)

```go
func main() {
    // Parse flags: --id, --port, --peers, --data-dir
    // Create storage backend
    // Create KV store
    // Create transport
    // Create RaftNode
    // Start gRPC server
    // Start Raft event loop
    // Wait for shutdown signal (SIGINT/SIGTERM)
    // Graceful shutdown
}
```

#### 11. gRPC Server — [`internal/transport/server.go`](file:///Users/shantanusingh/Desktop/raftra/internal/transport/server.go)

- Register both `RaftService` and `KVService`.
- Wire gRPC handlers to call `RaftNode.HandleRequestVote()` and `RaftNode.HandleAppendEntries()`.

#### 12. KV Store Shell — [`internal/kvstore/kvstore.go`](file:///Users/shantanusingh/Desktop/raftra/internal/kvstore/kvstore.go)

```go
type KVStore struct {
    mu   sync.RWMutex
    data map[string]string
}

func (kv *KVStore) Apply(entry LogEntry) interface{} {
    // Deserialize command, apply SET/GET/DELETE
}
func (kv *KVStore) Get(key string) (string, bool)
```

#### 13. KV Command Serialization — [`internal/kvstore/command.go`](file:///Users/shantanusingh/Desktop/raftra/internal/kvstore/command.go)

```go
type CommandType uint8
const (
    CmdSet    CommandType = iota
    CmdDelete
)

type Command struct {
    Type  CommandType
    Key   string
    Value string  // empty for DELETE
}

func EncodeCommand(cmd Command) ([]byte, error)
func DecodeCommand(data []byte) (Command, error)
```

### Phase 1 Verification

- [x] `go build ./...` compiles without errors
- [x] Three nodes can start and listen on different ports
- [x] gRPC health check: nodes can ping each other
- [x] Unit tests: command serialization round-trip, state types, config parsing
- [x] All nodes start as `Follower`

---

## Phase 2 — Leader Election (Days 3–4)

### Objective
Implement Raft leader election per §5.2 of the Raft paper. By end of Phase 2, a 3-node cluster can elect exactly one leader, handle re-elections when the leader disappears, and correctly manage terms.

---

### Phase 2A — Election Timer & Candidate Transition (Day 3, Morning)

#### 14. Election Timer Logic

In `raft.go` main event loop:

```
select {
case <-electionTimer.C:
    // No heartbeat received → become candidate
    rn.startElection()

case <-heartbeatTimer.C:
    // Leader only: send heartbeats
    rn.sendHeartbeats()

case entry := <-applyCh:
    // Apply committed entry to state machine
    rn.applyEntry(entry)
}
```

**Rules:**
- Randomize election timeout between `ElectionTimeoutMin` and `ElectionTimeoutMax` on every reset.
- Reset election timer when: receiving valid AppendEntries, granting a vote, or starting an election.

#### 15. Start Election — [`internal/raft/election.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/election.go)

```
startElection():
    1. Increment currentTerm
    2. Transition to Candidate
    3. Vote for self (persist votedFor = self)
    4. Reset election timer
    5. Send RequestVote RPCs to all peers in parallel
    6. Collect responses:
       - If majority votes received → become Leader
       - If AppendEntries from valid leader received → revert to Follower
       - If election timeout elapses → start new election
```

#### 16. RequestVote Handler — [`internal/raft/election.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/election.go)

Per Figure 2:

```
HandleRequestVote(req):
    1. If req.Term < currentTerm → reply false
    2. If req.Term > currentTerm → update term, revert to Follower, clear votedFor
    3. If votedFor is empty OR votedFor == candidateId:
       a. Check candidate's log is at least as up-to-date:
          - Compare last log term first
          - If equal, compare last log index
       b. If up-to-date → grant vote, persist votedFor, reset election timer
    4. Else → reply false
```

> [!IMPORTANT]
> **Log up-to-date check is critical for safety.** Without it, a candidate with a stale log could become leader and overwrite committed entries.
> Raft §5.4.1: "The voter denies its vote if its own log is more up-to-date than that of the candidate."

### Phase 2B — Leader Transition & Heartbeats (Day 3, Afternoon)

#### 17. Become Leader

```
becomeLeader():
    1. Set role = Leader
    2. Initialize nextIndex[peer] = lastLogIndex + 1 for all peers
    3. Initialize matchIndex[peer] = 0 for all peers
    4. Start heartbeat timer
    5. Send immediate heartbeats (empty AppendEntries) to all peers
    6. Log "became leader for term X"
```

#### 18. Heartbeat Logic

```
sendHeartbeats():
    For each peer (in parallel):
        Send AppendEntries with:
            term = currentTerm
            leaderId = self
            prevLogIndex = nextIndex[peer] - 1
            prevLogTerm = log[prevLogIndex].term
            entries = []  (empty for heartbeat)
            leaderCommit = commitIndex
```

#### 19. AppendEntries Handler (heartbeat-only for now, full replication in Phase 3)

```
HandleAppendEntries(req):
    1. If req.Term < currentTerm → reply false
    2. If req.Term >= currentTerm:
       a. Reset election timer
       b. If req.Term > currentTerm → update term, revert to Follower
       c. Set role = Follower (even if Candidate)
       d. Record leaderId
    3. (Log consistency check — Phase 3)
    4. Reply success = true
```

### Phase 2C — Election Edge Cases & Testing (Day 4)

#### 20. Critical Edge Cases to Handle

| Scenario | Expected Behavior |
|---|---|
| Split vote (no majority) | Election timeout → new election with higher term |
| Stale leader sends heartbeat | Follower rejects if term < currentTerm |
| Candidate receives AppendEntries from new leader | Reverts to Follower |
| Two candidates in same term | At most one can win (majority math) |
| Node restarts during election | Loads persisted term + votedFor, starts as Follower |
| Candidate's log is behind voter's | Vote denied (log up-to-date check) |

#### 21. Term Management Rules (apply everywhere)

```
// Called on every incoming RPC (request or response)
func (rn *RaftNode) checkTerm(incomingTerm uint64) {
    if incomingTerm > rn.persistent.CurrentTerm {
        rn.persistent.CurrentTerm = incomingTerm
        rn.persistent.VotedFor = ""
        rn.role = Follower
        rn.storage.SaveTerm(incomingTerm)
        rn.storage.SaveVotedFor("")
    }
}
```

> [!WARNING]
> This rule must be applied to **every** RPC handler and **every** RPC response handler. Missing it anywhere breaks election safety.

#### 22. Unit Tests for Election

| Test | Validates |
|---|---|
| `TestSingleNodeBecomesLeader` | 1-node cluster trivially elects self |
| `TestThreeNodeElection` | Exactly one leader in 3-node cluster |
| `TestTermIncrements` | Terms increase monotonically across elections |
| `TestVoteDeniedForStaleTerm` | RequestVote with old term rejected |
| `TestVoteDeniedForStaleLog` | RequestVote with outdated log rejected |
| `TestCandidateRevertsOnHigherTerm` | Candidate → Follower on higher term |
| `TestNoDoubleVoting` | Node votes for at most one candidate per term |
| `TestElectionAfterLeaderFailure` | Kill leader → new leader elected |
| `TestSplitVoteResolution` | Split vote → timeout → eventual leader |

### Phase 2 Verification

- [ ] 3 nodes start → exactly 1 leader elected within 2 seconds
- [ ] Leader sends heartbeats; followers don't start elections
- [ ] Kill leader → new leader elected within 1 second
- [ ] Restart killed node → joins as follower
- [ ] All unit tests pass with `-race` flag
- [ ] Terms strictly increase across elections

---

## Phase 3 — Log Replication & Commitment (Days 5–6)

### Objective
Implement the full log replication pipeline per §5.3 and §5.4. By end of Phase 3, clients can SET/GET/DELETE, the leader replicates entries, and commits happen only after majority acknowledgement.

---

### Phase 3A — Log Replication: Leader Side (Day 5, Morning)

#### 23. Client Command Flow

```
Client SET key value
    ↓
KVService.Set() gRPC handler
    ↓
If not leader → return leader_hint (redirect)
    ↓
RaftNode.ProposeCommand(cmd)
    ↓
Append LogEntry{index, term, command} to local log
    ↓
Persist log entry
    ↓
Replicate to followers via AppendEntries
    ↓
Wait for majority acknowledgement
    ↓
Advance commitIndex
    ↓
Apply to KV state machine
    ↓
Return success to client
```

#### 24. Log Entry Structure

```go
type LogEntry struct {
    Index   uint64
    Term    uint64
    Command []byte  // Encoded KV command
}
```

> [!NOTE]
> Log indices are 1-based. Index 0 is a sentinel (empty entry) to simplify prevLogIndex calculations.

#### 25. Leader Replication Logic — [`internal/raft/replication.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/replication.go)

```
replicateToFollower(peerID):
    prevLogIndex = nextIndex[peer] - 1
    prevLogTerm  = log[prevLogIndex].term
    entries      = log[nextIndex[peer]:]

    Send AppendEntries(term, leaderID, prevLogIndex, prevLogTerm, entries, commitIndex)

    If response.Success:
        nextIndex[peer] = prevLogIndex + len(entries) + 1
        matchIndex[peer] = nextIndex[peer] - 1
    Else:
        // Log inconsistency: decrement nextIndex and retry
        nextIndex[peer] = max(1, nextIndex[peer] - 1)
        // Retry replication (will be picked up next heartbeat or immediate retry)
```

> [!TIP]
> **Optimization (optional):** Instead of decrementing `nextIndex` by 1, the follower can return the conflicting term and the first index of that term. The leader can then skip back an entire conflicting term at once. This is described in §5.3 of the paper but is not required for correctness.

#### 26. Integrate Replication with Heartbeats

- Heartbeats should now carry pending entries (not just empty `entries[]`).
- On each heartbeat tick, for each follower, compute entries to send based on `nextIndex[peer]`.

---

### Phase 3B — Log Replication: Follower Side (Day 5, Afternoon)

#### 27. Full AppendEntries Handler

```
HandleAppendEntries(req):
    1. If req.Term < currentTerm → reply {term: currentTerm, success: false}

    2. Reset election timer. Record leaderId. Update term if needed.

    3. Log consistency check:
       If prevLogIndex > 0:
           If log has no entry at prevLogIndex → reply false
           If log[prevLogIndex].term != prevLogTerm → reply false

    4. Handle conflicts:
       For each new entry in req.Entries:
           If existing log entry at same index has different term:
               Truncate log from that index onward
               Break
       Append new entries not already in log

    5. Update commitIndex:
       If req.LeaderCommit > commitIndex:
           commitIndex = min(req.LeaderCommit, index of last new entry)

    6. Reply {term: currentTerm, success: true}
```

> [!CAUTION]
> **Step 4 must be implemented exactly per Figure 2.** A common bug is to unconditionally truncate the log from `prevLogIndex + 1`. This is WRONG — it can delete already-committed entries during reordered RPCs. Only truncate if there's an actual conflict (same index, different term).

#### 28. Log Conflict Resolution Details

```
// Correct implementation:
for i, entry := range req.Entries {
    existingIndex := req.PrevLogIndex + 1 + uint64(i)
    existing, err := rn.getLogEntry(existingIndex)
    if err != nil || existing == nil {
        // No existing entry — append from here
        rn.appendLogEntries(req.Entries[i:])
        break
    }
    if existing.Term != entry.Term {
        // Conflict! Truncate from here and append
        rn.truncateLogFrom(existingIndex)
        rn.appendLogEntries(req.Entries[i:])
        break
    }
    // Entry matches — skip (already have it)
}
```

---

### Phase 3C — Commitment & Application (Day 6, Morning)

#### 29. Commit Index Advancement (Leader) — [`internal/raft/commitment.go`](file:///Users/shantanusingh/Desktop/raftra/internal/raft/commitment.go)

Per §5.3 and §5.4.2:

```
advanceCommitIndex():
    For N = commitIndex + 1 to lastLogIndex:
        If log[N].term == currentTerm:  // CRITICAL: only commit current-term entries
            count = 1  // self
            For each peer:
                If matchIndex[peer] >= N:
                    count++
            If count > len(cluster) / 2:
                commitIndex = N

    // Apply newly committed entries
    while lastApplied < commitIndex:
        lastApplied++
        entry = log[lastApplied]
        kvStore.Apply(entry)
```

> [!WARNING]
> **§5.4.2 is critical:** A leader MUST NOT commit entries from previous terms by counting replicas. It can only commit an entry from its own term, which indirectly commits all prior entries. Violating this breaks the safety guarantee.

#### 30. Client-Facing gRPC Handlers

```go
// Set handler
func (h *Handler) Set(ctx context.Context, req *SetRequest) (*SetResponse, error) {
    if !h.raftNode.IsLeader() {
        return &SetResponse{
            Success:    false,
            Error:      "not leader",
            LeaderHint: h.raftNode.LeaderID(),
        }, nil
    }
    cmd := Command{Type: CmdSet, Key: req.Key, Value: req.Value}
    encoded, _ := EncodeCommand(cmd)
    err := h.raftNode.ProposeCommand(encoded)
    if err != nil {
        return &SetResponse{Success: false, Error: err.Error()}, nil
    }
    return &SetResponse{Success: true}, nil
}
```

**ProposeCommand** must block until the entry is committed (or timeout/leadership change). Implementation:

```go
func (rn *RaftNode) ProposeCommand(cmd []byte) error {
    rn.mu.Lock()
    if rn.role != Leader {
        rn.mu.Unlock()
        return ErrNotLeader
    }
    entry := LogEntry{
        Index:   rn.lastLogIndex() + 1,
        Term:    rn.persistent.CurrentTerm,
        Command: cmd,
    }
    rn.persistent.Log = append(rn.persistent.Log, entry)
    rn.storage.AppendEntries([]LogEntry{entry})

    // Create a channel to wait for commit
    commitCh := make(chan error, 1)
    rn.pendingCommits[entry.Index] = commitCh
    rn.mu.Unlock()

    // Trigger immediate replication
    rn.triggerReplication()

    // Wait for commit or timeout
    select {
    case err := <-commitCh:
        return err
    case <-time.After(5 * time.Second):
        return ErrCommitTimeout
    }
}
```

#### 31. GET Handling

- GET does **not** need to go through the Raft log (it's a read-only operation).
- For simplicity and correctness, serve GETs from the leader's local KV state.
- The leader should verify it's still the leader (hasn't been partitioned) before serving.

```go
func (h *Handler) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
    if !h.raftNode.IsLeader() {
        return &GetResponse{Error: "not leader", ...}, nil
    }
    value, found := h.raftNode.kvStore.Get(req.Key)
    return &GetResponse{Value: value, Found: found}, nil
}
```

> [!NOTE]
> A fully linearizable read would require a read-index protocol or a no-op commit. For this project, serving reads from the leader's committed state is sufficient and correct enough for interview discussion.

---

### Phase 3D — Replication Testing (Day 6, Afternoon)

#### 32. Unit Tests

| Test | Validates |
|---|---|
| `TestBasicSetGet` | SET then GET returns correct value |
| `TestReplicationToFollowers` | All followers have same log after SET |
| `TestCommitRequiresMajority` | Entry not committed until 2/3 nodes have it |
| `TestFollowerCatchUp` | Slow follower eventually catches up |
| `TestLogConflictResolution` | Follower with conflicting entries resolves correctly |
| `TestLeaderOnlyCommitsCurrentTerm` | §5.4.2 safety check |
| `TestNonLeaderRejectsWrites` | Follower returns leader hint |
| `TestDeleteOperation` | DELETE removes key from all nodes |
| `TestMultipleOperations` | Sequence of SET/DELETE/SET applied in order |
| `TestConcurrentWrites` | Multiple concurrent SETs all committed correctly |

### Phase 3 Verification

- [ ] Client can SET/GET/DELETE through leader
- [ ] Writes replicated to all followers
- [ ] Follower state machine matches leader's
- [ ] Non-leader returns redirect with leader hint
- [ ] Concurrent writes are serialized through Raft log
- [ ] All tests pass with `-race` flag

---

## Phase 4 — Persistence & Recovery (Day 7)

### Objective
Implement durable storage using bbolt so that nodes survive crashes. By end of Phase 4, a node can crash, restart, and correctly rejoin the cluster without data loss.

---

### Phase 4A — Bbolt Storage Implementation (Day 7, Morning)

#### 33. Bbolt Store — [`internal/storage/bbolt_store.go`](file:///Users/shantanusingh/Desktop/raftra/internal/storage/bbolt_store.go)

Two bbolt buckets:

| Bucket | Keys | Purpose |
|---|---|---|
| `meta` | `current_term`, `voted_for` | Raft persistent metadata |
| `log` | `<index>` (uint64 big-endian) | Log entries |

```go
type BboltStore struct {
    db *bbolt.DB
}

func NewBboltStore(path string) (*BboltStore, error) {
    db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
    // Create buckets if not exist
    return &BboltStore{db: db}, err
}
```

**Critical persistence rules (per Raft):**
- `currentTerm` and `votedFor` MUST be persisted **before** responding to any RPC.
- Log entries MUST be persisted **before** acknowledging AppendEntries.
- Use bbolt transactions to ensure atomicity.

#### 34. Recovery on Startup

```
Node starts:
    1. Open bbolt database
    2. Load currentTerm from meta bucket
    3. Load votedFor from meta bucket
    4. Load all log entries from log bucket
    5. Initialize volatile state:
       - commitIndex = 0  (will be updated via AppendEntries)
       - lastApplied = 0
    6. Replay committed log entries to rebuild KV state:
       - For entries 1..lastKnownCommitIndex: apply to KV store
    7. Start as Follower
    8. Begin Raft event loop
```

> [!IMPORTANT]
> **Commit index is NOT persisted.** Per Raft, `commitIndex` is volatile and reconstructed from the leader's `leaderCommit` in AppendEntries. The node replays log entries up to the commit index it learns from the leader after reconnecting.

#### 35. Handling commitIndex on Recovery

Since `commitIndex` is volatile, after restart:
- The node starts with `commitIndex = 0` and `lastApplied = 0`.
- The leader's heartbeats carry `leaderCommit`, which updates the follower's `commitIndex`.
- As `commitIndex` advances, the node applies entries to its KV store.

**Alternative approach (simpler for this project):** Persist `commitIndex` alongside metadata. This is technically unnecessary per Raft but simplifies recovery by allowing immediate replay on restart without waiting for leader contact.

---

### Phase 4B — Wire Persistence into Raft (Day 7, Afternoon)

#### 36. Integration Points

Every place in the Raft code that modifies persistent state must go through the storage layer:

| Operation | Storage Call |
|---|---|
| Start election (increment term) | `SaveTerm(newTerm)` |
| Vote for candidate | `SaveVotedFor(candidateID)` |
| Receive higher term from RPC | `SaveTerm(newTerm)`, `SaveVotedFor("")` |
| Append new log entry (leader) | `AppendEntries([]LogEntry{entry})` |
| Append entries from leader (follower) | `AppendEntries(entries)` |
| Truncate conflicting entries | `TruncateFrom(index)` |

#### 37. Update `cmd/raftra-server/main.go`

- Use `BboltStore` instead of `MemoryStore`.
- Accept `--data-dir` flag for persistence directory.
- On startup, check for existing bbolt database and recover if present.

#### 38. Persistence Tests

| Test | Validates |
|---|---|
| `TestTermSurvivesRestart` | Term persisted and recovered |
| `TestVotedForSurvivesRestart` | Vote persisted and recovered |
| `TestLogSurvivesRestart` | Log entries persisted and recovered |
| `TestKVStateRebuiltOnRestart` | KV store rebuilt from replayed log |
| `TestNodeRejoinsAfterRestart` | Restarted node catches up from leader |
| `TestLeaderRestartRecovery` | Ex-leader restarts, becomes follower, catches up |

### Phase 4 Verification

- [ ] Kill node → restart → node recovers term, vote, and log
- [ ] Restarted node correctly rebuilds KV state
- [ ] Restarted node catches up with cluster
- [ ] No data loss for committed entries
- [ ] Persistence operations don't significantly impact latency (< 10ms overhead)
- [ ] All tests pass with `-race` flag

---

## Phase 5 — Failure Testing & Network Partitions (Day 8)

### Objective
Build the automated test harness and demonstrate correctness under all failure scenarios listed in the project brief.

---

### Phase 5A — Test Harness (Day 8, Morning)

#### 39. In-Process Test Cluster — [`test/chaos/harness.go`](file:///Users/shantanusingh/Desktop/raftra/test/chaos/harness.go)

```go
type TestCluster struct {
    nodes    []*raft.RaftNode
    transports []*TestTransport  // Controllable transport layer
}

// Cluster control
func (tc *TestCluster) Start() error
func (tc *TestCluster) Stop()
func (tc *TestCluster) WaitForLeader(timeout time.Duration) (*raft.RaftNode, error)
func (tc *TestCluster) GetLeader() *raft.RaftNode

// Fault injection
func (tc *TestCluster) StopNode(id string)              // Simulate crash
func (tc *TestCluster) RestartNode(id string) error      // Simulate restart
func (tc *TestCluster) PartitionNode(id string)          // Drop all traffic to/from node
func (tc *TestCluster) HealPartition(id string)          // Restore traffic
func (tc *TestCluster) SlowLink(id string, latency time.Duration)  // Add latency

// Verification
func (tc *TestCluster) AllLogsConsistent() bool          // Check all nodes have same log
func (tc *TestCluster) AllKVConsistent() bool            // Check all KV stores match
func (tc *TestCluster) GetKV(nodeID, key string) (string, bool)
```

#### 40. Controllable Transport (`TestTransport`)

A wrapper transport that can:
- **Drop messages** (simulate crash/partition)
- **Delay messages** (simulate slow network)
- **Reorder messages** (stress consistency)

```go
type TestTransport struct {
    real      Transport
    blocked   map[string]bool   // peer → blocked
    latency   map[string]time.Duration
}

func (t *TestTransport) SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
    if t.blocked[peer] {
        return nil, ErrConnectionRefused
    }
    if lat, ok := t.latency[peer]; ok {
        time.Sleep(lat)
    }
    return t.real.SendAppendEntries(peer, req)
}
```

---

### Phase 5B — Failure Scenario Tests (Day 8, Afternoon)

#### 41. Required Test Scenarios

##### Scenario 1: Leader Crash & Recovery

```
1. Start 3-node cluster
2. Wait for leader election
3. SET "key1" = "value1" (verify success)
4. Kill leader
5. Wait for new leader election (< 2s)
6. SET "key2" = "value2" (verify success)
7. GET "key1" → "value1" (data survived leader change)
8. Restart old leader
9. Wait for convergence
10. Verify all 3 nodes have consistent state
```

##### Scenario 2: Follower Crash & Recovery

```
1. Start 3-node cluster, wait for leader
2. SET "key1" = "value1"
3. Kill one follower
4. SET "key2" = "value2" (should succeed — 2/3 majority)
5. SET "key3" = "value3" (should succeed)
6. Restart follower
7. Wait for catch-up
8. Verify restarted follower has all 3 keys
```

##### Scenario 3: Network Partition (Minority Isolation)

```
1. Start 3-node cluster, wait for leader
2. SET "key1" = "value1"
3. Partition one node (isolate it from the other two)

   [Node A (leader)] ←→ [Node B]    |    [Node C (isolated)]

4. SET "key2" = "value2" on majority side (should succeed)
5. Attempt SET on isolated node (must fail — no majority)
6. Heal partition
7. Wait for convergence
8. Verify Node C has "key1" and "key2"
```

##### Scenario 4: Leader Partition (Leader Isolated)

```
1. Start 3-node cluster, leader = Node A
2. SET "key1" = "value1"
3. Partition the leader:

   [Node A (old leader, isolated)]    |    [Node B] ←→ [Node C]

4. Node B or C should elect new leader
5. SET "key2" = "value2" on new majority (should succeed)
6. Attempt SET on old leader Node A (must fail — no majority)
7. Heal partition
8. Node A should step down (discovers higher term)
9. Verify all 3 nodes converge to same state
```

##### Scenario 5: Rapid Leader Kills

```
1. Start 3-node cluster
2. Perform writes
3. Kill leader
4. Wait for new leader
5. Perform writes
6. Kill new leader
7. Wait for new leader (only 1 follower remains + 1 restarted)
8. Restart both killed nodes
9. Verify cluster converges
```

##### Scenario 6: Persistence Under Crash

```
1. Start 3-node cluster
2. SET 100 key-value pairs
3. Kill ALL nodes
4. Restart ALL nodes
5. Verify all 100 key-value pairs are present
6. Verify new leader elected
7. SET more data
8. Verify replication works
```

### Phase 5 Verification

- [ ] All 6 failure scenarios pass reliably (run 10 times each)
- [ ] No test is flaky (timing-dependent)
- [ ] Tests run in < 60 seconds total
- [ ] Tests pass with `-race` flag
- [ ] Isolated minority cannot commit writes

---

## Phase 6 — Deployment & CLI (Day 9, First Half)

### Objective
Dockerize the system, create the CLI client, and set up Docker Compose for easy 3-node cluster management.

---

#### 42. Dockerfile — [`deployments/Dockerfile`](file:///Users/shantanusingh/Desktop/raftra/deployments/Dockerfile)

```dockerfile
# Build stage
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /raftra-server ./cmd/raftra-server
RUN CGO_ENABLED=0 go build -o /raftra-cli ./cmd/raftra-cli

# Runtime stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /raftra-server /usr/local/bin/
COPY --from=builder /raftra-cli /usr/local/bin/
EXPOSE 50051 9090
ENTRYPOINT ["raftra-server"]
```

#### 43. Docker Compose — [`deployments/docker-compose.yml`](file:///Users/shantanusingh/Desktop/raftra/deployments/docker-compose.yml)

```yaml
version: '3.8'
services:
  node1:
    build: ..
    container_name: raftra-node1
    command: >
      --id=node1
      --port=50051
      --metrics-port=9090
      --peers=node2:50051,node3:50051
      --data-dir=/data
    ports:
      - "50051:50051"
      - "9091:9090"
    volumes:
      - node1-data:/data
    networks:
      - raftra-net

  node2:
    build: ..
    container_name: raftra-node2
    command: >
      --id=node2
      --port=50051
      --metrics-port=9090
      --peers=node1:50051,node3:50051
      --data-dir=/data
    ports:
      - "50052:50051"
      - "9092:9090"
    volumes:
      - node2-data:/data
    networks:
      - raftra-net

  node3:
    build: ..
    container_name: raftra-node3
    command: >
      --id=node3
      --port=50051
      --metrics-port=9090
      --peers=node1:50051,node2:50051
      --data-dir=/data
    ports:
      - "50053:50051"
      - "9093:9090"
    volumes:
      - node3-data:/data
    networks:
      - raftra-net

volumes:
  node1-data:
  node2-data:
  node3-data:

networks:
  raftra-net:
    driver: bridge
```

#### 44. CLI Client — [`cmd/raftra-cli/main.go`](file:///Users/shantanusingh/Desktop/raftra/cmd/raftra-cli/main.go)

```
Usage:
  raftra-cli --addr=localhost:50051 set <key> <value>
  raftra-cli --addr=localhost:50051 get <key>
  raftra-cli --addr=localhost:50051 delete <key>

Features:
  - Auto-follow leader redirects
  - Pretty-printed output
  - Error messages for partition/unavailable
```

### Phase 6 Verification

- [ ] `docker compose up` starts 3-node cluster
- [ ] CLI can SET/GET/DELETE through any node
- [ ] CLI follows leader redirects automatically
- [ ] `docker compose stop node1` triggers re-election
- [ ] `docker compose start node1` → node catches up

---

## Phase 7 — Benchmarking & Observability (Day 9, Second Half)

### Objective
Add Prometheus metrics, implement benchmarks, and measure system performance.

---

### Phase 7A — Metrics (Day 9, Morning-ish)

#### 45. Prometheus Metrics — [`internal/metrics/metrics.go`](file:///Users/shantanusingh/Desktop/raftra/internal/metrics/metrics.go)

| Metric | Type | Description |
|---|---|---|
| `raft_current_term` | Gauge | Current Raft term |
| `raft_node_role` | Gauge | 0=Follower, 1=Candidate, 2=Leader |
| `raft_leader_elections_total` | Counter | Total elections started |
| `raft_commit_index` | Gauge | Current commit index |
| `raft_last_applied` | Gauge | Last applied index |
| `raft_log_entries_total` | Gauge | Total log entries |
| `raft_replication_latency_seconds` | Histogram | Time to replicate to majority |
| `raft_append_entries_total` | Counter | Total AppendEntries RPCs sent |
| `raft_request_vote_total` | Counter | Total RequestVote RPCs sent |
| `kv_requests_total` | Counter | Total KV requests (by type) |
| `kv_request_duration_seconds` | Histogram | KV request latency (by type) |
| `kv_store_size` | Gauge | Number of keys in KV store |

- Expose on `/metrics` endpoint (separate HTTP port, e.g., 9090).

#### 46. Structured Logging

Use `log/slog` with fields:

```go
rn.logger.Info("became leader",
    slog.Uint64("term", rn.persistent.CurrentTerm),
    slog.String("nodeID", rn.config.NodeID),
)
```

Every log line should include at minimum: `nodeID`, `term`, `role`.

---

### Phase 7B — Benchmarking (Day 9, Afternoon)

#### 47. Load Generator — [`benchmark/loadgen/main.go`](file:///Users/shantanusingh/Desktop/raftra/benchmark/loadgen/main.go)

```
Usage:
  raftra-loadgen --addr=localhost:50051 --ops=10000 --concurrency=10 --ratio=80:20

Outputs:
  Total operations:    10000
  Duration:            12.34s
  Throughput:          810 ops/sec
  SET throughput:      648 ops/sec
  GET throughput:      162 ops/sec

  Latency (SET):
    P50:   4.2ms
    P95:  12.1ms
    P99:  25.3ms

  Latency (GET):
    P50:   1.1ms
    P95:   3.4ms
    P99:   8.7ms

  Errors:              12 (0.12%)
```

#### 48. Go Benchmark Tests — [`benchmark/bench_test.go`](file:///Users/shantanusingh/Desktop/raftra/benchmark/bench_test.go)

```go
func BenchmarkSetOperation(b *testing.B) { ... }
func BenchmarkGetOperation(b *testing.B) { ... }
func BenchmarkMixedWorkload(b *testing.B) { ... }
func BenchmarkReplicationLatency(b *testing.B) { ... }
```

#### 49. Failover Time Measurement

```
Test:
    1. Start 3-node cluster
    2. Record time T1
    3. Kill leader
    4. Poll for new leader
    5. Record time T2
    6. Failover time = T2 - T1
    7. Report average over 10 runs
```

### Phase 7 Verification

- [ ] Prometheus metrics endpoint returns valid metrics
- [ ] Load generator runs without errors at 100+ ops/sec
- [ ] Benchmark results are reproducible (< 10% variance)
- [ ] Failover time < 2 seconds consistently

---

## Phase 8 — Documentation, Polish & Final Testing (Day 10)

### Objective
Complete all documentation, run comprehensive final tests, and ensure the project is presentation-ready.

---

#### 50. Architecture Document — [`docs/architecture.md`](file:///Users/shantanusingh/Desktop/raftra/docs/architecture.md)

Include:
- System architecture diagram (Mermaid)
- Raft state machine diagram (Follower → Candidate → Leader transitions)
- Request flow diagram (Client → Leader → Replication → Commit → Apply)
- Package dependency diagram
- Key design decisions and trade-offs

#### 51. Raft Protocol Document — [`docs/raft-protocol.md`](file:///Users/shantanusingh/Desktop/raftra/docs/raft-protocol.md)

- How the implementation maps to the Raft paper
- Deviations from the paper and why
- Safety properties maintained
- Liveness properties and timing assumptions

#### 52. Demo Walkthrough — [`docs/demo.md`](file:///Users/shantanusingh/Desktop/raftra/docs/demo.md)

Step-by-step demo script:

```
# Start cluster
docker compose up -d

# Verify leader election
raftra-cli --addr=localhost:50051 set name shantanu

# Verify replication
raftra-cli --addr=localhost:50052 get name  → "shantanu"

# Kill leader
docker compose stop node1

# Verify new leader
raftra-cli --addr=localhost:50052 set city mumbai

# Restart old leader
docker compose start node1

# Verify convergence
raftra-cli --addr=localhost:50051 get city  → "mumbai"

# Run benchmarks
raftra-loadgen --addr=localhost:50052 --ops=5000
```

#### 53. README.md

- Project description
- Architecture overview
- Quick start (Docker Compose)
- CLI usage
- Running tests
- Benchmark results
- What this project demonstrates
- What's out of scope and why

#### 54. Final Test Suite Run

```bash
# Unit tests
go test -race -v ./internal/...

# Integration tests
go test -race -v -tags=integration ./test/integration/...

# Chaos tests
go test -race -v -tags=chaos ./test/chaos/...

# Benchmarks
go test -bench=. -benchmem ./benchmark/...

# Docker smoke test
docker compose up -d
# Run CLI smoke tests
docker compose down
```

### Phase 8 Verification

- [ ] All tests pass (unit + integration + chaos)
- [ ] Docker Compose cluster works end-to-end
- [ ] Demo walkthrough runs without issues
- [ ] README is complete
- [ ] Architecture docs include diagrams
- [ ] No TODO/FIXME/HACK comments in code
- [ ] Code is `go vet` and `golangci-lint` clean

---

## Summary Timeline

| Day | Phase | Deliverable |
|-----|-------|-------------|
| 1 | 1A–1B | Go module, protobuf, Raft shell, interfaces |
| 2 | 1C | gRPC server, node startup, KV store shell |
| 3 | 2A–2B | Election timer, RequestVote, heartbeats |
| 4 | 2C | Election edge cases, election unit tests |
| 5 | 3A–3B | Log replication (leader + follower sides) |
| 6 | 3C–3D | Commitment, client API, replication tests |
| 7 | 4A–4B | Bbolt persistence, crash recovery |
| 8 | 5A–5B | Test harness, all 6 failure scenarios |
| 9 | 6 + 7 | Docker, CLI, metrics, benchmarks |
| 10 | 8 | Docs, polish, final testing |

---

## Risk Mitigation

| Risk | Mitigation |
|---|---|
| Concurrency bugs in Raft core | Run ALL tests with `-race` flag. Single mutex strategy for Raft state. |
| Flaky timing-dependent tests | Use controllable timers in test harness. Allow generous timeouts. |
| Persistence slows down replication | Batch writes. Use async persistence where Raft allows. |
| Phase 2 takes longer than 2 days | Skip log up-to-date optimization (decrement by 1), add later. |
| Phase 5 tests are complex | Start with simplest scenario (leader crash), add complexity incrementally. |
| Docker networking issues | Test locally with separate ports first, Docker is a thin layer on top. |

---

## Key Raft Invariants to Verify Throughout

These invariants must hold at ALL times. Every test should implicitly or explicitly check them:

1. **Election Safety**: At most one leader per term.
2. **Leader Append-Only**: A leader never overwrites or deletes entries in its log.
3. **Log Matching**: If two logs contain an entry with the same index and term, the logs are identical in all preceding entries.
4. **Leader Completeness**: If an entry is committed in a given term, it will be present in the logs of all leaders for all higher terms.
5. **State Machine Safety**: If a node has applied a log entry at a given index, no other node will ever apply a different entry at that index.

---

## Dependencies (Go Modules)

```
google.golang.org/grpc          v1.65+
google.golang.org/protobuf      v1.34+
go.etcd.io/bbolt                v1.3+
github.com/prometheus/client_golang  v1.19+
github.com/stretchr/testify     v1.9+    (test assertions)
```

## Tools Required

- Go 1.22+
- `protoc` (Protocol Buffers compiler)
- `protoc-gen-go` + `protoc-gen-go-grpc`
- Docker + Docker Compose
- `golangci-lint` (optional, for code quality)
