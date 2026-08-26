# Raftra: Fault-Tolerant Distributed Key-Value Store

Raftra is a robust, academically rigorous distributed key-value store built entirely from scratch in Go. It utilizes the **Raft consensus algorithm** to achieve strong consistency and fault tolerance across a cluster of independent nodes. 

This project is not a thin wrapper around an existing database engine or consensus library. It is a fundamental implementation of distributed systems theory, designed to survive node crashes, network partitions, and unpredictable delays while maintaining absolute data correctness.

---

## 🎯 Project Overview & Objectives

In a traditional single-server architecture, a crash means downtime or data loss. Raftra solves this by replicating the state machine (a key-value store) across a cluster of nodes. As long as a majority (quorum) of nodes remain operational and can communicate, the cluster continues to function normally.

**Primary Engineering Goals:**
1. **Absolute Correctness**: Adherence to the Raft paper ("In Search of an Understandable Consensus Algorithm") is strict. A partitioned minority cluster cannot commit conflicting writes. 
2. **Resilience & Fault Tolerance**: The system must seamlessly handle and recover from leader failures, follower crashes, and network partitions.
3. **Understandability**: The architecture distinctly separates the transport layer, consensus engine, persistent storage, and the application state machine.
4. **Testability**: Deep automated integration and chaos testing to prove the system works under extreme duress.

*(Note: Performance and extra features are explicitly secondary to correctness and safety.)*

---

## 🏗️ System Architecture

Raftra typically runs as a cluster of 3 nodes (though the consensus logic supports arbitrary sizes). 

```mermaid
graph TD
    Client((Client)) -->|gRPC SET/GET| Leader

    subgraph Raftra Cluster
        Leader[Node 1: Leader]
        Follower1[Node 2: Follower]
        Follower2[Node 3: Follower]

        Leader <-->|gRPC AppendEntries| Follower1
        Leader <-->|gRPC AppendEntries| Follower2
        Follower1 <..>|gRPC RequestVote| Follower2
    end
    
    Leader -->|Transactions| DiskL[(Bbolt Storage)]
    Follower1 -->|Transactions| DiskF1[(Bbolt Storage)]
    Follower2 -->|Transactions| DiskF2[(Bbolt Storage)]
```

### Core Node Components

Every node in the cluster is composed of four distinct layers:

1. **Transport Layer**: Manages inter-node communication and client-facing API via gRPC and Protocol Buffers.
2. **Consensus Engine (Raft)**: The central brain. It manages the node's current role (Follower, Candidate, Leader), election timers, term numbers, and log replication.
3. **Persistence Layer**: Built on top of Bbolt (a transactional B+ tree key-value store). It durably stores essential Raft metadata (Current Term, Voted For) and the append-only Raft log. This ensures a node can crash, restart, and perfectly reconstruct its state.
4. **Key-Value State Machine**: An in-memory map storing the actual user data. It is completely isolated from the network and only ever mutates when the consensus engine instructs it to apply a firmly committed log entry.

---

## ⚙️ Core Mechanics: How Raftra Works

### Leader Election
All nodes begin as **Followers**. If a Follower receives no heartbeats from a Leader within a randomized timeout window, it transitions to a **Candidate**. It increments its "term" (a logical clock), votes for itself, and asks the other nodes for votes. If it gathers votes from a majority of the cluster, it promotes itself to **Leader** and begins emitting its own heartbeats to assert authority.

### Log Replication & Commitment
Clients send modification commands (`SET`, `DELETE`) to the Leader. 
1. The Leader appends the command to its local log.
2. The Leader transmits the log entry to all Followers.
3. Once a majority of Followers have durably written the entry to their own disks and acknowledged it, the entry is considered **Committed**.
4. The Leader applies the committed entry to its Key-Value state machine and replies to the client.

### Resolving Conflicts
If a node crashes or the network partitions, nodes may end up with conflicting logs. Raftra strictly enforces the "Log Matching Property." The Leader forces Followers' logs to duplicate its own, automatically truncating uncommitted, conflicting entries from older, failed Leaders.

---

## 🚀 Implementation Phases (10-Day Plan)

The project is structured into a rigorous 10-day, 8-phase implementation plan. Each phase builds upon the last, ending with a strict verification checkpoint.

### Phase 1: Foundation & Node Architecture (Days 1–2)
- **Goal:** Set up the project skeleton, define gRPC/Protobuf interfaces, and establish the Raft state machine shell.
- **Key Deliverables:** Protobuf definitions (`raft.proto`), configuration parsing, concurrent node lifecycle management, and a dummy in-memory KV store.

### Phase 2: Leader Election (Days 3–4)
- **Goal:** Implement Raft leader election per Section 5.2 of the Raft specification.
- **Key Deliverables:** Randomized election timers, Candidate transitions, RequestVote RPC logic, log up-to-date safety checks, and heartbeat broadcasting.

### Phase 3: Log Replication & Commitment (Days 5–6)
- **Goal:** Implement the complete log replication pipeline. 
- **Key Deliverables:** AppendEntries RPC implementation (both leader dispatch and follower reception), strict log conflict resolution, and majority-based commit index advancement. The KV state machine begins applying actual committed commands.

### Phase 4: Persistence & Recovery (Day 7)
- **Goal:** Implement durable storage so nodes survive process crashes.
- **Key Deliverables:** Bbolt integration. Transactional saving of `currentTerm`, `votedFor`, and log entries prior to acknowledging RPCs. Logic to rebuild the volatile commit index and state machine upon a node restarting.

### Phase 5: Failure & Chaos Testing (Day 8)
- **Goal:** Build an automated test harness to prove correctness under extreme duress.
- **Key Deliverables:** Automated scenarios testing Leader crashes, Follower crashes, network partitions (minority isolation), split-brain scenarios, and whole-cluster restart verifications.

### Phase 6: Deployment & CLI (Day 9, First Half)
- **Goal:** Package the cluster for easy execution and interaction.
- **Key Deliverables:** Dockerization of the node server, a `docker-compose` cluster layout, and a custom CLI tool (`raftra-cli`) that automatically handles leader redirects for client operations.

### Phase 7: Benchmarking & Observability (Day 9, Second Half)
- **Goal:** Measure system performance and provide introspection.
- **Key Deliverables:** Prometheus metrics (tracking terms, commit indices, replication latency), structured logging (`log/slog`), and a custom load generator to benchmark throughput and latency.

### Phase 8: Polish & Documentation (Day 10)
- **Goal:** Finalize the project for presentation and deep-dive technical discussions.
- **Key Deliverables:** Comprehensive architecture documentation, protocol design choices, full test-suite pass, and a recorded demo walkthrough.

---

## 🛠️ Technology Stack

*   **Language**: Go (Golang)
*   **RPC Framework**: gRPC
*   **Serialization**: Protocol Buffers (Protobuf)
*   **Storage Engine**: Bbolt (Embedded B+ Tree database)
*   **Deployment**: Docker & Docker Compose
*   **Observability**: Prometheus & Go `log/slog`
*   **Testing**: Go testing framework with custom chaos/fault-injection harnesses.
