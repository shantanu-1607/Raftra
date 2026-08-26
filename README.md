# Raftra: Fault-Tolerant Distributed Key-Value Store

![Status](https://img.shields.io/badge/Status-In_Progress-yellow)
![Go Version](https://img.shields.io/badge/Go-1.22+-blue)
![Consensus](https://img.shields.io/badge/Consensus-Raft-orange)
![License](https://img.shields.io/badge/License-MIT-purple)

A fault-tolerant distributed key-value store built entirely from scratch in Go, using a self-implemented **Raft consensus algorithm** for leader election, log replication, and automatic failover across a 3-node cluster.

## 📖 The Core Problem

A single-server key-value store is simple but fragile — if the server crashes, the service goes down and data can be lost. Raftra solves this by replicating every write across multiple nodes using the Raft consensus protocol. As long as a majority of nodes (2 out of 3) remain healthy and connected, the cluster continues to operate normally — even if the leader crashes, a follower dies, or a network partition splits the cluster.

**What makes this interesting:** The Raft consensus engine is implemented from scratch following the original paper ("In Search of an Understandable Consensus Algorithm"), not imported from an existing library. Every RPC handler, every election timer, and every log conflict resolution routine is hand-written and tested.

---

## 🏗️ Architecture

```text
                          ┌──────────┐
                          │  Client  │
                          └────┬─────┘
                               │ gRPC (SET / GET / DELETE)
                               ▼
              ┌────────────────────────────────┐
              │         Raftra Cluster         │
              │                                │
              │   ┌──────────────────────┐     │
              │   │   Node 1 (Leader)    │     │
              │   │  ┌───────────────┐   │     │
              │   │  │ Raft Engine   │   │     │
              │   │  │ KV State Mach │   │     │
              │   │  │ Bbolt Storage │   │     │
              │   │  └───────────────┘   │     │
              │   └──────┬───────┬───────┘     │
              │          │       │              │
              │    AppendEntries  AppendEntries │
              │    + Heartbeats   + Heartbeats  │
              │          │       │              │
              │   ┌──────▼──┐ ┌─▼───────────┐  │
              │   │ Node 2  │ │   Node 3    │  │
              │   │Follower │ │  Follower   │  │
              │   │ + Bbolt │ │  + Bbolt    │  │
              │   └─────────┘ └─────────────┘  │
              └────────────────────────────────┘
```

### Node Internals

Each node is composed of four layers:

| Layer | Responsibility |
| :--- | :--- |
| **Transport** | gRPC server/client for inter-node Raft RPCs and client-facing KV API |
| **Consensus Engine** | Raft state machine — manages roles (Follower/Candidate/Leader), terms, elections, and log replication |
| **Persistence** | Bbolt-backed durable storage for `currentTerm`, `votedFor`, and the append-only Raft log |
| **KV State Machine** | In-memory `map[string]string` that only mutates when the consensus engine applies a committed entry |

---

## ⚙️ How It Works

### Leader Election

All nodes start as **Followers**. If a Follower receives no heartbeat within a randomized timeout, it becomes a **Candidate**, increments its term, votes for itself, and sends `RequestVote` RPCs. A majority of votes wins the election. The new Leader immediately begins sending heartbeats to maintain authority.

### Log Replication

Clients send writes (`SET`, `DELETE`) to the Leader. The Leader appends the command to its local log, replicates it to Followers via `AppendEntries` RPCs, and only considers the entry **committed** once a majority of nodes have durably stored it. Committed entries are applied to the KV state machine in order.

### Fault Tolerance

| Failure | Behavior |
| :--- | :--- |
| **Leader crashes** | Followers detect timeout, elect a new leader, cluster continues |
| **Follower crashes** | Leader continues with remaining majority; crashed node catches up on restart |
| **Network partition** | Majority side continues operating; minority side cannot commit (no quorum) |
| **Node restart** | Recovers persistent state from Bbolt, rejoins cluster, replays missed log entries |

---

## 🛡️ Correctness Guarantees

Raftra enforces the five key Raft safety properties:

| Property | Guarantee |
| :--- | :--- |
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites or deletes its own log entries |
| **Log Matching** | If two logs have an entry with the same index and term, all preceding entries are identical |
| **Leader Completeness** | A committed entry is present in the log of every future leader |
| **State Machine Safety** | No two nodes ever apply different entries at the same log index |

---

## 🛠️ Technology Stack

| Component | Technology |
| :--- | :--- |
| Language | Go |
| Consensus | Self-implemented Raft |
| RPC | gRPC + Protocol Buffers |
| Storage | Bbolt (embedded B+ tree) |
| State Machine | In-memory Go map |
| Deployment | Docker + Docker Compose |
| Metrics | Prometheus |
| Logging | Go `log/slog` (structured) |
| Testing | Go test + custom chaos harness |

---

## 🗺️ Roadmap & Phase Completion

| Phase | Day | Deliverable | Status |
| :--- | :---: | :--- | :---: |
| 1 | 1–2 | Project skeleton, gRPC/Protobuf definitions, node lifecycle | ⬜ |
| 2 | 3–4 | Leader election, terms, voting, heartbeats | ⬜ |
| 3 | 5–6 | Log replication, conflict resolution, majority commit, client API | ⬜ |
| 4 | 7 | Bbolt persistence, crash recovery, state rebuild on restart | ⬜ |
| 5 | 8 | Automated chaos testing (leader kill, partition, split-brain) | ⬜ |
| 6 | 9 | Docker Compose cluster, CLI client with leader-redirect | ⬜ |
| 7 | 9 | Prometheus metrics, structured logging, benchmarks | ⬜ |
| 8 | 10 | Documentation, architecture diagrams, demo walkthrough, final test sweep | ⬜ |

---

## 🚫 Explicitly Out of Scope

This project is intentionally scoped to be completable in 10 days with a focus on correctness. The following are deliberately excluded:

- Dynamic cluster membership changes
- Snapshots / log compaction
- Multi-region or multi-datacenter deployment
- Sharding, transactions, or MVCC
- Authentication, TLS, or production security
- Kubernetes deployment
- Web dashboard

---

## 📄 License

This project is licensed under the MIT License.
