# Raftra Benchmark Results

This document records the performance and benchmark test results for the Raftra distributed KV store.

---

## Load Generator Results (HTTP Stress Test)

*Tool: `bin/raftra-loadgen`*
*Cluster: 3-node Raft cluster on a single MacBook (localhost)*

### Test 1: 1,000,000 Operations — Benchmark Mode (`-nosync`)

```
Total Operations:    1,000,000
Duration:            171.17s (~2.85 minutes)
Throughput:          5,842.3 ops/sec
SET Throughput:      4,674.4 ops/sec (800,094 ops)
GET Throughput:      1,167.9 ops/sec (199,906 ops)
Errors:              0 (0.00%)

SET Latency (Quorum Replicated):
  P50:   20.84ms
  P95:   30.95ms
  P99:   40.23ms
  Min:   449.50µs
  Max:   227.87ms

GET Latency (In-Memory Read):
  P50:   93.92µs
  P95:   530.08µs
  P99:   1.48ms
  Min:   28.25µs
  Max:   101.65ms
```

**Config:** `-ops=1000000 -concurrency=100 -ratio=80:20 -addr=http://localhost:8001`
**Server flags:** `-nosync` (bbolt fsync disabled for benchmark mode)

### Test 2: 10,000 Operations — Durability Mode (Default)

```
Total Operations:    10,000
Duration:            106.76s
Throughput:          93.7 ops/sec
SET Throughput:      74.8 ops/sec (7,986 ops)
GET Throughput:      18.9 ops/sec (2,014 ops)
Errors:              0 (0.00%)

SET Latency (Quorum Replicated):
  P50:   1,323.76ms
  P95:   1,539.16ms
  P99:   1,774.95ms
  Min:   138.05ms
  Max:   2,152.76ms

GET Latency (In-Memory Read):
  P50:   158.21µs
  P95:   2.50ms
  P99:   7.61ms
  Min:   54.08µs
  Max:   11.75ms
```

**Config:** `-ops=10000 -concurrency=100 -ratio=80:20 -addr=http://localhost:8001`
**Server flags:** Default (full fsync durability)

---

## Analysis

| Metric               | Durable Mode   | Benchmark Mode (`-nosync`) | Speedup |
|----------------------|----------------|---------------------------|---------|
| Throughput           | 93.7 ops/sec   | 5,842.3 ops/sec           | **62x** |
| SET P50 Latency      | 1,323.76ms     | 20.84ms                   | **63x** |
| SET P99 Latency      | 1,774.95ms     | 40.23ms                   | **44x** |
| GET P50 Latency      | 158.21µs       | 93.92µs                   | **1.7x**|
| Error Rate           | 0%             | 0%                        | Same    |

**Key Finding:** The bottleneck in durable mode is macOS `fsync` (~10ms per call). The Raft consensus engine itself is fast — proven by the 62x speedup when fsync is bypassed. In production on Linux with NVMe SSDs, fsync latency drops to ~0.1ms, which would yield near-benchmark-mode performance with full durability.

---

## Go Microbenchmarks (`make bench`)

*Recorded: 2026-10-03 · Apple M2 (8 cores), 8 GB RAM, macOS, Go 1.26.4, `GOARCH=arm64`*
*Setup: 3-node in-process cluster via `test/chaos` harness — real bbolt **with fsync**, in-memory transport (no gRPC/HTTP)*
*Command: `go test -bench=. -benchmem -count=3 -run='^$' ./benchmark/`*

```
BenchmarkSetOperation-8              81    14515229 ns/op                         161481 B/op   881 allocs/op
BenchmarkSetOperation-8              92    14450537 ns/op                         161538 B/op   881 allocs/op
BenchmarkSetOperation-8              79    15537212 ns/op                         167454 B/op   889 allocs/op
BenchmarkGetOperation-8        40469042          29.21 ns/op                           0 B/op     0 allocs/op
BenchmarkGetOperation-8        39991681          29.43 ns/op                           0 B/op     0 allocs/op
BenchmarkGetOperation-8        40177046          29.45 ns/op                           0 B/op     0 allocs/op
BenchmarkMixedWorkload-8            469     2562557 ns/op                          36529 B/op   190 allocs/op
BenchmarkMixedWorkload-8            482     2508305 ns/op                          36801 B/op   190 allocs/op
BenchmarkMixedWorkload-8            423     3003328 ns/op                          37632 B/op   193 allocs/op
BenchmarkReplicationLatency-8        76    16538412 ns/op   16460 us/quorum-commit 160894 B/op   888 allocs/op
BenchmarkReplicationLatency-8        76    15240765 ns/op   15236 us/quorum-commit 160840 B/op   882 allocs/op
BenchmarkReplicationLatency-8        91    15033553 ns/op   15028 us/quorum-commit 163408 B/op   883 allocs/op
```

| Benchmark | Mean | Derived rate | Spread (max/min) |
|---|---|---|---|
| SetOperation (sequential quorum write) | 14.83 ms/op | ~67 writes/s | 7.5% |
| GetOperation (state-machine read) | 29.36 ns/op, 0 allocs | ~34M reads/s | 0.8% |
| MixedWorkload (80% read / 20% write, parallel) | 2.69 ms/op | ~372 ops/s | 19.7% |
| ReplicationLatency (proposal → quorum commit) | 15.58 ms | — | 9.5% |

---

## Failover Benchmark (`make bench-failover`)

*Recorded: 2026-10-03 · same machine · 2 passes × 10 trials, each on a fresh 3-node cluster*
*Failover time = leader crash → first write committed by the new leader (detection + election + quorum commit)*

```
Pass 1: 224, 232, 222, 265, 284, 611, 494, 210, 281, 191 ms   → min 191ms  max 611ms  avg 301ms
Pass 2: 294, 294, 303, 252, 264, 575, 228, 237, 516, 460 ms   → min 228ms  max 575ms  avg 342ms
```

| Metric (20 trials) | Value |
|---|---|
| Average | **322 ms** |
| Median | 273 ms |
| Min / Max | 191 ms / 611 ms |
| Trials ≤ 303 ms | 15 / 20 |
| Requirement (avg < 2 s) | ✅ met by ~6× |

Trials of ~460–611 ms are consistent with a split vote followed by a second randomized election round.

> **Measurement fix (2026-10-03):** an earlier version of this test sometimes crashed a leader that had *already* been deposed by a startup election, producing impossible 18–35 ms "failovers" (below the 150 ms minimum election timeout). The test now waits for the warm-up write to reach every node and crashes the highest-term leader. All numbers above are from the fixed test.
