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
