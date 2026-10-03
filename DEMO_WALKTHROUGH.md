# Raftra: Demo Walkthrough

Welcome to the Raftra demo! This guide will walk you through setting up a 3-node cluster, interacting with it, and watching it recover from failures.

## Prerequisites

1. Build the binaries:
   ```bash
   make build
   ```

2. Open 4 terminal windows. You'll use three for the server nodes and one for the client (`raftra-cli`).

---

## 1. Starting the Cluster

Start each node in its own terminal. We'll use the `-nosync` flag for faster benchmarking, but you can omit it for full disk durability.

**Terminal 1 (Node 1):**
```bash
./bin/raftra-server -id node1 -port 50051 -http-port 8001 -data-dir data1 -nosync -peers node2:localhost:50052,node3:localhost:50053 -http-peers node2:http://localhost:8002,node3:http://localhost:8003
```

**Terminal 2 (Node 2):**
```bash
./bin/raftra-server -id node2 -port 50052 -http-port 8002 -data-dir data2 -nosync -peers node1:localhost:50051,node3:localhost:50053 -http-peers node1:http://localhost:8001,node3:http://localhost:8003
```

**Terminal 3 (Node 3):**
```bash
./bin/raftra-server -id node3 -port 50053 -http-port 8003 -data-dir data3 -nosync -peers node1:localhost:50051,node2:localhost:50052 -http-peers node1:http://localhost:8001,node2:http://localhost:8002
```

Notice in the logs that the nodes will initially start as followers, eventually time out, and one will become the leader.

---

## 2. Interacting via CLI

In **Terminal 4**, start the interactive CLI:

```bash
./bin/raftra-cli --addr http://localhost:8001,http://localhost:8002,http://localhost:8003
```

### Checking Status

First, let's see the cluster status and find out who the leader is:

```text
> status
```

You'll see which node leads the current term, and every node's role, term and commit index.

### Writing Data

Let's add some data. The CLI automatically handles HTTP 307 redirects if it hits a follower.

```text
> set message "Hello, Raftra!"
> set user:1 "Shantanu"
```

### Reading Data

```text
> get message
> get user:1
```

---

## 3. Chaos Engineering: Simulating Failures

Raftra is fault-tolerant. Let's prove it by killing the leader!

1. Look at your `status` output to see which node is the leader (e.g., `node1`).
2. Go to the terminal running the leader (e.g., Terminal 1) and press **Ctrl+C** to kill it.
3. Quickly look at the logs in the remaining two terminals. You will see one of them notice the lack of heartbeats, transition to `Candidate`, and win a new election!
4. Go back to the CLI in Terminal 4 and run:

```text
> status
```

You'll see the term has increased, and a new leader has emerged! The remaining two nodes form a majority (2 out of 3), so the cluster is still healthy.

### Reading and Writing During Failover

Verify the data is still there:

```text
> get message
```

Try writing new data (the cluster still has a quorum):

```text
> set status "Still alive!"
```

### Node Recovery

Now, restart the node you killed in its terminal:

```bash
# Example: Restarting Node 1
./bin/raftra-server -id node1 -port 50051 -http-port 8001 -data-dir data1 -nosync -peers node2:localhost:50052,node3:localhost:50053 -http-peers node2:http://localhost:8002,node3:http://localhost:8003
```

Watch the logs: The node will wake up, realize it's out of date, and the leader will automatically replicate the missing `status "Still alive!"` entry to it.

---

## 4. Load Testing (Bonus)

If you want to stress-test the cluster, stop the CLI and run the load generator:

```bash
./bin/raftra-loadgen -ops=10000 -concurrency=100 -ratio=80:20 -addr=http://localhost:8001
```

This will blast the cluster with 10,000 operations and give you a performance summary.

---
**Congratulations!** You've just witnessed distributed consensus in action.
