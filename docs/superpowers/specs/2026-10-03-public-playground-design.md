# Raftra Public Playground: Design

**Date:** 2026-10-03
**Status:** Draft, awaiting review
**Goal:** Anyone on the internet can download `raftra-cli`, use a live 3-node Raftra cluster on AWS, and watch it survive leader crashes on a landing page. It should run on about $15/month of AWS student credits.

---

## 1. Decisions already made

| Question | Decision |
|---|---|
| Who can write? | Anyone. It's an open playground that resets every hour. |
| Budget | As cheap as possible: $100 of AWS student credits. |
| Domain | None. Free DuckDNS hostnames for nodes, GitHub Pages for the site. |
| Live demo | Automatic chaos: the leader is killed every 10 minutes. There is no visitor-facing kill button. |
| Hosting | 3 × AWS Lightsail $5 instances, one Raftra node each, in 3 availability zones. Moving to EC2 later reuses everything except provisioning. |

## 2. Architecture

```
                      Internet (anyone)
          ┌──────────────────┼───────────────────────┐
          │ HTTPS (443) only │                       │
          ▼                  ▼                       ▼
  raftra-n1.duckdns.org  raftra-n2.duckdns.org  raftra-n3.duckdns.org
 ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
 │ Lightsail $5     │ │ Lightsail $5     │ │ Lightsail $5     │
 │ AZ a             │ │ AZ b             │ │ AZ c             │
 │ Caddy :443 ─────►│ │ Caddy :443 ─────►│ │ Caddy :443 ─────►│
 │ raftra :8001     │ │ raftra :8001     │ │ raftra :8001     │
 │ chaos + reset    │ │ chaos + reset    │ │ chaos + reset    │
 └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
          └──── Raft gRPC :50051 over PRIVATE IPs ──┘

 GitHub (free): site/ → GitHub Pages · release binaries → GitHub Releases
```

- **Region:** `ap-south-1` (Mumbai), the closest to the developer. The 3 instances go in AZs `a`, `b` and `c`. The $5 price was checked on the global pricing page. Confirm the Mumbai price in the console before creating the instances.
- **Hostnames:** `raftra-n1/n2/n3.duckdns.org` are the intended names. If any are taken, use similar free names. They only appear in `raftra.env`, `site/config.js` and the release ldflags.
- **Instance:** Lightsail Linux $5 bundle: 0.5 GB RAM, 2 vCPU, 20 GB SSD, 1 TB transfer, public IPv4 and a free attached static IP. Ubuntu 24.04 LTS.
- **No Docker on the servers.** 0.5 GB of RAM is too tight. Each node runs the static `raftra-server` binary as a systemd service. The existing Docker Compose setup stays for local use.
- **Each node has its own public hostname.** This lets the existing 307 leader redirects work unchanged (`-http-peers` holds the public HTTPS URLs), and lets the landing page check each node independently.
- **Raft gRPC (port 50051) is never exposed publicly.** It has no authentication, so anyone who could reach it could send forged `AppendEntries` and corrupt the cluster. Lightsail firewall rules only filter traffic arriving on the public IP. Traffic between instances on private IPs in the same region is allowed, so `-peers` uses private IPs.

### Firewall (public IP), identical on all 3 instances

| Port | Source | Purpose |
|---|---|---|
| 22/tcp | Developer's IP, plus "Allow Lightsail browser SSH" | Administration |
| 80/tcp | Anywhere | Let's Encrypt HTTP challenge (Caddy redirects to 443) |
| 443/tcp | Anywhere | Public API and `/status` through Caddy |

Ports 8001 and 50051 are not opened publicly.

## 3. Part 1: Server playground limits (Go)

All new behavior is controlled by flags that **default to off**. Local runs, Docker Compose, tests and benchmarks behave exactly as today.

### New `raftra-server` flags

| Flag | Default | Playground value | Effect |
|---|---|---|---|
| `-max-key-bytes` | `0` (off) | `128` | Write with a longer key → `413` |
| `-max-value-bytes` | `0` (off) | `1024` | `PUT`/`POST` with a larger body → `413` |
| `-max-keys` | `0` (off) | `10000` | Write that would add a new key when the store is full → `507`. Overwrites and deletes are still allowed. |
| `-write-rate` | `0` (off) | `5` | Writes per second allowed per client IP (token bucket) |
| `-write-burst` | `0` | `10` | Bucket size, i.e. a short burst allowance. If it's `0` while `-write-rate` is on, the burst is `ceil(write-rate)`. |
| `-trust-proxy` | `false` | `true` | Take the client IP from the last `X-Forwarded-For` value instead of the TCP peer address. Only safe behind Caddy. |
| `-cors-origin` | `""` (off) | `https://shantanu-1607.github.io` | Adds `Access-Control-Allow-Origin` to `GET /status` so the landing page can read it. `*` is useful for local development. |

### Behavior details

- Limits apply only to **HTTP writes** (`PUT`, `POST`, `DELETE`) handled on the node itself. A follower's 307 redirect is cheap and isn't rate-limited. The leader enforces the limits when it actually proposes. The gRPC `KVService` isn't reachable publicly, so it isn't limited.
- Order of checks on a write: redirect-if-follower → rate limit (`429`) → key size (`413`) → key-count cap (`507`) → body size (`413`, read via `http.MaxBytesReader`) → `ProposeCommand`. The cheap checks run before the body is read.
- `429` responses include `Retry-After: 1`. Every error body uses the existing `{"error": "..."}` JSON shape.
- The key-count cap is best-effort. It checks `KVStoreSize()` before proposing, just like the existing `POST` create-if-absent check.
- **Rate limiter:** a small in-repo token bucket keyed by client IP (`map[string]*bucket` behind a mutex), with an injectable clock for tests. Idle buckets are swept once a minute. **No new dependency:** this avoids pulling in `golang.org/x/time/rate`, per the `AGENTS.md` rule.
- **Code layout:** new `internal/transport/limits.go` (config struct and write-limit checks) and `internal/transport/ratelimit.go` (token-bucket limiter and client-IP helper). `http_server.go` takes a `Limits` value. `cmd/raftra-server/main.go` parses the flags.

## 4. Part 2: AWS deployment (Lightsail)

The developer runs the AWS console steps. The repository provides scripts and a runbook in `deployments/lightsail/`.

### Files

| File | Purpose |
|---|---|
| `README.md` | Step-by-step runbook: create instances, static IPs, DuckDNS, firewall, run setup, verify, budget alarm, teardown |
| `setup-node.sh` | Run once per instance as root. Creates the `raftra` user and `/var/lib/raftra`, downloads `raftra-server` (linux/amd64) from the GitHub Release, installs Caddy from its official apt repo, writes config, installs and enables the systemd units. Idempotent. |
| `raftra.env.example` | Per-node settings: `NODE_ID`, `PEERS` (private IPs), `HTTP_PEERS` (public HTTPS URLs), `PUBLIC_HOSTNAME`, limit flags. Installed as `/etc/raftra/raftra.env`. |
| `raftra.service` | systemd unit for `raftra-server` with `Restart=on-failure` and `RestartSec=45` |
| `Caddyfile` | `{$PUBLIC_HOSTNAME} { reverse_proxy 127.0.0.1:8001 }` plus `request_body max_size 2KB` as a second line of defense |
| `chaos.sh` + `raftra-chaos.service` + `raftra-chaos.timer` | Leader chaos (below) |
| `reset.sh` + `raftra-reset.service` + `raftra-reset.timer` | Hourly wipe (below) |
| `deploy.sh` | Run from the laptop: SSH to each node in turn, download a given release version, and restart. One node at a time, so the cluster keeps quorum during upgrades. |

### Chaos: kill the leader every 10 minutes

- The timer fires at minutes `:05, :15, :25, :35, :45, :55` on **every** node.
- `chaos.sh` asks the local node `curl -s 127.0.0.1:8001/status`. **Only if** the reply contains `"is_leader":true` does it run `systemctl kill --signal=SIGKILL raftra`. SIGKILL simulates pulling the power plug: no graceful shutdown.
- systemd sees the crash and restarts the node **45 seconds later** (`RestartSec=45`). That's long enough for the landing page to show the dead node, the new election, and the old node rejoining and catching up.
- No cross-machine coordination is needed, because only the node that is leader acts.

### Reset: wipe the playground every hour

- The timer fires at `:00:00` on every node with `AccuracySec=1s`. **Required:** the systemd default accuracy is 1 minute, which could make nodes reset up to a minute apart.
- `reset.sh`: `systemctl stop raftra` → delete `/var/lib/raftra/*` → sleep until `:00:30` → `systemctl start raftra`.
- **Why the 30-second gap:** every node must be stopped and wiped before any node starts again. Otherwise a slow node that still has the old log and a higher term could become leader and re-replicate the old data into the freshly wiped nodes. Clocks are NTP-synced (Ubuntu default), so a 30 s margin is ample.
- The reset also bounds log growth. There's no snapshotting, so the bbolt log and the in-memory log would otherwise grow forever, and the whole log is loaded into memory on restart.
- The chaos run at `:55` restarts its node at about `:55:45`, well clear of the `:00` reset.

### Cost and safety

- 3 × $5 = **$15/month**. Static IPs are included while attached. $100 of credits ≈ 6.5 months; check the credits' expiry date in AWS Billing.
- **AWS Budgets alarm** at $20/month, emailed to the developer.
- **Teardown** (in the runbook): delete the 3 instances **and release the 3 static IPs**. Unattached static IPs are billed.

## 5. Part 3: CLI distribution

### CLI changes (`cmd/raftra-cli`)

- `version` and `defaultAddr` become `var`s that are set at build time with `-ldflags -X`. `make build` keeps `http://localhost:8001`, so `DEMO_WALKTHROUGH.md` is unchanged. Release builds set the three public node URLs.
- `--addr` accepts a **comma-separated list**. Each command tries the addresses in order. Connection errors and `502`/`503`/`504` are retryable: move to the next address, and after a full pass wait 300 ms, retrying for up to ~3 s. This matters because chaos makes one node unreachable for 45 s at a time, and Caddy answers `502` while its node is down.
- Friendly messages for the new limits: `413` → "key or value too large (max …)", `429` → "slow down, try again in a second", `507` → "playground is full, it resets at the top of every hour".

### Release pipeline (GitHub, free)

| File | Purpose |
|---|---|
| `.goreleaser.yaml` | Builds `raftra-cli` for darwin/linux/windows × amd64/arm64 and `raftra-server` for linux amd64/arm64. `CGO_ENABLED=0`, `-s -w`, version and default address via ldflags. Archive names `raftra-cli_<os>_<arch>.tar.gz` (`.zip` on Windows), plus `checksums.txt`. |
| `.github/workflows/release.yml` | On a `v*` tag push, runs GoReleaser, which publishes a GitHub Release. |
| `.github/workflows/ci.yml` | On every push and PR: `gofmt` check, `go vet`, `go test -race ./...` |
| `site/install.sh` | `curl -fsSL https://shantanu-1607.github.io/Raftra/install.sh \| sh`: detects OS and architecture, downloads `releases/latest/download/raftra-cli_<os>_<arch>.tar.gz`, verifies the checksum, and installs to `~/.local/bin`. If that directory isn't on the user's `PATH`, it prints a hint explaining how to add it. macOS and Linux only. Windows users use the download button. |

**macOS note:** the binaries are unsigned. A browser-downloaded binary gets quarantined by Gatekeeper ("developer cannot be verified"), but one installed with `curl` doesn't. The page recommends the install script on macOS and documents `xattr -d com.apple.quarantine raftra-cli` for manual downloads.

## 6. Part 4: Landing page (`site/`, GitHub Pages)

- A static site with no framework or build step: `index.html`, `styles.css`, `app.js`, `config.js`, `install.sh`.
- Deployed by `.github/workflows/pages.yml` on pushes to `main` that touch `site/**`. One-time manual setting: repository Settings → Pages → Source: GitHub Actions.
- `config.js` holds the three node URLs, so changing domains later is a one-line edit. The CLI's ldflags hold the same list.

### Sections

1. **Hero:** a one-line explanation of Raftra, the install command with a copy button, and a GitHub link.
2. **Live cluster:**
   - Three node cards (👑 leader, follower, 💀 down) showing term and commit index.
   - Countdowns to the next chaos event and the next reset, computed from the clock: chaos at `:x5`, reset at `:00`.
   - An event feed derived by comparing successive polls, e.g. "node2 elected leader (term 15)" and "node1 down", "node1 rejoined".
   - Polls each node's `GET /status` every 2 s with a 1.5 s timeout. Failures and non-200 responses (such as Caddy's `502`) show as down.
3. **Install:** buttons per OS and architecture, linking to the stable `releases/latest/download/...` URLs. The visitor's OS is highlighted.
4. **Try it:** a 30-second CLI tutorial (`status`, `set`, `get`, `delete`), including the playground rules (limits, hourly reset, no secrets).
5. **How it works:** the architecture diagram, the Raft guarantees, and the benchmark highlights (5,842 ops/s, 322 ms failover).
6. **Footer:** GitHub, README and license.

**The page never displays visitor-written keys or values.** There's nothing to moderate, and nobody can deface the page.

## 7. Error handling summary

| Situation | Server | CLI | Page |
|---|---|---|---|
| Node killed by chaos | Caddy `502` | Tries the next node | 💀 card, event logged |
| Election in progress | Follower `503` "no leader" | Retries for up to ~3 s | No 👑 shown for that moment |
| Hourly reset (`:00:00–:00:30`) | All nodes down | Error after retries: "playground is resetting" | All cards down, "resetting" banner |
| Too large, too fast, store full | `413` / `429` / `507` | Friendly message | — |

## 8. Testing

Per `AGENTS.md`, the developer runs every test command and compares the results with the expected output.

- **Go unit tests** in `internal/transport/limits_test.go` and `http_server_test.go`, using `httptest` and a single-node `RaftNode` on `storage.NewMemoryStore()` (single-node proposals commit immediately):
  - Every limit returns its status code, and requests within the limits succeed.
  - Rate limiting is per IP: the bucket refills over time (using the injected clock), and a different IP is unaffected.
  - The full store rejects new keys but allows overwrites and deletes.
  - `X-Forwarded-For` is honored only with `-trust-proxy`.
  - The CORS header appears only when configured.
  - **With all flags at their defaults, behavior is identical to today.**
- **CLI test:** with two `httptest` servers where the first returns `502`, the command succeeds through the second.
- `make test` (with `-race`), `go vet ./...`, `gofmt -l .`.
- **Scripts:** `bash -n` on all shell scripts, and `shellcheck` if available.
- **Release dry run:** `goreleaser check` and `goreleaser release --snapshot --clean` locally.
- **Local playground rehearsal:** a 3-node local cluster with playground flags. Check 413/429/507 with the CLI. Open `site/` locally against it with `-cors-origin '*'`.
- **AWS verification checklist** (in the runbook):
  - A leader is elected.
  - The CLI from the laptop can `set`/`get` through any node.
  - `nc -vz <public-ip> 50051` and `:8001` **fail**.
  - Chaos at `:x5` kills only the leader, and the node returns about 45 s later.
  - The reset at `:00` empties all nodes, and the cluster recovers by about `:00:31`.
  - The page shows all of it.

## 9. Documentation updates

- `README.md`: "Try the live playground" section and install instructions.
- `AGENTS.md`: the new flags, `deployments/lightsail/`, `site/`, the workflows, and the chaos/reset schedule.
- `deployments/lightsail/README.md`: the runbook.

## 10. Build order

1. Server limits and tests (Part 1).
2. CLI changes, GoReleaser, CI and release workflows (Part 3). Tag `v0.2.0` so the deployment has binaries to download.
3. Lightsail deployment scripts and runbook. The developer provisions AWS (Part 2).
4. Landing page and Pages workflow (Part 4).

## 11. Out of scope (possible later work)

- A custom domain.
- Migrating to EC2 (VPC, security groups, EBS, IAM).
- An interactive "kill the leader" button.
- Running commands from the browser.
- Snapshots or log compaction (the hourly reset bounds growth instead).
- Authentication or per-user namespaces.
- Code signing for macOS and Windows binaries.
