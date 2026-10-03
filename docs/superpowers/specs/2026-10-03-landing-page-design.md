# Raftra Landing Page — Design

**Date:** 2026-10-03 · **Status:** approved · **Part 4** of `2026-10-03-public-playground-design.md` (this spec refines its §6).

**Goal:** A landing page at `https://shantanu-1607.github.io/Raftra/` that sells Raftra by showing it survive: a live view of the real 3-node playground cluster (the leader is killed every 10 minutes), an in-browser Raft simulator visitors can break, CLI downloads for every platform, honest numbers, and a short build story.

Inspiration: an engineering-first project page (sticky nav, interactive demo, honest benchmarks, expandable phases). Nothing is copied; the structure, visuals and copy are Raftra's own.

---

## 1. Constraints

- Static files only, no framework, no build step, no npm. Plain ES modules.
- External assets: Google Fonts only. Everything else inline or in `site/`.
- Served by GitHub Pages from `site/`; project-page base path is `/Raftra/`, so all links are relative.
- Besides the GitHub API (star count and latest version, cached per session), the page reads **only** `GET /status` from the nodes (CORS is allowed for `https://shantanu-1607.github.io`). It never reads or displays keys or values.
- Works at phone width (≥ 360 px), no horizontal scroll. Honors `prefers-reduced-motion` (background and simulator animations pause / step instead of animate).
- Dark-only theme (the "flight recorder" look is the point).

## 2. Files

| File | Responsibility |
|---|---|
| `site/index.html` | Markup for all sections. |
| `site/styles.css` | Design tokens and all styles. |
| `site/config.js` | `export` of the 3 node URLs, repo slug, poll interval/timeout, chaos/reset schedule. Changing domains is a one-line edit. |
| `site/js/cluster.js` | **Pure** live-cluster logic: parse `/status`, pick the leader, diff two polls into events, chaos/reset schedule maths. |
| `site/js/platform.js` | **Pure** OS/arch detection and release asset names/URLs. |
| `site/js/raft-sim.js` | **Pure** Raft simulator model (`createSim`, `step`, `toggleNode`, `setPartition`, `clientWrite`). |
| `site/js/live.js` | DOM: polling loop and rendering of the live section, status pill, countdowns, event feed, trace strips. |
| `site/js/sim-view.js` | DOM: SVG renderer and controls for the simulator. |
| `site/js/background.js` | Living background canvas. Consumes snapshots; owns no network code. |
| `site/js/main.js` | Entry point: wires the modules together, copy buttons, install highlighting. |
| `site/install.sh` | Existing installer, unchanged. |
| `test/site/*.test.mjs` | `node --test` unit tests for the three pure modules (no npm, no dependencies). |
| `.github/workflows/pages.yml` | Deploys `site/` to GitHub Pages on pushes to `main` touching `site/**` (and `workflow_dispatch`). |

`live.js` → `background.js` interface: `live.js` calls `onSnapshot(readings)` (a callback passed by `main.js`), and `main.js` forwards it to `background.update(readings)`. `readings` is an array of `{ id, up, role, term, commitIndex, lastApplied, leaderId, latencyMs }`. `background.js` exports `startBackground(canvas)` returning `{ update(readings) }`.

## 3. Visual system — "flight recorder"

- **Colours (CSS custom properties on `:root`):** graphite background `#0c0d0e`, panel `#141618`, rule lines `#24282b`, text warm off-white `#ebe6db`, muted `#8b9095`, **amber `#ffb21a`** (leader, live signal, primary accent), **red `#ff4b3e`** (kill, down), teal `#4fd1a5` (committed / healthy). Term colours for log blocks: a fixed cycle of 6 muted hues.
- **Type:** *Big Shoulders Display* (condensed, 700–900) for headlines and big numbers; *IBM Plex Mono* for data, labels, code; *IBM Plex Sans* for body copy.
- **Texture:** subtle scanline overlay, faint 24 px grid on panels, instrument-style labels (`CH-01 · NODE1`), thin amber hairlines. Panels look like readouts, not cards.

## 4. Sections (in order)

1. **Top bar (sticky):** `RAFTRA` wordmark; live status pill (`● LIVE · LEADER node1 · TERM 4`, or `NO LEADER` / `RESETTING`); nav: Live · Simulator · Install · Try · Internals · Numbers · Build log · GitHub. Nav collapses to a scrollable row on phones.
2. **Hero:** headline "We kill the leader every ten minutes. Your writes survive." Sub: Raftra is a fault-tolerant key-value store built on a from-scratch Raft implementation in Go. Install one-liner with copy button (`curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh | sh`), buttons "Watch it live" and "GitHub". A compact readout of the next kill countdown.
3. **Live cluster (`#live`):**
   - Three node readouts: role (LEADER / FOLLOWER / CANDIDATE / DOWN), term, commit index, last applied, response time. Leader gets the amber treatment; down gets red.
   - **Trace strip** per node: last 90 polls (3 min) as thin bars — amber = leader, teal = follower, grey = candidate, red = down.
   - Countdowns (shown once, in the hero): next chaos kill at `:x5:00`, next reset at `:00:00` (computed from the visitor's clock, UTC-aligned minutes). During `:00:00–:00:40` a "RESETTING — playground wipe in progress" banner shows.
   - **Event feed** (newest first, max 40, timestamps `HH:MM:SS`): derived by diffing successive polls — `node1 down`, `node1 back (follower)`, `node2 elected leader (term 15)`, `term 14 → 15`, `cluster reset`, `no leader`. First poll logs `connected — leader nodeX, term N`.
   - Polling: every 2 s, each node independently, `fetch` with 1.5 s `AbortController` timeout, `cache: "no-store"`. Network error, timeout or non-200 (e.g. Caddy `502`) = down. Polling pauses while the tab is hidden.
4. **Simulator (`#simulator`):** "Now break one yourself."
   - 5 nodes on a ring (SVG). Messages (RequestVote, votes, AppendEntries, acks) are dots travelling between nodes.
   - Model: simplified Raft — randomized election timeouts (sim-time 150–300 units), heartbeats every 50 units, RequestVote with up-to-date log check, AppendEntries with prevLogIndex/term check and nextIndex backoff, majority commit of current-term entries, message latency 10–25 units. No pre-vote (keeps it textbook; noted in UI copy).
   - Controls: click a node = kill / revive; **Partition** button toggles {n1,n2} | {n3,n4,n5}; **Client write** appends to the leader (shakes if none); speed: 0.25× / 1× / 2×; **Reset**.
   - Each node shows role ring, term, and its log as a row of blocks coloured by term; committed blocks are solid, uncommitted outlined.
   - Caption line narrates the latest notable event ("S3 timed out → candidate, term 4", "S1 won with 3 votes").
   - Simulation runs only while the section is on screen (`IntersectionObserver`). Reduced motion: speed defaults to 0.25× and dots don't trail.
5. **Install (`#install`):** detects OS/arch from `navigator.userAgentData` (fallback `navigator.userAgent`/`platform`) and highlights the matching build. Primary: `curl | sh` for macOS/Linux, `.zip` for Windows. Grid of all 6 builds linking to `https://github.com/shantanu-1607/Raftra/releases/latest/download/raftra-cli_<os>_<arch>.<tar.gz|zip>` (`darwin|linux|windows` × `amd64|arm64`), plus `checksums.txt`. macOS note: browser downloads are quarantined → use the script, or `xattr -d com.apple.quarantine raftra-cli`. Windows note: unzip and run `raftra-cli.exe`.
6. **Try it (`#try`):** a terminal transcript of `raftra-cli status`, `set city mumbai`, `get city`, `delete city`, plus the interactive REPL mention. Playground rules: key ≤ 128 bytes, value ≤ 1 KiB, 10,000 keys max, 5 writes/s per IP (burst 10), wiped every hour at `:00`, the leader is killed at `:x5`, don't store secrets.
7. **Internals (`#internals`):** inline SVG architecture diagram (client → HTTPS/Caddy → HTTP gateway on any node → 307 to leader → leader log → `AppendEntries` over private-IP gRPC → followers → bbolt; commit → KV map). Guarantees list (election safety, log matching, leader completeness, linearizable writes, majority survival, pre-vote, no-op on election). "Not implemented" list (snapshots/compaction, dynamic membership, linearizable reads — follower GETs can be stale, batching/group commit, TLS between nodes).
8. **Numbers (`#numbers`):** big-number tiles then tables, all from `AGENTS.md`/`benchmark_results.md`:
   - Load generator (3 nodes, one MacBook, 100 workers, 80% SET): `-nosync` 5,842 ops/s, SET p50 20.84 ms / p99 40.23 ms, 0 errors over 1M ops; durable 93.7 ops/s, SET p50 1,323.76 ms — and why (one fsynced bbolt transaction per proposal under the node mutex; macOS `F_FULLFSYNC`; group commit is the fix, not done yet).
   - Microbenchmarks (Apple M2, `-count=3`): Set 14.83 ms/op, Get 29.36 ns/op (0 allocs), Mixed 2.69 ms/op, replication 15.58 ms per quorum commit.
   - Failover: 245 ms average, 243 ms median, 205–291 ms over 20 trials (target < 2 s).
   - Tests: 71/71 tests pass with `-race`; chaos scenarios 80/80 runs (8 scenarios × `-count=10`), from `README.md`.
9. **Build log (`#build`):** 8 phases, one line each (foundation → election → replication → persistence → chaos → deployment & CLI → benchmarks & metrics → docs), plus a "Then it went public" line (limits, Lightsail, chaos timer). Two expandable incident reports (`<details>`): (a) a restarted node with a higher term kept deposing healthy leaders → **pre-vote**; (b) data committed in an old term stayed invisible after failover until the next write → **no-op entry on election**.
10. **Footer:** GitHub, README, MIT license, "built by Shantanu Singh". Easter egg line: "The three columns behind this page aren't decoration — they're the live nodes' logs."

## 5. Living background (`bg.js`)

- Fixed full-viewport `<canvas>` behind content at low opacity (~0.12–0.18), DPR-aware, resized on window resize.
- Three vertical columns (one per real node), each a stack of small log blocks drifting slowly upward.
- On a snapshot: if a node's `commitIndex` grew, add that many blocks (capped at 8 per update) that flash teal and settle; every successful poll sends a faint pulse down the column; leader column tinted amber; down node column dims to dark red and stops drifting; term change briefly draws a horizontal hairline across all columns.
- Before the first snapshot, columns idle in grey. `prefers-reduced-motion`: draws a static frame per snapshot, no drift. Pauses when the tab is hidden.

## 6. Error handling

| Situation | Page behaviour |
|---|---|
| A node is down / Caddy 502 / timeout | Card → DOWN (red), trace red, background column dims, event logged once on transition. |
| All nodes down at `:00:00–:00:40` | "RESETTING" banner and pill; event `cluster reset`. |
| All nodes down outside reset window | Pill `OFFLINE`; live section note "Can't reach the cluster right now"; simulator and the rest work. |
| Election in progress (no node reports leader) | Pill `NO LEADER`; event `no leader`. |
| Two nodes claim leader (stale view across polls) | Show the one with the higher term as leader. |
| JS disabled | Static content, install links and commands still work; live section shows a short noscript note. |

## 7. Pages workflow

`.github/workflows/pages.yml`: triggers `push` to `main` with `paths: ["site/**", ".github/workflows/pages.yml"]` and `workflow_dispatch`; permissions `contents: read, pages: write, id-token: write`; concurrency group `pages`; jobs: `actions/checkout@v4` → `actions/configure-pages@v5` → `actions/upload-pages-artifact@v3` (path `site`) → `actions/deploy-pages@v4`. One-time manual step: repository Settings → Pages → Source: **GitHub Actions**.

## 8. Testing (the developer runs these)

- Unit tests: `node --test "test/site/*.test.mjs"` (pure modules only).
- Local serve: `python3 -m http.server 8080 -d site`, open `http://localhost:8080`.
- Against the live cluster: CORS only allows `https://shantanu-1607.github.io`, so locally the live section shows OFFLINE unless pointed at a local cluster. For local live testing: run a 3-node local cluster with `-cors-origin '*'` and override the node list via `?nodes=http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003` (supported by `config.js`).
- Kill a local node → card goes DOWN within ~2 s, event logged, background column dims; restart → "back", new leader shown.
- Simulator: kill the leader → new leader within a few sim-seconds; partition → minority can't commit (writes stay outlined), heal → logs converge.
- Phone width (DevTools 375 px): no horizontal scroll; nav scrolls.
- After deploy: open the Pages URL, confirm the live section shows the real cluster and survives a `:x5` kill.

## 9. Docs

- `README.md`: link to the landing page in the playground section.
- `AGENTS.md`: `site/` file list and `pages.yml` in Repository Structure.

## 10. Out of scope

Custom domain, analytics, running commands from the browser, a "kill the real leader" button, light theme.
