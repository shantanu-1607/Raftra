# Raftra Landing Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the "flight recorder" landing page in `site/` (live cluster, Raft simulator, living background, install for every platform, numbers, short build log) and deploy it with GitHub Pages.

**Architecture:** Static ES modules, no build step. Logic that can be wrong (status diffing, schedule maths, platform detection, the Raft model) lives in three pure modules under `site/js/` with `node --test` unit tests in `test/site/`. DOM modules (`live.js`, `sim-view.js`, `background.js`, `main.js`) are thin renderers over the pure modules and are verified in the browser.

**Tech Stack:** HTML, CSS (custom properties), vanilla JS ES modules, SVG, Canvas 2D, Google Fonts (Big Shoulders Display, IBM Plex Mono, IBM Plex Sans), Node's built-in test runner (`node --test`, Node ≥ 20), GitHub Actions Pages.

**Spec:** `docs/superpowers/specs/2026-10-03-landing-page-design.md`

## Global Constraints

- No framework, no npm, no `package.json`, no build step. External assets: Google Fonts only.
- All page links relative (served at `/Raftra/`).
- The page reads only `GET /status` from nodes; never displays keys or values.
- Node URLs: `https://raftra-n1.duckdns.org`, `https://raftra-n2.duckdns.org`, `https://raftra-n3.duckdns.org`; overridable with `?nodes=url1,url2,url3`.
- Repo slug `shantanu-1607/Raftra`. Release assets: `raftra-cli_<darwin|linux|windows>_<amd64|arm64>.<tar.gz|zip>` (zip on Windows) + `checksums.txt` at `https://github.com/shantanu-1607/Raftra/releases/latest/download/`.
- Schedule (UTC, server timers): chaos kill at minute ≡ 5 (mod 10), second 0; reset at `:00:00`, nodes back by `:00:30`; reset banner window `:00:00–:00:40`.
- Poll every 2000 ms, 1500 ms timeout, `cache: "no-store"`; pause while `document.hidden`.
- Colours: bg `#0c0d0e`, panel `#141618`, rule `#24282b`, text `#ebe6db`, muted `#8b9095`, amber `#ffb21a`, red `#ff4b3e`, teal `#4fd1a5`.
- Honor `prefers-reduced-motion`. No horizontal scroll at 360 px.
- Numbers on the page are exactly those in the spec §4.8 (from `benchmark_results.md` / `AGENTS.md`). Never invent figures.
- The developer commits everything; the agent never runs `git commit`/`git push` (project rule). "Commit" steps below are suggested messages for the developer.

---

### Task 1: Live-cluster logic (`cluster.js`)

**Files:**
- Create: `site/js/cluster.js`
- Test: `test/site/cluster.test.mjs`

**Interfaces:**
- Produces:
  - `parseStatus(id: string, body: object, latencyMs: number) → Reading`
  - `downReading(id: string) → Reading` where `Reading = { id, up, role: "leader"|"follower"|"candidate"|"down", term, commitIndex, lastApplied, leaderId, latencyMs }`
  - `leaderOf(readings: Reading[]) → Reading|null` (highest-term up leader)
  - `diffSnapshots(prev: Reading[]|null, next: Reading[], opts?: { inReset?: boolean }) → { kind: "info"|"up"|"down"|"leader"|"warn"|"reset"|"offline", text: string }[]`
  - `msUntilChaos(nowMs) → number`, `msUntilReset(nowMs) → number`, `inResetWindow(nowMs) → boolean`, `formatCountdown(ms) → "MM:SS"`
  - `pushTrace(trace: string[], role: string, max = 90) → string[]` (new array, oldest dropped)

- [ ] **Step 1: Write the failing tests** — `test/site/cluster.test.mjs`

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseStatus, downReading, leaderOf, diffSnapshots,
  msUntilChaos, msUntilReset, inResetWindow, formatCountdown, pushTrace,
} from "../../site/js/cluster.js";

const st = (id, role, term, commit = 0) =>
  parseStatus(id, { role, term, commit_index: commit, last_applied: commit, leader_id: "" }, 20);

test("parseStatus normalises the /status body", () => {
  const r = parseStatus("node1", { role: "Leader", term: 4, commit_index: 5, last_applied: 5, leader_id: "node1", is_leader: true }, 42);
  assert.deepEqual(r, { id: "node1", up: true, role: "leader", term: 4, commitIndex: 5, lastApplied: 5, leaderId: "node1", latencyMs: 42 });
  assert.equal(parseStatus("n", { role: "Candidate" }, 1).role, "candidate");
  assert.equal(parseStatus("n", { role: "Follower" }, 1).role, "follower");
});

test("leaderOf prefers the highest term", () => {
  assert.equal(leaderOf([st("a", "Leader", 3), st("b", "Leader", 5), downReading("c")]).id, "b");
  assert.equal(leaderOf([st("a", "Follower", 3), downReading("b")]), null);
});

test("first snapshot reports the connection", () => {
  const ev = diffSnapshots(null, [st("node1", "Leader", 4), st("node2", "Follower", 4), st("node3", "Follower", 4)]);
  assert.deepEqual(ev, [{ kind: "info", text: "connected — leader node1, term 4" }]);
  assert.equal(diffSnapshots(null, [downReading("a"), downReading("b")])[0].kind, "offline");
});

test("a dead leader and a new election produce events", () => {
  const prev = [st("node1", "Leader", 4), st("node2", "Follower", 4), st("node3", "Follower", 4)];
  const mid = [downReading("node1"), st("node2", "Follower", 4), st("node3", "Follower", 4)];
  assert.deepEqual(diffSnapshots(prev, mid), [
    { kind: "down", text: "node1 down" },
    { kind: "warn", text: "no leader — election in progress" },
  ]);
  const next = [downReading("node1"), st("node2", "Leader", 5), st("node3", "Follower", 5)];
  assert.deepEqual(diffSnapshots(mid, next), [{ kind: "leader", text: "node2 elected leader (term 5)" }]);
  const back = [st("node1", "Follower", 5), st("node2", "Leader", 5), st("node3", "Follower", 5)];
  assert.deepEqual(diffSnapshots(next, back), [{ kind: "up", text: "node1 back (follower)" }]);
});

test("all nodes going down is one event, labelled reset inside the window", () => {
  const prev = [st("a", "Leader", 2), st("b", "Follower", 2)];
  const down = [downReading("a"), downReading("b")];
  assert.deepEqual(diffSnapshots(prev, down, { inReset: true }), [{ kind: "reset", text: "playground reset — wiping data" }]);
  assert.deepEqual(diffSnapshots(prev, down, { inReset: false }), [{ kind: "down", text: "all nodes unreachable" }]);
  assert.deepEqual(diffSnapshots(down, down), []);
});

test("schedule maths is UTC-aligned", () => {
  const h = Date.UTC(2026, 9, 3, 12, 0, 0);
  assert.equal(msUntilChaos(h), 5 * 60_000);
  assert.equal(msUntilChaos(h + 5 * 60_000), 10 * 60_000);
  assert.equal(msUntilChaos(h + 5 * 60_000 + 1000), 10 * 60_000 - 1000);
  assert.equal(msUntilChaos(h + 56 * 60_000), 9 * 60_000);
  assert.equal(msUntilReset(h + 59 * 60_000 + 30_000), 30_000);
  assert.equal(inResetWindow(h + 39_000), true);
  assert.equal(inResetWindow(h + 40_000), false);
  assert.equal(formatCountdown(65_400), "01:06");
  assert.equal(formatCountdown(0), "00:00");
});

test("pushTrace keeps the newest max entries", () => {
  let t = [];
  for (let i = 0; i < 5; i++) t = pushTrace(t, String(i), 3);
  assert.deepEqual(t, ["2", "3", "4"]);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `node --test test/site/`
Expected: FAIL — `Cannot find module '.../site/js/cluster.js'`.

- [ ] **Step 3: Implement** — `site/js/cluster.js`

```js
// Pure helpers for the live cluster view: no DOM, no network.

export function parseStatus(id, body, latencyMs) {
  const role = String(body.role || "").toLowerCase();
  return {
    id,
    up: true,
    role: role === "leader" || role === "candidate" ? role : "follower",
    term: Number(body.term) || 0,
    commitIndex: Number(body.commit_index) || 0,
    lastApplied: Number(body.last_applied) || 0,
    leaderId: body.leader_id || "",
    latencyMs,
  };
}

export function downReading(id) {
  return { id, up: false, role: "down", term: null, commitIndex: null, lastApplied: null, leaderId: "", latencyMs: null };
}

// leaderOf picks the up leader with the highest term. A deposed leader can
// still claim the role for a poll or two before it hears the new term.
export function leaderOf(readings) {
  let best = null;
  for (const r of readings) {
    if (r.up && r.role === "leader" && (!best || r.term > best.term)) best = r;
  }
  return best;
}

// diffSnapshots turns two consecutive polls into human-readable events.
export function diffSnapshots(prev, next, opts = {}) {
  const leader = leaderOf(next);
  const allDown = (rs) => rs.every((r) => !r.up);
  if (!prev) {
    if (allDown(next)) return [{ kind: "offline", text: "cluster unreachable" }];
    return [{ kind: "info", text: leader ? `connected — leader ${leader.id}, term ${leader.term}` : "connected — no leader yet" }];
  }
  if (allDown(next)) {
    if (allDown(prev)) return [];
    return opts.inReset
      ? [{ kind: "reset", text: "playground reset — wiping data" }]
      : [{ kind: "down", text: "all nodes unreachable" }];
  }
  const events = [];
  const before = new Map(prev.map((r) => [r.id, r]));
  for (const n of next) {
    const p = before.get(n.id);
    if (!p) continue;
    if (p.up && !n.up) events.push({ kind: "down", text: `${n.id} down` });
    if (!p.up && n.up) events.push({ kind: "up", text: `${n.id} back (${n.role})` });
  }
  const pl = leaderOf(prev);
  if (leader && (!pl || pl.id !== leader.id || pl.term !== leader.term)) {
    events.push({ kind: "leader", text: `${leader.id} elected leader (term ${leader.term})` });
  } else if (!leader && pl) {
    events.push({ kind: "warn", text: "no leader — election in progress" });
  }
  return events;
}

const MIN = 60_000;
const untilNext = (nowMs, period, phase) => {
  const r = (((phase - nowMs) % period) + period) % period;
  return r === 0 ? period : r;
};

// The server timers run in UTC: chaos at :05, :15, … :55 and reset at :00.
export const msUntilChaos = (nowMs) => untilNext(nowMs, 10 * MIN, 5 * MIN);
export const msUntilReset = (nowMs) => untilNext(nowMs, 60 * MIN, 0);
export const inResetWindow = (nowMs) => nowMs % (60 * MIN) < 40_000;

export function formatCountdown(ms) {
  const s = Math.max(0, Math.ceil(ms / 1000));
  return `${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
}

export function pushTrace(trace, role, max = 90) {
  const t = trace.concat(role);
  return t.length > max ? t.slice(t.length - max) : t;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `node --test test/site/`
Expected: `# pass 7`, `# fail 0`.

- [ ] **Step 5: Commit (developer)** — `feat(site): add pure live-cluster logic with tests`

---

### Task 2: Platform detection (`platform.js`)

**Files:**
- Create: `site/js/platform.js`
- Test: `test/site/platform.test.mjs`

**Interfaces:**
- Produces:
  - `BUILDS: { os, arch, label, sub }[]` (6 entries, display order)
  - `assetName(os, arch) → string`, `assetURL(repo, os, arch) → string`
  - `detectPlatform({ userAgent, platform, uaPlatform, uaArch }) → { os: "darwin"|"linux"|"windows"|"mobile"|null, arch: "amd64"|"arm64"|null, guessed: boolean }`

- [ ] **Step 1: Write the failing tests** — `test/site/platform.test.mjs`

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { BUILDS, assetName, assetURL, detectPlatform } from "../../site/js/platform.js";

test("asset names match .goreleaser.yaml", () => {
  assert.equal(assetName("darwin", "arm64"), "raftra-cli_darwin_arm64.tar.gz");
  assert.equal(assetName("windows", "amd64"), "raftra-cli_windows_amd64.zip");
  assert.equal(assetURL("shantanu-1607/Raftra", "linux", "amd64"),
    "https://github.com/shantanu-1607/Raftra/releases/latest/download/raftra-cli_linux_amd64.tar.gz");
  assert.equal(BUILDS.length, 6);
});

test("detects desktop platforms", () => {
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", platform: "Win32" }),
    { os: "windows", arch: "amd64", guessed: false });
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (X11; Linux x86_64)", platform: "Linux x86_64" }),
    { os: "linux", arch: "amd64", guessed: false });
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (X11; Linux aarch64)", platform: "Linux aarch64" }),
    { os: "linux", arch: "arm64", guessed: false });
});

test("macOS: trusts client hints, otherwise guesses Apple silicon", () => {
  const ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel", uaPlatform: "macOS", uaArch: "x86" }),
    { os: "darwin", arch: "amd64", guessed: false });
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel", uaPlatform: "macOS", uaArch: "arm" }),
    { os: "darwin", arch: "arm64", guessed: false });
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel" }),
    { os: "darwin", arch: "arm64", guessed: true });
});

test("phones are not desktop builds", () => {
  assert.equal(detectPlatform({ userAgent: "Mozilla/5.0 (Linux; Android 14; Pixel 8)", platform: "Linux armv8l" }).os, "mobile");
  assert.equal(detectPlatform({ userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)", platform: "iPhone" }).os, "mobile");
});
```

- [ ] **Step 2: Run to verify it fails** — `node --test test/site/` → FAIL, module not found.

- [ ] **Step 3: Implement** — `site/js/platform.js`

```js
// Release asset names (see .goreleaser.yaml) and a best-effort guess of the visitor's platform.

export const BUILDS = [
  { os: "darwin", arch: "arm64", label: "macOS", sub: "Apple silicon" },
  { os: "darwin", arch: "amd64", label: "macOS", sub: "Intel" },
  { os: "linux", arch: "amd64", label: "Linux", sub: "x86-64" },
  { os: "linux", arch: "arm64", label: "Linux", sub: "ARM64" },
  { os: "windows", arch: "amd64", label: "Windows", sub: "x64" },
  { os: "windows", arch: "arm64", label: "Windows", sub: "ARM64" },
];

export const assetName = (os, arch) => `raftra-cli_${os}_${arch}.${os === "windows" ? "zip" : "tar.gz"}`;
export const assetURL = (repo, os, arch) => `https://github.com/${repo}/releases/latest/download/${assetName(os, arch)}`;

// detectPlatform reads navigator-style strings. uaPlatform/uaArch come from
// navigator.userAgentData (Chromium only); uaArch is "arm" or "x86".
export function detectPlatform({ userAgent = "", platform = "", uaPlatform = "", uaArch = "" } = {}) {
  const ua = userAgent.toLowerCase();
  const p = (uaPlatform || platform).toLowerCase();
  let os = null;
  if (/android|iphone|ipad|ipod/.test(ua)) os = "mobile";
  else if (p.includes("win") || ua.includes("windows")) os = "windows";
  else if (p.includes("mac") || ua.includes("mac os")) os = "darwin";
  else if (p.includes("linux") || ua.includes("linux") || ua.includes("x11")) os = "linux";

  let arch = null;
  let guessed = false;
  if (uaArch) arch = uaArch === "arm" ? "arm64" : "amd64";
  else if (os === "darwin") {
    // Every Mac browser says "Intel Mac OS X"; most Macs in use are Apple silicon.
    arch = "arm64";
    guessed = true;
  } else if (/aarch64|arm64/.test(ua) || /aarch64|arm/.test(p)) arch = "arm64";
  else if (/x86_64|x64|win64|wow64|amd64/.test(ua) || /x86_64|win32/.test(p)) arch = "amd64";
  return { os, arch, guessed };
}
```

- [ ] **Step 4: Run to verify it passes** — `node --test test/site/` → all pass.

- [ ] **Step 5: Commit (developer)** — `feat(site): add platform detection and release asset helpers`

---

### Task 3: Raft simulator model (`raft-sim.js`)

**Files:**
- Create: `site/js/raft-sim.js`
- Test: `test/site/raft-sim.test.mjs`

**Interfaces:**
- Produces:
  - `mulberry32(seed) → () => number` (seeded RNG)
  - `createSim({ n = 5, rng = Math.random }) → Sim`
  - `step(sim, dt)` — advances `dt` ticks (1 tick ≈ 1 ms at 1×), in 1-tick substeps
  - `toggleNode(sim, id)`, `setPartition(sim, on: boolean)`, `clientWrite(sim) → number` (leaders that accepted)
  - `Sim = { now, nodes: Node[], messages: Msg[], partition: [Set, Set]|null, events: {at, kind, text}[], leaderHistory: {term, id}[] }`
  - `Node = { id, alive, role, term, votedFor, log: {term, value}[], commitIndex, votes: Set, nextIndex, matchIndex, electionDeadline, heartbeatDue }`
  - `Msg = { id, from, to, type: "RV"|"RVR"|"AE"|"AER", sentAt, arriveAt, term, ... }`
  - `inSameSide(sim, a, b) → boolean`

- [ ] **Step 1: Write the failing tests** — `test/site/raft-sim.test.mjs`

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { createSim, step, toggleNode, setPartition, clientWrite, mulberry32 } from "../../site/js/raft-sim.js";

const leaders = (sim) => sim.nodes.filter((n) => n.alive && n.role === "leader");
const run = (sim, ticks) => step(sim, ticks);
function runUntil(sim, pred, max = 5000) {
  for (let t = 0; t < max; t += 10) { step(sim, 10); if (pred()) return true; }
  return false;
}

test("elects exactly one leader", () => {
  const sim = createSim({ rng: mulberry32(1) });
  assert.ok(runUntil(sim, () => leaders(sim).length === 1));
  run(sim, 500);
  assert.equal(leaders(sim).length, 1);
});

test("a client write commits on every node", () => {
  const sim = createSim({ rng: mulberry32(2) });
  runUntil(sim, () => leaders(sim).length === 1);
  assert.equal(clientWrite(sim), 1);
  assert.ok(runUntil(sim, () => sim.nodes.every((n) => n.commitIndex === 1)));
  assert.ok(sim.nodes.every((n) => n.log.length === 1));
});

test("killing the leader elects a new one in a higher term", () => {
  const sim = createSim({ rng: mulberry32(3) });
  runUntil(sim, () => leaders(sim).length === 1);
  const old = leaders(sim)[0];
  toggleNode(sim, old.id);
  assert.ok(runUntil(sim, () => leaders(sim).length === 1));
  assert.ok(leaders(sim)[0].term > old.term);
  toggleNode(sim, old.id);
  run(sim, 1000);
  assert.equal(old.role, "follower");
});

test("minority side cannot commit; logs converge after healing", () => {
  const sim = createSim({ rng: mulberry32(4) });
  runUntil(sim, () => leaders(sim).length === 1);
  setPartition(sim, true); // {S1,S2} | {S3,S4,S5}
  run(sim, 2000);
  clientWrite(sim);
  run(sim, 1000);
  const minority = sim.nodes.filter((n) => n.id === "S1" || n.id === "S2");
  const majority = sim.nodes.filter((n) => !minority.includes(n));
  assert.ok(minority.every((n) => n.commitIndex === 0));
  assert.ok(majority.every((n) => n.commitIndex >= 1));
  setPartition(sim, false);
  assert.ok(runUntil(sim, () => {
    const c = sim.nodes.map((n) => n.commitIndex);
    return c.every((x) => x === c[0]) && sim.nodes.every((n) => n.log.length === sim.nodes[0].log.length);
  }));
  const terms = (n) => n.log.map((e) => e.term).join(",");
  assert.ok(sim.nodes.every((n) => terms(n) === terms(sim.nodes[0])));
});

test("election safety holds under random chaos", () => {
  const rng = mulberry32(5);
  const sim = createSim({ rng });
  for (let i = 0; i < 200; i++) {
    run(sim, 100);
    const r = rng();
    if (r < 0.15) toggleNode(sim, sim.nodes[Math.floor(rng() * 5)].id);
    else if (r < 0.2) setPartition(sim, !sim.partition);
    else if (r < 0.5) clientWrite(sim);
  }
  const byTerm = new Map();
  for (const { term, id } of sim.leaderHistory) {
    assert.ok(!byTerm.has(term) || byTerm.get(term) === id, `two leaders in term ${term}`);
    byTerm.set(term, id);
  }
  // Log matching on committed prefixes.
  const minCommit = Math.min(...sim.nodes.map((n) => n.commitIndex));
  for (let i = 0; i < minCommit; i++) {
    const t = sim.nodes[0].log[i].term;
    assert.ok(sim.nodes.every((n) => n.log[i].term === t));
  }
});
```

- [ ] **Step 2: Run to verify it fails** — `node --test test/site/` → FAIL, module not found.

- [ ] **Step 3: Implement** — `site/js/raft-sim.js`

```js
// A small textbook Raft model for the in-browser simulator. Pure: no DOM, no timers.
// Time is in ticks (1 tick ≈ 1 ms at 1×). No pre-vote, no persistence layer:
// a killed node keeps its term, vote and log, as if they were on disk.

const CFG = { electionMin: 150, electionMax: 300, heartbeat: 50, latencyMin: 10, latencyMax: 25, maxEntries: 8 };

export function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function createSim({ n = 5, rng = Math.random } = {}) {
  const sim = { now: 0, rng, nodes: [], messages: [], partition: null, events: [], leaderHistory: [], nextMsg: 1, nextValue: 1 };
  for (let i = 1; i <= n; i++) {
    sim.nodes.push({
      id: `S${i}`, alive: true, role: "follower", term: 0, votedFor: null, log: [], commitIndex: 0,
      votes: new Set(), nextIndex: {}, matchIndex: {}, electionDeadline: 0, heartbeatDue: 0,
    });
  }
  for (const n of sim.nodes) resetElection(sim, n);
  return sim;
}

const byId = (sim, id) => sim.nodes.find((n) => n.id === id);
const majority = (sim) => Math.floor(sim.nodes.length / 2) + 1;
const lastTerm = (n) => (n.log.length ? n.log[n.log.length - 1].term : 0);
const between = (sim, lo, hi) => lo + sim.rng() * (hi - lo);

function log(sim, kind, text) {
  sim.events.push({ at: sim.now, kind, text });
  if (sim.events.length > 50) sim.events.shift();
}

function resetElection(sim, n) {
  n.electionDeadline = sim.now + between(sim, CFG.electionMin, CFG.electionMax);
}

export function inSameSide(sim, a, b) {
  if (!sim.partition) return true;
  return sim.partition.some((side) => side.has(a) && side.has(b));
}

function send(sim, from, to, type, payload) {
  sim.messages.push({
    id: sim.nextMsg++, from, to, type, sentAt: sim.now,
    arriveAt: sim.now + between(sim, CFG.latencyMin, CFG.latencyMax), ...payload,
  });
}

function becomeFollower(sim, n, term) {
  if (term > n.term) { n.term = term; n.votedFor = null; }
  if (n.role !== "follower") n.role = "follower";
  n.votes = new Set();
}

function startElection(sim, n) {
  n.term += 1;
  n.role = "candidate";
  n.votedFor = n.id;
  n.votes = new Set([n.id]);
  resetElection(sim, n);
  log(sim, "election", `${n.id} timed out → candidate, term ${n.term}`);
  for (const p of sim.nodes) {
    if (p.id !== n.id) send(sim, n.id, p.id, "RV", { term: n.term, lastLogIndex: n.log.length, lastLogTerm: lastTerm(n) });
  }
}

function becomeLeader(sim, n) {
  n.role = "leader";
  sim.leaderHistory.push({ term: n.term, id: n.id });
  log(sim, "leader", `${n.id} won term ${n.term} with ${n.votes.size} votes`);
  for (const p of sim.nodes) { n.nextIndex[p.id] = n.log.length + 1; n.matchIndex[p.id] = 0; }
  n.heartbeatDue = sim.now;
}

function sendAppend(sim, leader, peerId) {
  const prevLogIndex = leader.nextIndex[peerId] - 1;
  send(sim, leader.id, peerId, "AE", {
    term: leader.term,
    prevLogIndex,
    prevLogTerm: prevLogIndex > 0 ? leader.log[prevLogIndex - 1].term : 0,
    entries: leader.log.slice(prevLogIndex, prevLogIndex + CFG.maxEntries).map((e) => ({ ...e })),
    leaderCommit: leader.commitIndex,
  });
}

function advanceCommit(sim, leader) {
  for (let i = leader.log.length; i > leader.commitIndex; i--) {
    if (leader.log[i - 1].term !== leader.term) break; // §5.4.2: count current-term entries only
    const count = 1 + sim.nodes.filter((p) => p.id !== leader.id && leader.matchIndex[p.id] >= i).length;
    if (count >= majority(sim)) {
      leader.commitIndex = i;
      log(sim, "commit", `${leader.id} committed entry ${i} (${count}/${sim.nodes.length} have it)`);
      return;
    }
  }
}

function deliver(sim, m) {
  const n = byId(sim, m.to);
  if (m.term > n.term) becomeFollower(sim, n, m.term);
  switch (m.type) {
    case "RV": {
      const upToDate = m.lastLogTerm > lastTerm(n) || (m.lastLogTerm === lastTerm(n) && m.lastLogIndex >= n.log.length);
      const granted = m.term === n.term && (n.votedFor === null || n.votedFor === m.from) && upToDate;
      if (granted) { n.votedFor = m.from; resetElection(sim, n); }
      send(sim, n.id, m.from, "RVR", { term: n.term, granted });
      break;
    }
    case "RVR":
      if (n.role === "candidate" && m.term === n.term && m.granted) {
        n.votes.add(m.from);
        if (n.votes.size >= majority(sim)) becomeLeader(sim, n);
      }
      break;
    case "AE": {
      if (m.term < n.term) { send(sim, n.id, m.from, "AER", { term: n.term, success: false, matchIndex: 0 }); break; }
      if (n.role !== "follower") becomeFollower(sim, n, m.term);
      resetElection(sim, n);
      const ok = m.prevLogIndex === 0 || (m.prevLogIndex <= n.log.length && n.log[m.prevLogIndex - 1].term === m.prevLogTerm);
      if (!ok) { send(sim, n.id, m.from, "AER", { term: n.term, success: false, matchIndex: 0 }); break; }
      m.entries.forEach((e, k) => {
        const idx = m.prevLogIndex + 1 + k;
        if (n.log[idx - 1] && n.log[idx - 1].term !== e.term) n.log.length = idx - 1; // drop the divergent suffix
        if (!n.log[idx - 1]) n.log.push(e);
      });
      const matchIndex = m.prevLogIndex + m.entries.length;
      n.commitIndex = Math.max(n.commitIndex, Math.min(m.leaderCommit, matchIndex));
      send(sim, n.id, m.from, "AER", { term: n.term, success: true, matchIndex });
      break;
    }
    case "AER":
      if (n.role !== "leader" || m.term !== n.term) break;
      if (m.success) {
        n.matchIndex[m.from] = Math.max(n.matchIndex[m.from], m.matchIndex);
        n.nextIndex[m.from] = n.matchIndex[m.from] + 1;
        advanceCommit(sim, n);
      } else {
        n.nextIndex[m.from] = Math.max(1, n.nextIndex[m.from] - 1);
        sendAppend(sim, n, m.from);
      }
      break;
  }
}

function tick(sim) {
  const due = sim.messages.filter((m) => m.arriveAt <= sim.now).sort((a, b) => a.arriveAt - b.arriveAt || a.id - b.id);
  if (due.length) {
    sim.messages = sim.messages.filter((m) => m.arriveAt > sim.now);
    for (const m of due) {
      if (byId(sim, m.to).alive && inSameSide(sim, m.from, m.to)) deliver(sim, m);
    }
  }
  for (const n of sim.nodes) {
    if (!n.alive) continue;
    if (n.role === "leader") {
      if (sim.now >= n.heartbeatDue) {
        n.heartbeatDue = sim.now + CFG.heartbeat;
        for (const p of sim.nodes) if (p.id !== n.id) sendAppend(sim, n, p.id);
      }
    } else if (sim.now >= n.electionDeadline) {
      startElection(sim, n);
    }
  }
}

export function step(sim, dt) {
  const end = sim.now + dt;
  while (sim.now < end) { sim.now += 1; tick(sim); }
}

export function toggleNode(sim, id) {
  const n = byId(sim, id);
  n.alive = !n.alive;
  if (n.alive) {
    n.role = "follower";
    n.votes = new Set();
    resetElection(sim, n);
    log(sim, "up", `${id} restarted as follower (term ${n.term})`);
  } else {
    log(sim, "down", `${id} crashed${n.role === "leader" ? " — it was the leader" : ""}`);
  }
}

export function setPartition(sim, on) {
  const ids = sim.nodes.map((n) => n.id);
  sim.partition = on ? [new Set(ids.slice(0, 2)), new Set(ids.slice(2))] : null;
  log(sim, on ? "partition" : "heal", on ? `network split: ${ids.slice(0, 2).join(",")} | ${ids.slice(2).join(",")}` : "network healed");
}

// clientWrite hands a new entry to every node that believes it is leader, the
// way confused clients would. A stale leader in a minority accepts it but can
// never commit it.
export function clientWrite(sim) {
  const ls = sim.nodes.filter((n) => n.alive && n.role === "leader");
  const value = sim.nextValue++;
  for (const l of ls) {
    l.log.push({ term: l.term, value });
    log(sim, "write", `client write #${value} → ${l.id} (entry ${l.log.length}, term ${l.term})`);
  }
  if (!ls.length) log(sim, "warn", `client write #${value} rejected: no leader`);
  return ls.length;
}
```

- [ ] **Step 4: Run to verify it passes** — `node --test test/site/` → all pass (3 files). If the partition test is flaky for a seed, it's a model bug — fix the model, never the seed.

- [ ] **Step 5: Commit (developer)** — `feat(site): add Raft simulator model with tests`

---

### Task 4: Page shell, styles and static content

**Files:**
- Create: `site/index.html`, `site/styles.css`, `site/config.js`, `site/js/main.js` (copy buttons + install section only in this task)

**Interfaces:**
- Consumes: `BUILDS`, `assetURL`, `detectPlatform` (Task 2).
- Produces (DOM contract for Tasks 5–7):
  - `config.js`: `export const CONFIG = { repo, nodes: [{ id, url }], pollMs: 2000, timeoutMs: 1500 }`; `nodes` overridable by `?nodes=` (comma-separated URLs → ids `node1..nodeN`).
  - Element IDs: `#bg` (canvas), `#pill`, `#hero-chaos`, `#live-nodes` (3 × `.node[data-id]` each with `[data-f=role|term|commit|applied|latency]` and `.trace`), `#cd-chaos`, `#cd-reset`, `#reset-banner`, `#offline-note`, `#feed`, `#sim-svg`, `#sim-caption`, `#sim-controls` (buttons `[data-act=partition|write|reset]`, `[data-speed]`), `#install-primary`, `#install-grid`, `[data-copy]` buttons copying the text of `[data-copy-src]` sibling `code`.

- [ ] **Step 1:** Write `config.js`, `index.html` (all 10 sections from spec §4 with final copy, `<noscript>` note in live section, `<script type="module" src="js/main.js">`), and `styles.css` (tokens, type scale, scanlines, grid panels, responsive nav, `prefers-reduced-motion`).
- [ ] **Step 2:** `main.js`: copy buttons (Clipboard API, "COPIED" for 1.5 s, fallback select text), render `#install-grid` from `BUILDS` and highlight `detectPlatform(...)` (await `navigator.userAgentData?.getHighEntropyValues(["architecture"])` when available); primary block shows `curl | sh` for macOS/Linux, `.zip` link for Windows, "on your computer" note for mobile.
- [ ] **Step 3: Verify** — `python3 -m http.server 8080 -d site`, open `http://localhost:8080`: all sections render, no console errors, install highlight matches your Mac (arm64), copy works, 375 px width has no horizontal scroll.
- [ ] **Step 4: Commit (developer)** — `feat(site): landing page shell, styles and install section`

---

### Task 5: Live cluster (`live.js`)

**Files:**
- Create: `site/js/live.js`; Modify: `site/js/main.js`

**Interfaces:**
- Consumes: Task 1 functions; `CONFIG`; DOM contract from Task 4.
- Produces: `startLive({ onSnapshot: (readings) => void })`.

- [ ] **Step 1:** Poll loop: every `pollMs`, `Promise.all` over nodes with `fetch(url + "/status", { cache: "no-store", signal: AbortSignal.timeout(timeoutMs) })`, latency via `performance.now()`; non-200 or throw → `downReading`. Skip while `document.hidden`; poll immediately on `visibilitychange` to visible.
- [ ] **Step 2:** Render: node readouts (role class `is-leader|is-follower|is-candidate|is-down`), trace strips (`pushTrace`, 90 bars), pill (`LIVE · LEADER x · TERM n` / `NO LEADER` / `RESETTING` / `OFFLINE`), events from `diffSnapshots(prev, next, { inReset })` prepended to `#feed` with `HH:MM:SS` local time (max 40), `#reset-banner` when `inResetWindow(Date.now())` and all down, `#offline-note` when all down outside the window. Countdowns update every 250 ms via `msUntilChaos`/`msUntilReset`/`formatCountdown` (also `#hero-chaos`).
- [ ] **Step 3:** `main.js` calls `startLive({ onSnapshot })`.
- [ ] **Step 4: Verify** — local 3-node cluster with `-cors-origin '*'` (commands given to the developer), open `http://localhost:8080/?nodes=http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`; Ctrl-C the leader → card DOWN within ~2 s, `nodeX down`, new leader event; restart → `back (follower)`. Without `?nodes`, the page shows OFFLINE (CORS) — expected locally.
- [ ] **Step 5: Commit (developer)** — `feat(site): live cluster recorder`

---

### Task 6: Simulator view (`sim-view.js`)

**Files:**
- Create: `site/js/sim-view.js`; Modify: `site/js/main.js`

**Interfaces:**
- Consumes: Task 3 API; `#sim-svg`, `#sim-caption`, `#sim-controls`.
- Produces: `startSimView(root: HTMLElement)`.

- [ ] **Step 1:** `requestAnimationFrame` loop calls `step(sim, min(50, frameMs) * speed)`; runs only while the section intersects the viewport. Speed buttons 0.25/1/2 (default 0.25 under reduced motion, else 1). Ticks are slowed further by a fixed visual factor (×0.25 sim ticks per real ms) so a 150–300 tick election timeout is visible — the caption says "slowed down".
- [ ] **Step 2:** SVG: 5 nodes on a ring; each node = outer timeout arc (shrinks to election deadline for followers/candidates), role ring colour (amber leader, muted follower, grey-dashed candidate, red dead with ✕), label `S1`, `T3`; log row of up to 12 blocks (term colours, solid if `index ≤ commitIndex`, outlined otherwise). Messages: dots interpolated from `sentAt→arriveAt` (RV amber-outline, RVR small, AE teal, AER small teal); dropped messages (dead/partitioned receiver) fade red at the end. Partition shown as a dashed red line across the ring.
- [ ] **Step 3:** Interactions: click/tap node → `toggleNode`; buttons → `setPartition(!sim.partition)`, `clientWrite` (shake the button if it returns 0), Reset → new `createSim()`. Keyboard: nodes are `<g tabindex="0" role="button">`, Enter/Space toggles. Caption shows the latest `sim.events` text.
- [ ] **Step 4: Verify** — leader emerges within a few seconds; kill it → new leader, higher term; partition + write → majority commits, minority leader's entry stays outlined; heal → logs converge.
- [ ] **Step 5: Commit (developer)** — `feat(site): interactive Raft simulator`

---

### Task 7: Living background (`background.js`)

**Files:**
- Create: `site/js/background.js`; Modify: `site/js/main.js`

**Interfaces:**
- Produces: `startBackground(canvas) → { update(readings) }`.

- [ ] **Step 1:** Fixed full-viewport canvas, DPR-aware, resize-aware, opacity via CSS (~0.16). Three columns at 20 % / 50 % / 80 % width. Each column holds blocks `{ y, born }` drifting up 6 px/s; seeded with a few grey blocks.
- [ ] **Step 2:** `update(readings)`: commit index growth → add `min(8, delta)` blocks that flash teal → settle; every up reading → a pulse travels down the column; leader column amber tint; down → dark red, no drift; term increase → full-width hairline fading over 1.5 s.
- [ ] **Step 3:** Reduced motion → draw a single frame per update, no rAF loop. Pause rAF while `document.hidden`.
- [ ] **Step 4: Verify** — with the local cluster, `raftra-cli set` a few keys → blocks appear in all three columns; kill a node → its column turns red.
- [ ] **Step 5: Commit (developer)** — `feat(site): living log background`

---

### Task 8: Pages workflow and docs

**Files:**
- Create: `.github/workflows/pages.yml`
- Modify: `README.md` (playground section: link to `https://shantanu-1607.github.io/Raftra/`), `AGENTS.md` (Repository Structure: `site/` files, `test/site/`, `pages.yml`; Development Workflow: `node --test test/site/`)

- [ ] **Step 1:** Write `pages.yml`:

```yaml
name: Pages

on:
  push:
    branches: [main]
    paths: ["site/**", "test/site/**", ".github/workflows/pages.yml"]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: true

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
      - name: Site unit tests
        run: node --test test/site/
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v3
        with:
          path: site
      - id: deployment
        uses: actions/deploy-pages@v4
```

- [ ] **Step 2:** Docs edits as listed.
- [ ] **Step 3: Verify** — developer: Settings → Pages → Source: GitHub Actions; push; the Pages run is green; the URL shows the live cluster (CORS origin matches).
- [ ] **Step 4: Commit (developer)** — `ci: deploy site to GitHub Pages` + `docs: link the landing page`

---

### Task 9: Final pass

- [ ] Browser screenshots at 1440 px and 375 px; fix spacing/overflow.
- [ ] `node --test test/site/` all green; no console errors; Lighthouse accessibility sanity (contrast of muted text on graphite ≥ 4.5:1).
- [ ] Re-read every number on the page against `benchmark_results.md`.
