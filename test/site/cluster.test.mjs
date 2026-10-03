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
