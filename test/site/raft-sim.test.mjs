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
