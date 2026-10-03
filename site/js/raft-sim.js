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
