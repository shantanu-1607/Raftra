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
