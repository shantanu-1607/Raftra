// The live recorder: polls each node's /status and renders the tape, pill, countdowns and feed.
import { CONFIG } from "../config.js";
import {
  parseStatus, downReading, leaderOf, diffSnapshots,
  msUntilChaos, msUntilReset, inResetWindow, formatCountdown, pushTrace,
} from "./cluster.js";

const TRACE_LEN = 90; // 3 minutes at one poll every 2 s
const MAX_FEED = 40;
const ROLE_LABEL = { leader: "Leader", follower: "Follower", candidate: "Candidate", down: "Dead", stale: "Stepping down" };

async function pollNode({ id, url }) {
  const t0 = performance.now();
  try {
    const res = await fetch(`${url}/status`, { cache: "no-store", signal: AbortSignal.timeout(CONFIG.timeoutMs) });
    if (!res.ok) return downReading(id);
    const body = await res.json();
    return parseStatus(id, body, Math.round(performance.now() - t0));
  } catch {
    return downReading(id);
  }
}

function buildTrack(el, node) {
  el.innerHTML = `
    <div class="readout">
      <div class="readout-top"><span class="name">${node.id}</span><span class="lat" data-f="latency"></span></div>
      <div class="role" data-f="role">Waiting</div>
      <dl>
        <div><dt>term</dt><dd data-f="term">–</dd></div>
        <div><dt>commit</dt><dd data-f="commit">–</dd></div>
        <div><dt>applied</dt><dd data-f="applied">–</dd></div>
      </dl>
    </div>
    <div class="trace" aria-hidden="true">${"<i></i>".repeat(TRACE_LEN)}</div>`;
  const f = (name) => el.querySelector(`[data-f="${name}"]`);
  return {
    el, trace: [],
    role: f("role"), term: f("term"), commit: f("commit"), applied: f("applied"), latency: f("latency"),
    bars: [...el.querySelectorAll(".trace i")],
  };
}

const clock = (d) => d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });

export function startLive({ onSnapshot = () => {} } = {}) {
  const $ = (id) => document.getElementById(id);
  const tape = $("live-nodes");
  const axis = tape.querySelector(".axis");
  const tracks = new Map();
  for (const node of CONFIG.nodes) {
    let el = tape.querySelector(`.track[data-id="${node.id}"]`);
    if (!el) {
      el = document.createElement("div");
      el.className = "track";
      el.dataset.id = node.id;
      tape.insertBefore(el, axis);
    }
    tracks.set(node.id, buildTrack(el, node));
  }
  for (const el of tape.querySelectorAll(".track")) if (!tracks.has(el.dataset.id)) el.remove();

  const pill = $("pill");
  const feed = $("feed");
  feed.innerHTML = `<li class="feed-empty">Waiting for the first answer.</li>`;

  function addEvent({ kind, text }) {
    feed.querySelector(".feed-empty")?.remove();
    const li = document.createElement("li");
    li.className = `k-${kind}`;
    li.innerHTML = `<time>${clock(new Date())}</time><span></span>`;
    li.querySelector("span").textContent = text;
    feed.prepend(li);
    while (feed.children.length > MAX_FEED) feed.lastElementChild.remove();
  }

  function setPill(state, text) {
    pill.dataset.state = state;
    pill.querySelector("span").textContent = text;
  }

  function render(readings, reset) {
    const leader = leaderOf(readings);
    const allDown = readings.every((r) => !r.up);
    for (const r of readings) {
      const t = tracks.get(r.id);
      if (!t) continue;
      // A deposed leader can still report "Leader" until it hears the new term.
      const role = r.role === "leader" && leader && leader.id !== r.id ? "stale" : r.role;
      t.el.className = `track is-${role === "stale" ? "follower" : role}`;
      t.role.textContent = ROLE_LABEL[role];
      t.term.textContent = r.up ? r.term : "–";
      t.commit.textContent = r.up ? r.commitIndex : "–";
      t.applied.textContent = r.up ? r.lastApplied : "–";
      t.latency.textContent = r.up ? `${r.latencyMs} ms` : "no answer";
      t.trace = pushTrace(t.trace, role === "stale" ? "follower" : role, TRACE_LEN);
      const pad = TRACE_LEN - t.trace.length;
      t.bars.forEach((bar, i) => { bar.className = i < pad ? "" : `t-${t.trace[i - pad]}`; });
    }
    $("reset-banner").hidden = !(allDown && reset);
    $("offline-note").hidden = !(allDown && !reset);
    if (leader) setPill("leader", `${leader.id} leads term ${leader.term}`);
    else if (allDown) setPill(reset ? "reset" : "offline", reset ? "Hourly wipe" : "Unreachable");
    else setPill("noleader", "Electing a leader");
  }

  let prev = null;
  let busy = false;
  async function poll() {
    if (busy || document.hidden) return;
    busy = true;
    try {
      const readings = await Promise.all(CONFIG.nodes.map(pollNode));
      const reset = inResetWindow(Date.now());
      for (const ev of diffSnapshots(prev, readings, { inReset: reset })) addEvent(ev);
      prev = readings;
      render(readings, reset);
      onSnapshot(readings);
    } finally {
      busy = false;
    }
  }

  function tick() {
    const now = Date.now();
    const chaos = formatCountdown(msUntilChaos(now));
    const wipe = formatCountdown(msUntilReset(now));
    $("cd-chaos").textContent = chaos;
    $("hero-chaos").textContent = chaos;
    $("cd-reset").textContent = wipe;
    $("hero-reset").textContent = wipe;
  }

  poll();
  setInterval(poll, CONFIG.pollMs);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) poll(); });
  tick();
  setInterval(tick, 250);
}
