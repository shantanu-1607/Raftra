// Draws the Raft simulator (raft-sim.js) as SVG and wires up its controls.
import { createSim, step, toggleNode, setPartition, clientWrite, inSameSide } from "./raft-sim.js";

const NS = "http://www.w3.org/2000/svg";
const BASE_RATE = 0.1;     // simulated ticks per real millisecond at 1×: a 150–300 tick timeout takes 1.5–3 s
const CX = 320, CY = 282, RING = 196, R = 44;
const LOG_SHOWN = 10;
const TERM_COLORS = ["#6f9bd6", "#c78fd8", "#d6a86f", "#7fc7d9", "#d67f8f", "#a5d67f"];
const ELECTION_MAX = 300;
const ROLE_WORD = { leader: "leader", follower: "follower", candidate: "candidate" };

const el = (tag, attrs = {}, parent) => {
  const e = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
};

function positions(n) {
  return Array.from({ length: n }, (_, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    return { x: CX + RING * Math.cos(a), y: CY + RING * Math.sin(a) };
  });
}

export function startSimView(root) {
  const svg = root.querySelector("#sim-svg");
  const caption = root.querySelector("#sim-caption");
  const logList = root.querySelector("#sim-log");
  const controls = root.querySelector("#sim-controls");
  const partitionBtn = controls.querySelector('[data-act="partition"]');
  const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;

  let sim = createSim();
  let speed = reduce ? 0.25 : 1;
  let acc = 0; // fractional ticks not yet simulated, also used to smooth drawing
  const vnow = () => sim.now + acc;
  const pos = positions(sim.nodes.length);
  const posOf = (id) => pos[sim.nodes.findIndex((n) => n.id === id)];

  // Static layers, back to front.
  el("circle", { cx: CX, cy: CY, r: RING, class: "s-ring" }, svg);
  const splitLayer = el("g", {}, svg);
  const msgLayer = el("g", {}, svg);
  const nodeLayer = el("g", {}, svg);

  const views = sim.nodes.map((n, i) => {
    const { x, y } = pos[i];
    const g = el("g", { class: "s-node", tabindex: "0", role: "button", "data-id": n.id }, nodeLayer);
    const timer = el("circle", { cx: x, cy: y, r: R + 8, class: "s-timer", transform: `rotate(-90 ${x} ${y})` }, g);
    el("circle", { cx: x, cy: y, r: R, class: "s-body" }, g);
    const cross = el("path", { class: "s-x", d: `M${x - 26} ${y - 26} L${x + 26} ${y + 26} M${x + 26} ${y - 26} L${x - 26} ${y + 26}`, visibility: "hidden" }, g);
    const crown = el("path", { class: "s-crown", d: `M${x - 14} ${y - R - 14} l5 -11 l9 7 l9 -7 l5 11 z`, visibility: "hidden" }, g);
    const name = el("text", { x, y: y + 4, class: "s-name" }, g);
    name.textContent = n.id;
    const term = el("text", { x, y: y + 22, class: "s-term" }, g);
    // The log sits under the node, or above it for the bottom two so it stays inside the box.
    const below = y < CY + RING * 0.5;
    const logY = below ? y + R + 16 : y - R - 30;
    const logG = el("g", {}, g);
    return { g, timer, crown, term, cross, logG, logX: x, logY, key: "", label: "" };
  });

  const circ = 2 * Math.PI * (R + 8);
  views.forEach((v) => v.timer.setAttribute("stroke-dasharray", `${circ} ${circ}`));

  function drawSplit() {
    splitLayer.textContent = "";
    if (!sim.partition) return;
    const [a, b] = sim.partition.map((side) => {
      const ps = [...side].map(posOf);
      return { x: ps.reduce((s, p) => s + p.x, 0) / ps.length, y: ps.reduce((s, p) => s + p.y, 0) / ps.length };
    });
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
    let dx = -(b.y - a.y), dy = b.x - a.x;
    const len = Math.hypot(dx, dy);
    dx /= len; dy /= len;
    const L = 300;
    el("line", { x1: mx - dx * L, y1: my - dy * L, x2: mx + dx * L, y2: my + dy * L, class: "s-split" }, splitLayer);
    const t = el("text", { x: mx + dx * 255 + 12, y: my + dy * 255 + 4, class: "s-split-label" }, splitLayer);
    t.textContent = "network split";
  }

  function drawNodes() {
    sim.nodes.forEach((n, i) => {
      const v = views[i];
      const role = n.alive ? n.role : "dead";
      v.g.setAttribute("class", `s-node ${role}`);
      v.crown.setAttribute("visibility", role === "leader" ? "visible" : "hidden");
      v.cross.setAttribute("visibility", role === "dead" ? "visible" : "hidden");
      v.term.textContent = n.alive ? `term ${n.term}` : "crashed";
      // The ring empties as the election timeout runs out.
      const left = n.alive && n.role !== "leader" ? Math.max(0, Math.min(1, (n.electionDeadline - vnow()) / ELECTION_MAX)) : 0;
      v.timer.setAttribute("stroke-dashoffset", String(circ * (1 - left)));
      const label = `${n.id}, ${n.alive ? ROLE_WORD[n.role] : "crashed"}. ${n.alive ? "Press to crash it." : "Press to restart it."}`;
      if (label !== v.label) { v.g.setAttribute("aria-label", label); v.label = label; }

      const key = `${n.log.length}:${n.commitIndex}:${n.log.length ? n.log[n.log.length - 1].term : 0}`;
      if (key === v.key) return;
      v.key = key;
      v.logG.textContent = "";
      const start = Math.max(0, n.log.length - LOG_SHOWN);
      const shown = n.log.slice(start);
      const bw = 9, gap = 3;
      const total = LOG_SHOWN * (bw + gap) - gap;
      const x0 = v.logX - total / 2;
      for (let k = 0; k < LOG_SHOWN; k++) {
        const e = shown[k];
        const x = x0 + k * (bw + gap);
        if (!e) {
          el("rect", { x, y: v.logY, width: bw, height: 14, fill: "none", stroke: "#283036", "stroke-width": 1 }, v.logG);
          continue;
        }
        const color = TERM_COLORS[(e.term - 1) % TERM_COLORS.length];
        const committed = start + k + 1 <= n.commitIndex;
        el("rect", { x, y: v.logY, width: bw, height: 14, class: "s-blk", fill: committed ? color : "none", stroke: color }, v.logG);
      }
      if (start > 0) {
        const t = el("text", { x: x0 - 6, y: v.logY + 11, class: "s-term", "text-anchor": "end" }, v.logG);
        t.textContent = `+${start}`;
      }
    });
  }

  function drawMessages() {
    let html = "";
    for (const m of sim.messages) {
      const a = posOf(m.from), b = posOf(m.to);
      const t = Math.max(0, Math.min(1, (vnow() - m.sentAt) / (m.arriveAt - m.sentAt)));
      const x = a.x + (b.x - a.x) * t, y = a.y + (b.y - a.y) * t;
      const to = sim.nodes.find((n) => n.id === m.to);
      const doomed = !to.alive || !inSameSide(sim, m.from, m.to);
      let cls, r;
      if (m.type === "RV") { cls = "s-msg-rv"; r = 7; }
      else if (m.type === "AE") { cls = "s-msg-ae"; r = m.entries.length ? 7.5 : 4.5; }
      else { cls = "s-msg-reply"; r = 3.5; }
      const style = doomed ? ` style="opacity:${(1 - t * 0.7).toFixed(2)}" fill="#f0473b" stroke="none"` : "";
      html += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" class="${cls}"${style}/>`;
    }
    msgLayer.innerHTML = html;
  }

  let lastEventCount = -1, lastEventAt = -1;
  function drawText() {
    const evs = sim.events;
    const newest = evs[evs.length - 1];
    if (evs.length === lastEventCount && (!newest || newest.at === lastEventAt)) return;
    lastEventCount = evs.length;
    lastEventAt = newest ? newest.at : -1;
    caption.textContent = newest ? sentence(newest.text) : "Waiting for the first election.";
    logList.innerHTML = "";
    for (const e of evs.slice(-7).reverse()) {
      const li = document.createElement("li");
      li.textContent = `${String(Math.round(e.at)).padStart(5)} ms  ${e.text}`;
      logList.appendChild(li);
    }
  }
  const sentence = (s) => s.charAt(0).toUpperCase() + s.slice(1) + ".";

  function draw() { drawSplit(); drawNodes(); drawMessages(); drawText(); }

  // Controls.
  nodeLayer.addEventListener("click", (e) => {
    const g = e.target.closest(".s-node");
    if (g) { toggleNode(sim, g.dataset.id); draw(); }
  });
  nodeLayer.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const g = e.target.closest(".s-node");
    if (g) { e.preventDefault(); toggleNode(sim, g.dataset.id); draw(); }
  });
  const speedBtns = [...controls.querySelectorAll("[data-speed]")];
  const markSpeed = () => speedBtns.forEach((b) => b.setAttribute("aria-pressed", String(Number(b.dataset.speed) === speed)));
  markSpeed();
  controls.addEventListener("click", (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    if (b.dataset.speed) { speed = Number(b.dataset.speed); markSpeed(); return; }
    switch (b.dataset.act) {
      case "write":
        if (clientWrite(sim) === 0) {
          b.classList.remove("shake");
          void b.offsetWidth;
          b.classList.add("shake");
        }
        break;
      case "partition":
        setPartition(sim, !sim.partition);
        break;
      case "reset":
        sim = createSim();
        acc = 0;
        views.forEach((v) => { v.key = ""; });
        lastEventCount = -1;
        break;
    }
    partitionBtn.setAttribute("aria-pressed", String(Boolean(sim.partition)));
    partitionBtn.textContent = sim.partition ? "Heal the network" : "Split the network";
    draw();
  });

  // Run only while the simulator is on screen.
  let visible = false, raf = 0, last = 0;
  function frame(now) {
    const dt = Math.min(50, now - last);
    last = now;
    // step() advances whole ticks; carry the fraction so ¼× really is a quarter speed.
    acc += dt * BASE_RATE * speed;
    const whole = Math.floor(acc);
    acc -= whole;
    if (whole) step(sim, whole);
    draw();
    raf = requestAnimationFrame(frame);
  }
  new IntersectionObserver(([entry]) => {
    visible = entry.isIntersecting;
    cancelAnimationFrame(raf);
    if (visible) { last = performance.now(); raf = requestAnimationFrame(frame); }
  }, { threshold: 0.15 }).observe(root);
  draw();
}
