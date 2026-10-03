// The living background: one faint column per real node, drawn as that node's log.
// A block is added when the node commits, each answered poll sends a pulse up the
// column, the leader's column glows orange and a dead node's column turns red.
import { leaderOf } from "./cluster.js";

const BLOCK = 10;   // block height in CSS px
const GAP = 5;
const MAX_NEW = 8;  // blocks added per update, at most
const COLORS = {
  idle: [142, 151, 157],
  up: [233, 228, 216],
  leader: [255, 122, 26],
  down: [240, 71, 59],
  fresh: [85, 207, 171],
};

export function startBackground(canvas) {
  const ctx = canvas.getContext("2d");
  const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
  const cols = [0.16, 0.5, 0.84].map((x) => ({ x, state: "idle", blocks: 6, fresh: [], pulses: [], lastCommit: null }));
  const hairlines = [];
  let lastTerm = null;
  let w = 0, h = 0;
  // The cursor works as a flashlight: log slots near it light up. Mouse and pen only.
  const pointer = { x: -1e4, y: -1e4, sx: -1e4, sy: -1e4, on: false };
  const fine = matchMedia("(hover: hover) and (pointer: fine)").matches;
  const LIGHT = 190;
  const lit = (x, y) => {
    if (!pointer.on) return 0;
    const d = Math.hypot(x - pointer.sx, y - pointer.sy);
    return d >= LIGHT ? 0 : (1 - d / LIGHT) ** 2;
  };

  function resize() {
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    w = window.innerWidth;
    h = window.innerHeight;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    draw(performance.now());
  }

  function draw(now) {
    ctx.clearRect(0, 0, w, h);
    if (pointer.on) {
      pointer.sx += (pointer.x - pointer.sx) * 0.18;
      pointer.sy += (pointer.y - pointer.sy) * 0.18;
      const g = ctx.createRadialGradient(pointer.sx, pointer.sy, 0, pointer.sx, pointer.sy, LIGHT * 1.6);
      g.addColorStop(0, "rgba(255,122,26,0.075)");
      g.addColorStop(1, "rgba(255,122,26,0)");
      ctx.fillStyle = g;
      ctx.fillRect(pointer.sx - LIGHT * 1.6, pointer.sy - LIGHT * 1.6, LIGHT * 3.2, LIGHT * 3.2);
    }
    const colW = Math.max(14, Math.min(26, w * 0.018));
    const capacity = Math.floor((h - 40) / (BLOCK + GAP));
    for (const c of cols) {
      const x = Math.round(c.x * w - colW / 2);
      const base = c.state === "down" ? COLORS.down : COLORS[c.state] || COLORS.idle;
      const alpha = c.state === "leader" ? 0.15 : c.state === "down" ? 0.12 : 0.07;
      // Rail.
      ctx.fillStyle = `rgba(${base.join(",")},0.05)`;
      ctx.fillRect(x + colW / 2 - 0.5, 0, 1, h);
      // Empty log slots up the whole column, so it reads as a log even when it's short.
      ctx.lineWidth = 1;
      for (let i = 0; i < capacity; i++) {
        const y = h - 20 - (i + 1) * (BLOCK + GAP);
        ctx.strokeStyle = `rgba(${base.join(",")},${0.045 + 0.4 * lit(x + colW / 2, y)})`;
        ctx.strokeRect(x + 0.5, y + 0.5, colW - 1, BLOCK - 1);
      }
      // Committed entries, stacked from the bottom. The newest sit on top.
      const shown = Math.min(c.blocks, capacity);
      for (let i = 0; i < shown; i++) {
        const y = h - 20 - (i + 1) * (BLOCK + GAP);
        const age = shown - 1 - i; // 0 = newest
        const f = c.fresh.find((e) => e.slot === age);
        let rgb = base, a = alpha;
        if (f) {
          const k = Math.max(0, 1 - (now - f.at) / 2500);
          rgb = COLORS.fresh;
          a = alpha + 0.45 * k;
        }
        ctx.fillStyle = `rgba(${rgb.join(",")},${a + 0.45 * lit(x + colW / 2, y)})`;
        ctx.fillRect(x, y, colW, BLOCK);
      }
      // Heartbeat pulses travel up the rail.
      for (const p of c.pulses) {
        const t = (now - p) / 1600;
        if (t > 1) continue;
        const y = h - t * h;
        const g = ctx.createLinearGradient(0, y, 0, y + 90);
        g.addColorStop(0, `rgba(${base.join(",")},${0.22 * (1 - t)})`);
        g.addColorStop(1, `rgba(${base.join(",")},0)`);
        ctx.fillStyle = g;
        ctx.fillRect(x + colW / 2 - 1, y, 2, 90);
      }
    }
    // A new term draws a hairline across all columns.
    for (const l of hairlines) {
      const t = (now - l.at) / 1800;
      if (t > 1) continue;
      ctx.fillStyle = `rgba(255,122,26,${0.28 * (1 - t)})`;
      ctx.fillRect(0, l.y, w, 1);
    }
  }

  function prune(now) {
    for (const c of cols) {
      c.pulses = c.pulses.filter((p) => now - p < 1600);
      c.fresh = c.fresh.filter((e) => now - e.at < 2500);
    }
    while (hairlines.length && now - hairlines[0].at > 1800) hairlines.shift();
  }

  let raf = 0;
  function loop(now) {
    prune(now);
    draw(now);
    raf = requestAnimationFrame(loop);
  }

  function update(readings) {
    const now = performance.now();
    const leader = leaderOf(readings);
    readings.forEach((r, i) => {
      const c = cols[i];
      if (!c) return;
      c.state = !r.up ? "down" : leader && leader.id === r.id ? "leader" : "up";
      if (!r.up) return;
      if (c.lastCommit === null || r.commitIndex < c.lastCommit) {
        c.blocks = Math.max(r.commitIndex, 1); // first answer, or the log was wiped
      } else if (r.commitIndex > c.lastCommit) {
        const added = Math.min(MAX_NEW, r.commitIndex - c.lastCommit);
        c.blocks = r.commitIndex;
        c.fresh = c.fresh.map((e) => ({ ...e, slot: e.slot + added }));
        for (let k = 0; k < added; k++) c.fresh.push({ slot: k, at: now });
      }
      c.lastCommit = r.commitIndex;
      c.pulses.push(now);
    });
    const term = leader ? leader.term : null;
    if (term !== null) {
      if (lastTerm !== null && term > lastTerm) hairlines.push({ at: now, y: Math.round(h * (0.25 + Math.random() * 0.5)) });
      lastTerm = term;
    }
    if (reduce) { prune(now); draw(now); }
  }

  window.addEventListener("resize", resize);
  if (fine && !reduce) {
    window.addEventListener("pointermove", (e) => {
      if (e.pointerType !== "mouse" && e.pointerType !== "pen") return;
      if (!pointer.on) { pointer.sx = e.clientX; pointer.sy = e.clientY; }
      pointer.x = e.clientX; pointer.y = e.clientY; pointer.on = true;
    }, { passive: true });
    document.documentElement.addEventListener("pointerleave", () => { pointer.on = false; });
  }
  resize();
  if (!reduce) {
    raf = requestAnimationFrame(loop);
    document.addEventListener("visibilitychange", () => {
      cancelAnimationFrame(raf);
      if (!document.hidden) raf = requestAnimationFrame(loop);
    });
  }
  return { update };
}
