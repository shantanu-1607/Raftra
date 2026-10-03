// Plays the write path through the "How a write survives" diagram on a loop:
// packets travel the arrows in order, disk and map rows flash, and the matching
// numbered step underneath lights up.

const NS = "http://www.w3.org/2000/svg";
const LOOP = 7600; // ms

// Arrow geometry, matching the paths drawn in index.html.
const P = {
  toFollower: [[330, 22], [96, 22], [96, 116]],
  redirect: [[200, 120], [200, 44], [326, 44]],
  toLeader: [[480, 54], [480, 116]],
  appendLeft: [[376, 256], [248, 256]],
  appendRight: [[584, 256], [712, 256]],
  ackLeft: [[248, 270], [370, 270]],
  ackRight: [[712, 270], [590, 270]],
  reply: [[480, 116], [480, 54]],
};

// [path, start ms, duration ms, packet colour class, step 1–5]
const PACKETS = [
  ["toFollower", 0, 900, "pk-req", 1],
  ["redirect", 1000, 800, "pk-reply", 2],
  ["toLeader", 1900, 600, "pk-req", 3],
  ["appendLeft", 3100, 700, "pk-req", 4],
  ["appendRight", 3100, 700, "pk-req", 4],
  ["ackLeft", 4300, 700, "pk-reply", 4],
  ["ackRight", 4300, 700, "pk-reply", 4],
  ["reply", 5500, 600, "pk-ok", 5],
];

// Rows that flash: [x, y, start ms, step]. Rects match the layer boxes in the SVG.
const FLASHES = [
  [376, 286, 2500, 3], // leader writes its log
  [36, 286, 3800, 4], // followers store the entry
  [716, 286, 3800, 4],
  [376, 328, 5000, 5], // committed: leader applies to its map
  [36, 328, 6300, 5], // followers apply on the next heartbeat
  [716, 328, 6300, 5],
];
const FLASH_MS = 900;

// Which step is "current" at time t.
const STEPS = [[0, 1], [1000, 2], [1900, 3], [3100, 4], [5000, 5], [7000, 0]];

function pointAt(points, t) {
  const segs = [];
  let total = 0;
  for (let i = 1; i < points.length; i++) {
    const len = Math.hypot(points[i][0] - points[i - 1][0], points[i][1] - points[i - 1][1]);
    segs.push(len);
    total += len;
  }
  let d = t * total;
  for (let i = 0; i < segs.length; i++) {
    if (d <= segs[i] || i === segs.length - 1) {
      const k = segs[i] ? Math.min(1, d / segs[i]) : 1;
      const [x0, y0] = points[i], [x1, y1] = points[i + 1];
      return [x0 + (x1 - x0) * k, y0 + (y1 - y0) * k];
    }
    d -= segs[i];
  }
  return points[points.length - 1];
}

const ease = (t) => (t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2);

export function startDiagram(svg, stepsList) {
  if (!svg) return;
  const steps = stepsList ? [...stepsList.children] : [];
  const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (reduce) return; // the static diagram and the numbered steps already tell the story

  const layer = document.createElementNS(NS, "g");
  layer.setAttribute("class", "dg-anim");
  layer.setAttribute("aria-hidden", "true");
  svg.appendChild(layer);

  const flashes = FLASHES.map(([x, y]) => {
    const r = document.createElementNS(NS, "rect");
    Object.entries({ x, y, width: 208, height: 34, class: "dg-flash", opacity: 0 }).forEach(([k, v]) => r.setAttribute(k, v));
    layer.appendChild(r);
    return r;
  });
  const packets = PACKETS.map(([, , , cls]) => {
    const g = document.createElementNS(NS, "g");
    g.setAttribute("class", cls);
    g.setAttribute("opacity", "0");
    for (const [r, o] of [[11, 0.18], [6, 1]]) {
      const c = document.createElementNS(NS, "circle");
      c.setAttribute("r", r);
      c.setAttribute("opacity", o);
      g.appendChild(c);
    }
    layer.appendChild(g);
    return g;
  });

  const ok = document.createElementNS(NS, "text");
  Object.entries({ x: 468, y: 92, class: "dg-ok", "text-anchor": "end", opacity: 0 }).forEach(([k, v]) => ok.setAttribute(k, v));
  ok.textContent = "200 OK";
  layer.appendChild(ok);

  let raf = 0, t0 = 0, lastStep = -1;
  function frame(now) {
    const t = (now - t0) % LOOP;
    PACKETS.forEach(([path, start, dur], i) => {
      const k = (t - start) / dur;
      const g = packets[i];
      if (k < 0 || k > 1) { g.setAttribute("opacity", "0"); return; }
      const [x, y] = pointAt(P[path], ease(k));
      g.setAttribute("transform", `translate(${x.toFixed(1)} ${y.toFixed(1)})`);
      g.setAttribute("opacity", k > 0.92 ? String((1 - k) / 0.08) : "1");
    });
    FLASHES.forEach(([, , start], i) => {
      const k = (t - start) / FLASH_MS;
      flashes[i].setAttribute("opacity", k < 0 || k > 1 ? "0" : String((1 - k) * 0.9));
    });
    const okK = (t - 5500) / 1400;
    ok.setAttribute("opacity", okK < 0 || okK > 1 ? "0" : String(okK < 0.8 ? 1 : (1 - okK) / 0.2));
    let step = 0;
    for (const [at, s] of STEPS) if (t >= at) step = s;
    if (step !== lastStep) {
      steps.forEach((li, i) => li.classList.toggle("is-active", i + 1 === step));
      lastStep = step;
    }
    raf = requestAnimationFrame(frame);
  }

  const stop = () => {
    cancelAnimationFrame(raf);
    raf = 0;
    steps.forEach((li) => li.classList.remove("is-active"));
    lastStep = -1;
  };
  new IntersectionObserver(([e]) => {
    if (e.isIntersecting && !raf) { t0 = performance.now(); raf = requestAnimationFrame(frame); }
    else if (!e.isIntersecting) stop();
  }, { threshold: 0.3 }).observe(svg);
}
