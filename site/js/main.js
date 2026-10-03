// Entry point: wires the live recorder, background, simulator and install section together.
import { CONFIG } from "../config.js";
import { detectPlatform } from "./platform.js";
import { startLive } from "./live.js";
import { startBackground } from "./background.js";
import { startSimView } from "./sim-view.js";
import { startDiagram } from "./diagram.js";

const FAILOVER_MS = [227, 236, 207, 281, 291, 270, 211, 217, 283, 208, 269, 253, 249, 219, 276, 234, 205, 281, 232, 255];
const OS_NAME = { darwin: "macOS", linux: "Linux", windows: "Windows" };
const ARCH_NAME = { darwin: { arm64: "Apple silicon", amd64: "Intel" }, linux: { arm64: "ARM64", amd64: "x86-64" }, windows: { arm64: "ARM64", amd64: "x64" } };

function setupCopy() {
  document.addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-copy]");
    if (!btn) return;
    const code = btn.closest("[data-copy-wrap]").querySelector("[data-copy-src]");
    try {
      await navigator.clipboard.writeText(code.textContent.trim());
      btn.textContent = "Copied";
      btn.dataset.done = "";
    } catch {
      getSelection().selectAllChildren(code); // let the visitor copy it themselves
      btn.textContent = "Selected";
    }
    setTimeout(() => { btn.textContent = "Copy"; delete btn.dataset.done; }, 1600);
  });
}

function selectOS(os) {
  for (const tab of document.querySelectorAll("[data-os-tab]")) {
    tab.setAttribute("aria-selected", String(tab.dataset.osTab === os));
    tab.tabIndex = tab.dataset.osTab === os ? 0 : -1;
  }
  for (const panel of document.querySelectorAll("[data-os-panel]")) panel.hidden = panel.dataset.osPanel !== os;
}

async function setupInstall() {
  const tabs = [...document.querySelectorAll("[data-os-tab]")];
  tabs.forEach((tab, i) => {
    tab.addEventListener("click", () => selectOS(tab.dataset.osTab));
    tab.addEventListener("keydown", (e) => {
      const d = e.key === "ArrowRight" ? 1 : e.key === "ArrowLeft" ? -1 : 0;
      if (!d) return;
      const next = tabs[(i + d + tabs.length) % tabs.length];
      selectOS(next.dataset.osTab);
      next.focus();
    });
  });
  selectOS("darwin");

  const nav = navigator;
  let uaArch = "", uaPlatform = "";
  try {
    if (nav.userAgentData) {
      uaPlatform = nav.userAgentData.platform || "";
      const hints = await nav.userAgentData.getHighEntropyValues(["architecture"]);
      uaArch = hints.architecture || "";
    }
  } catch { /* client hints are optional */ }
  const { os, arch, guessed } = detectPlatform({ userAgent: nav.userAgent, platform: nav.platform, uaPlatform, uaArch });
  const detect = document.getElementById("install-detect");

  if (os === "mobile") {
    detect.innerHTML = `Install it on a computer<small>The CLI runs on macOS, Linux and Windows. Pick your system below, or keep watching the cluster from your phone.</small>`;
    return;
  }
  if (!os) return;
  selectOS(os);

  if (os === "windows") {
    // The hero's one-liner is for macOS and Linux; give Windows visitors theirs.
    const hero = document.querySelector(".hero .cmd");
    hero.classList.add("cmd-ps");
    hero.querySelector("[data-copy-src]").textContent = document.querySelector("#os-windows .cmd-ps [data-copy-src]").textContent;
  }
  if (!arch) return;
  document.querySelector(`.matrix a[data-os="${os}"][data-arch="${arch}"]`)?.classList.add("is-you");
  const what = `${OS_NAME[os]} on ${ARCH_NAME[os][arch]}`;
  const hint = guessed
    ? "Your browser doesn’t say which chip this Mac has. The script checks for itself."
    : "Detected from your browser. The script checks your CPU for itself too.";
  detect.innerHTML = `${what}<small>${hint}</small>`;
}

function drawFailover() {
  const svg = document.getElementById("failover-strip");
  if (!svg) return;
  const lo = 180, hi = 320, x = (ms) => 16 + ((ms - lo) / (hi - lo)) * 368;
  const avg = FAILOVER_MS.reduce((a, b) => a + b, 0) / FAILOVER_MS.length;
  let s = `<line x1="16" x2="384" y1="66" y2="66" class="st-axis"/>`;
  for (const t of [200, 250, 300]) {
    s += `<line x1="${x(t)}" x2="${x(t)}" y1="62" y2="70" class="st-axis"/><text x="${x(t)}" y="86" class="st-tick">${t} ms</text>`;
  }
  s += `<line x1="${x(avg)}" x2="${x(avg)}" y1="8" y2="66" class="st-avg"/><text x="${x(avg) + 6}" y="16" class="st-avg-l">average</text>`;
  FAILOVER_MS.forEach((ms, i) => { s += `<circle cx="${x(ms).toFixed(1)}" cy="${28 + (i % 4) * 9}" r="4.5" class="st-dot"/>`; });
  svg.innerHTML = s;
}

// Star count and latest version from GitHub's public API (60 requests an hour per visitor IP),
// cached for the browser session. The page works the same if this fails.
async function githubJSON(path) {
  const key = `gh:${path}`;
  try {
    const hit = sessionStorage.getItem(key);
    if (hit) return JSON.parse(hit);
  } catch { /* storage can be blocked */ }
  const res = await fetch(`https://api.github.com/repos/${CONFIG.repo}${path}`, { headers: { Accept: "application/vnd.github+json" } });
  if (!res.ok) throw new Error(`GitHub ${res.status}`);
  const data = await res.json();
  try { sessionStorage.setItem(key, JSON.stringify(data)); } catch { /* fine */ }
  return data;
}

async function setupGitHub() {
  try {
    const repo = await githubJSON("");
    const n = repo.stargazers_count;
    if (typeof n === "number" && n > 0) { // a "0" badge only discourages
      for (const el of document.querySelectorAll("[data-stars]")) {
        el.textContent = n.toLocaleString("en");
        el.hidden = false;
      }
    }
  } catch { /* keep the plain "Star" buttons */ }
  try {
    const rel = await githubJSON("/releases/latest");
    if (rel.tag_name) for (const el of document.querySelectorAll("[data-version]")) el.textContent = rel.tag_name;
  } catch { /* keep the version baked into the page */ }
}

// Underline the nav link of the section crossing a thin band 35% down the screen.
function setupNav() {
  const links = new Map([...document.querySelectorAll('.nav a[href^="#"]')].map((a) => [a.getAttribute("href").slice(1), a]));
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) links.get(e.target.id)?.classList.toggle("is-here", e.isIntersecting);
  }, { rootMargin: "-35% 0px -64% 0px" });
  for (const id of links.keys()) {
    const el = document.getElementById(id);
    if (el) io.observe(el);
  }
}

setupCopy();
setupGitHub();
setupNav();
setupInstall();
drawFailover();
const background = startBackground(document.getElementById("bg"));
startLive({ onSnapshot: (readings) => background.update(readings) });
startSimView(document.getElementById("sim-root"));
startDiagram(document.querySelector(".diagram"), document.querySelector(".steps"));
