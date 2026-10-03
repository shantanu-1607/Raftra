// Entry point: wires the live recorder, background, simulator and install section together.
import { CONFIG } from "../config.js";
import { detectPlatform, assetURL, assetName } from "./platform.js";
import { startLive } from "./live.js";
import { startBackground } from "./background.js";
import { startSimView } from "./sim-view.js";

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

async function setupInstall() {
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
  const box = document.getElementById("install-primary");
  const detect = box.querySelector(".detect");

  if (os === "mobile") {
    detect.innerHTML = `The CLI runs on macOS, Linux and Windows.<small>Open this page on a computer to install it. The recorder and simulator work fine on your phone.</small>`;
    return;
  }
  if (!os || !arch) return; // keep the default macOS/Linux instructions

  const link = document.querySelector(`.matrix a[data-os="${os}"][data-arch="${arch}"]`);
  link?.classList.add("is-you");
  const what = `${OS_NAME[os]} on ${ARCH_NAME[os][arch]}`;
  const hint = guessed ? "Your browser doesn’t say which chip this Mac has. If it’s an Intel Mac, the script still picks the right build." : "Detected from your browser.";

  if (os === "windows") {
    box.innerHTML = `
      <p class="detect">${what}<small>${hint}</small></p>
      <a class="btn btn-primary" href="${assetURL(CONFIG.repo, os, arch)}">Download ${assetName(os, arch)}</a>
      <p class="fine">Unzip it, open a terminal in that folder and run <code>.\\raftra-cli.exe status</code>.</p>`;
    return;
  }
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

setupCopy();
setupInstall();
drawFailover();
const background = startBackground(document.getElementById("bg"));
startLive({ onSnapshot: (readings) => background.update(readings) });
startSimView(document.getElementById("sim-root"));
