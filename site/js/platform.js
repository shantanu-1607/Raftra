// Release asset names (see .goreleaser.yaml) and a best-effort guess of the visitor's platform.

export const BUILDS = [
  { os: "darwin", arch: "arm64", label: "macOS", sub: "Apple silicon" },
  { os: "darwin", arch: "amd64", label: "macOS", sub: "Intel" },
  { os: "linux", arch: "amd64", label: "Linux", sub: "x86-64" },
  { os: "linux", arch: "arm64", label: "Linux", sub: "ARM64" },
  { os: "windows", arch: "amd64", label: "Windows", sub: "x64" },
  { os: "windows", arch: "arm64", label: "Windows", sub: "ARM64" },
];

export const assetName = (os, arch) => `raftra-cli_${os}_${arch}.${os === "windows" ? "zip" : "tar.gz"}`;
export const assetURL = (repo, os, arch) => `https://github.com/${repo}/releases/latest/download/${assetName(os, arch)}`;

// detectPlatform reads navigator-style strings. uaPlatform/uaArch come from
// navigator.userAgentData (Chromium only); uaArch is "arm" or "x86".
export function detectPlatform({ userAgent = "", platform = "", uaPlatform = "", uaArch = "" } = {}) {
  const ua = userAgent.toLowerCase();
  const p = (uaPlatform || platform).toLowerCase();
  let os = null;
  if (/android|iphone|ipad|ipod/.test(ua)) os = "mobile";
  else if (p.includes("win") || ua.includes("windows")) os = "windows";
  else if (p.includes("mac") || ua.includes("mac os")) os = "darwin";
  else if (p.includes("linux") || ua.includes("linux") || ua.includes("x11")) os = "linux";

  let arch = null;
  let guessed = false;
  if (uaArch) arch = uaArch === "arm" ? "arm64" : "amd64";
  else if (os === "darwin") {
    // Every Mac browser says "Intel Mac OS X"; most Macs in use are Apple silicon.
    arch = "arm64";
    guessed = true;
  } else if (/aarch64|arm64/.test(ua) || /aarch64|arm/.test(p)) arch = "arm64";
  else if (/x86_64|x64|win64|wow64|amd64/.test(ua) || /x86_64|win32/.test(p)) arch = "amd64";
  return { os, arch, guessed };
}
