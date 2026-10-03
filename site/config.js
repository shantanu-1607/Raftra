// Where the landing page finds the live cluster.
// For local testing, ?nodes=http://127.0.0.1:8001,http://127.0.0.1:8002,... overrides the list
// (start those nodes with -cors-origin '*').
const DEFAULT_NODES = [
  "https://raftra-n1.duckdns.org",
  "https://raftra-n2.duckdns.org",
  "https://raftra-n3.duckdns.org",
];

const override = new URLSearchParams(location.search).get("nodes");
const urls = override
  ? override.split(",").map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean)
  : DEFAULT_NODES;

export const CONFIG = {
  repo: "shantanu-1607/Raftra",
  nodes: urls.map((url, i) => ({ id: `node${i + 1}`, url })),
  pollMs: 2000,
  timeoutMs: 1500,
};
