# Raftra public playground on AWS Lightsail

Three `$5` Lightsail instances in Mumbai (`ap-south-1`), one per availability zone. Each runs `raftra-server` under systemd, with Caddy in front for automatic HTTPS. The steps below are the full runbook; the playground's limits and schedule are summarised in the root `README.md` and `AGENTS.md`.

```
https://raftra-n{1,2,3}.duckdns.org ──► Caddy :443 ──► raftra-server :8001
                                         Raft gRPC :50051 between PRIVATE IPs only
```

| File | Installed to | Purpose |
|---|---|---|
| `setup-node.sh` | (run once) | Swap, `raftra` user, config, units, binary, Caddy. Safe to re-run. |
| `install-server.sh` | `/usr/local/lib/raftra/` | Downloads `raftra-server` from GitHub Releases and verifies the checksum |
| `raftra.env.example` | `/etc/raftra/raftra.env` | Per-node settings (written by `setup-node.sh`) |
| `raftra.service` | `/etc/systemd/system/` | The node. `Restart=on-failure`, `RestartSec=45` |
| `Caddyfile`, `caddy-raftra.conf` | `/etc/caddy/`, `caddy.service.d/` | HTTPS reverse proxy, 2 KB body cap, `/metrics` hidden |
| `chaos.sh` + `raftra-chaos.{service,timer}` | | At `:05, :15, … :55` the leader SIGKILLs itself. Each node notes the term 5 s earlier, and a leader elected in the meantime is spared, so exactly one node dies per round |
| `reset.sh` + `raftra-reset.{service,timer}` | | At `:00:00` every node wipes its data; restart at `:00:30` |
| `deploy.sh` | (laptop) | Roll out a new release one node at a time |
| `update-config.sh` | (laptop) | Copy this folder's scripts and systemd units to all nodes |

## 1. Create the instances (Lightsail console)

1. **Create instance** ×3. Use Mumbai, Linux/Unix, OS Only → **Ubuntu 24.04 LTS**, and the **$5** dual-stack plan (public IPv4). Settings for each:

   | Name | Availability zone |
   |---|---|
   | `raftra-n1` | `ap-south-1a` |
   | `raftra-n2` | `ap-south-1b` |
   | `raftra-n3` | `ap-south-1c` |

2. **Networking → Create static IP** ×3. Attach one to each instance.
3. **Each instance → Networking → IPv4 Firewall:**
   - SSH (22): keep it. Later, restrict it to your IP and tick "Allow Lightsail browser SSH".
   - HTTP (80): open.
   - HTTPS (443): open.
   - **Never open 8001 or 50051.**
4. **DuckDNS:** set `raftra-n1/n2/n3` to the three **static** IPs. Don't add IPv6 (AAAA) records.
5. Note each instance's **private IP**, which is shown on its page (`172.26.x.x`).

## 2. Set up each node

Open each instance's browser SSH ("Connect using SSH") and run the following, with **your three private IPs in order n1 n2 n3**. Only the first number changes per node:

```bash
curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/deployments/lightsail/setup-node.sh \
  | sudo bash -s -- 1 <n1-private-ip> <n2-private-ip> <n3-private-ip>
```

On `raftra-n2`, use `-- 2 …` with the same three IPs; on `raftra-n3`, use `-- 3 …`. An optional 5th argument pins a release, e.g. `v0.2.0`. The default is the latest release.

## 3. Verify

```bash
# On a node
systemctl status raftra caddy --no-pager
journalctl -u raftra -n 30 --no-pager
curl -s 127.0.0.1:8001/status
systemctl list-timers 'raftra-*'

# From your laptop
curl -s https://raftra-n1.duckdns.org/status
raftra-cli set hello world && raftra-cli get hello
```

Expected:
- One node reports `"is_leader":true`, and all three agree on `leader_id`.
- `raftra-cli` works with no `--addr`, because the release default is the three DuckDNS URLs.
- `https://raftra-n1.duckdns.org/metrics` returns 404.

## 4. Operate

- **Upgrade:** `RAFTRA_SSH_KEY=~/Downloads/LightsailDefaultKey-ap-south-1.pem ./deploy.sh v0.2.1`. Download the key from Lightsail → Account → SSH keys.
  - Upgrading from v0.2.0 to v0.2.1 or later: until all three nodes run the new version, an old node treats a pre-vote as a real vote, so the rollout can cause an extra leader change. It stops once `deploy.sh` finishes.
- **Changed a script or unit here?** `RAFTRA_SSH_KEY=… ./update-config.sh` copies them to all nodes and restarts the timers.
- **Logs:**
  - `journalctl -u raftra -f`
  - Chaos and reset events: `journalctl -t raftra-chaos -t raftra-reset`
- **Pause chaos:** `sudo systemctl stop raftra-chaos.timer` on all three. Start it again the same way.

## 5. Security model

- **Public by design:** anyone can read, write and delete any key. It's a playground, so never store real data. Everything is wiped hourly.
- **Two locks on the internal ports:**
  - The Lightsail firewall, plus `ufw` on each host (covers IPv6 too), leaves only 22, 80 and 443 open publicly.
  - Raft gRPC `50051` accepts only the two peer private IPs. Without this, anyone reaching it could forge `AppendEntries` and corrupt the cluster.
  - `8001` is loopback-only, reachable by Caddy alone.
- **Abuse limits:**
  - Per-IP write rate (`429`), key, value and body size caps (`413`), a 10,000-key cap (`507`).
  - `MemoryMax=300M` restarts only `raftra` if a write flood grows the log.
  - The hourly wipe bounds log growth.
- **Host:**
  - SSH is key-only, with password login refused.
  - `raftra` runs as an unprivileged, sandboxed user (see `raftra.service`).
  - Ubuntu security updates install automatically (`unattended-upgrades`).
  - `/metrics` is hidden by Caddy.
- **Optional tightening:** restrict SSH (22) in the Lightsail firewall to your IP and tick "Allow Lightsail browser SSH".
- **Check a node:** `sudo ufw status`, `systemd-analyze security raftra`.

## 6. Cost and teardown

- 3 × $5 = **$15/month**, paid from credits. Static IPs are free **while attached**.
- **Budget alarm:** in AWS Billing → Budgets, create a $20/month cost budget with an email alert.
- **Teardown:** delete the 3 instances, **then release the 3 static IPs**. Unattached static IPs are billed.
