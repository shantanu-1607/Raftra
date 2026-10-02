# Raftra public playground on AWS Lightsail

Three `$5` Lightsail instances in Mumbai (`ap-south-1`), one per availability zone. Each runs `raftra-server` under systemd, with Caddy in front for automatic HTTPS. The design is in `docs/superpowers/specs/2026-10-03-public-playground-design.md`.

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
| `chaos.sh` + `raftra-chaos.{service,timer}` | | At `:05, :15, … :55` the leader SIGKILLs itself |
| `reset.sh` + `raftra-reset.{service,timer}` | | At `:00:00` every node wipes its data; restart at `:00:30` |
| `deploy.sh` | (laptop) | Roll out a new release one node at a time |

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
- **Logs:**
  - `journalctl -u raftra -f`
  - Chaos and reset events: `journalctl -t raftra-chaos -t raftra-reset`
- **Pause chaos:** `sudo systemctl stop raftra-chaos.timer` on all three. Start it again the same way.

## 5. Cost and teardown

- 3 × $5 = **$15/month**, paid from credits. Static IPs are free **while attached**.
- **Budget alarm:** in AWS Billing → Budgets, create a $20/month cost budget with an email alert.
- **Teardown:** delete the 3 instances, **then release the 3 static IPs**. Unattached static IPs are billed.
