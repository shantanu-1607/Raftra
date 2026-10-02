#!/usr/bin/env bash
# setup-node.sh N PRIVATE_IP_1 PRIVATE_IP_2 PRIVATE_IP_3 [VERSION]
#
# One-time (re-runnable) setup of playground node N (1, 2 or 3) on a fresh
# Ubuntu 24.04 Lightsail instance. Run as root, e.g.:
#   curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/deployments/lightsail/setup-node.sh \
#     | sudo bash -s -- 1 172.26.0.11 172.26.0.12 172.26.0.13
set -euo pipefail

REPO=shantanu-1607/Raftra
RAW="https://raw.githubusercontent.com/$REPO/${RAFTRA_REF:-main}/deployments/lightsail"

usage() {
	echo "usage: setup-node.sh N PRIVATE_IP_1 PRIVATE_IP_2 PRIVATE_IP_3 [VERSION]" >&2
	exit 1
}
[ $# -ge 4 ] || usage
case "$1" in 1 | 2 | 3) ;; *) usage ;; esac
[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }

n=$1
privs=("$2" "$3" "$4")
version=${5:-latest}

echo "==> Node $n: swap, user, directories"
# 512 MB of RAM is tight for apt; 1 GB of swap keeps installs from failing.
if ! swapon --show | grep -q /swapfile; then
	fallocate -l 1G /swapfile
	chmod 600 /swapfile
	mkswap /swapfile >/dev/null
	swapon /swapfile
	grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >>/etc/fstab
fi
id raftra >/dev/null 2>&1 || useradd --system --home-dir /var/lib/raftra --shell /usr/sbin/nologin raftra
install -d -o raftra -g raftra -m 750 /var/lib/raftra
install -d -m 755 /etc/raftra /usr/local/lib/raftra

echo "==> Config /etc/raftra/raftra.env"
peers=""
http_peers=""
for i in 1 2 3; do
	[ "$i" = "$n" ] && continue
	peers="${peers:+$peers,}node$i:${privs[$((i - 1))]}:50051"
	http_peers="${http_peers:+$http_peers,}node$i:https://raftra-n$i.duckdns.org"
done
cat >/etc/raftra/raftra.env <<ENV
NODE_ID=node$n
PUBLIC_HOSTNAME=raftra-n$n.duckdns.org
PEERS=$peers
HTTP_PEERS=$http_peers
LIMIT_FLAGS=-max-key-bytes 128 -max-value-bytes 1024 -max-keys 10000 -write-rate 5 -write-burst 10 -trust-proxy -cors-origin https://shantanu-1607.github.io
ENV
chmod 644 /etc/raftra/raftra.env
cat /etc/raftra/raftra.env

echo "==> Scripts and systemd units"
for f in install-server.sh chaos.sh reset.sh; do
	curl -fsSL -o "/usr/local/lib/raftra/$f" "$RAW/$f"
	chmod 755 "/usr/local/lib/raftra/$f"
done
for f in raftra.service raftra-chaos.service raftra-chaos.timer raftra-reset.service raftra-reset.timer; do
	curl -fsSL -o "/etc/systemd/system/$f" "$RAW/$f"
done

echo "==> raftra-server"
/usr/local/lib/raftra/install-server.sh "$version"

echo "==> Caddy (HTTPS)"
if ! command -v caddy >/dev/null 2>&1; then
	apt-get update -qq
	apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl gnupg
	curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/gpg.key |
		gpg --batch --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
	curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt >/etc/apt/sources.list.d/caddy-stable.list
	apt-get update -qq
	apt-get install -y -qq caddy
fi
install -d /etc/systemd/system/caddy.service.d
curl -fsSL -o /etc/systemd/system/caddy.service.d/raftra.conf "$RAW/caddy-raftra.conf"
curl -fsSL -o /etc/caddy/Caddyfile "$RAW/Caddyfile"

echo "==> Host firewall (ufw) and automatic security updates"
# Second lock behind the Lightsail firewall. It covers IPv6 too, because
# raftra-server's 0.0.0.0 listener also accepts IPv6. Raft gRPC (50051) is
# allowed only from the two peer private IPs; 8001 stays loopback-only (Caddy).
apt-get install -y -qq ufw unattended-upgrades
ufw default deny incoming >/dev/null
ufw default allow outgoing >/dev/null
for port in 22 80 443; do
	ufw allow "$port/tcp" >/dev/null
done
for i in 1 2 3; do
	[ "$i" = "$n" ] && continue
	ufw allow from "${privs[$((i - 1))]}" to any port 50051 proto tcp >/dev/null
done
ufw --force enable >/dev/null
ufw status
systemctl enable --now unattended-upgrades >/dev/null 2>&1 || true

echo "==> Start everything"
systemctl daemon-reload
systemctl enable --now raftra-chaos.timer raftra-reset.timer
systemctl enable raftra caddy
systemctl restart raftra caddy

sleep 2
echo
echo "Local status: $(curl -fsS --max-time 2 http://127.0.0.1:8001/status || echo 'not answering yet')"
echo "Done. Public URL: https://raftra-n$n.duckdns.org/status (certificate can take ~1 min)"
