#!/usr/bin/env bash
# update-config.sh — run from your laptop to copy this folder's scripts and
# systemd units to all three nodes (deploy.sh only updates the binary).
# Timers restart right away; raftra.service changes apply on its next restart.
#   RAFTRA_SSH_KEY=~/.ssh/LightsailDefaultKey-ap-south-1.pem ./update-config.sh
set -euo pipefail
key=${RAFTRA_SSH_KEY:?set RAFTRA_SSH_KEY to the Lightsail .pem key}
cd "$(dirname "$0")"

scripts="install-server.sh chaos.sh reset.sh"
units="raftra.service raftra-chaos.service raftra-chaos.timer raftra-reset.service raftra-reset.timer"

for i in 1 2 3; do
	host="raftra-n$i.duckdns.org"
	echo "==> $host"
	scp -q -i "$key" -o StrictHostKeyChecking=accept-new $scripts $units "ubuntu@$host:/tmp/"
	ssh -i "$key" "ubuntu@$host" "
		cd /tmp &&
		sudo install -m 755 $scripts /usr/local/lib/raftra/ &&
		sudo install -m 644 $units /etc/systemd/system/ &&
		rm -f $scripts $units &&
		sudo systemctl daemon-reload &&
		sudo systemctl restart raftra-chaos.timer raftra-reset.timer &&
		systemctl list-timers 'raftra-*' --no-pager | head -3"
done
echo "Config updated on all nodes."
