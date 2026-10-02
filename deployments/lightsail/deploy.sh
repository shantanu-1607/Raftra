#!/usr/bin/env bash
# deploy.sh VERSION — run from your laptop to roll out a release, one node at a
# time, so the other two keep quorum. Needs the Lightsail SSH key:
#   RAFTRA_SSH_KEY=~/Downloads/LightsailDefaultKey-ap-south-1.pem ./deploy.sh v0.2.1
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: deploy.sh VERSION (e.g. v0.2.1)" >&2; exit 1; }
version=$1
key=${RAFTRA_SSH_KEY:?set RAFTRA_SSH_KEY to the Lightsail .pem key}

for i in 1 2 3; do
	host="raftra-n$i.duckdns.org"
	echo "==> $host: installing $version"
	ssh -i "$key" -o StrictHostKeyChecking=accept-new "ubuntu@$host" \
		"sudo /usr/local/lib/raftra/install-server.sh $version && sudo systemctl restart raftra"
	printf "    waiting for %s to answer" "$host"
	for _ in $(seq 1 30); do
		if curl -fsS --max-time 2 "https://$host/status" >/dev/null 2>&1; then
			echo " ok"
			break
		fi
		printf "."
		sleep 2
	done
	sleep 5 # let it catch up before taking down the next node
done
echo "Deployed $version to all nodes."
