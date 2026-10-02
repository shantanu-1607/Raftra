#!/usr/bin/env bash
# Runs on every node at :00:00. Every node stops and wipes its data, and none
# starts again before :00:30, so no node with the old log can re-replicate it.
set -euo pipefail
systemctl stop raftra
find /var/lib/raftra -mindepth 1 -delete
m=$(date +%-M); s=$(date +%-S)
if [ "$m" -eq 0 ] && [ "$s" -lt 30 ]; then
	sleep $((30 - s))
fi
systemctl start raftra
logger -t raftra-reset "playground data wiped and node restarted"
