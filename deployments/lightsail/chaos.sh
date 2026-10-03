#!/usr/bin/env bash
# Runs on every node at :04:55, :14:55, ... :54:55. At the next full minute
# (:05:00, :15:00, ...) the current leader SIGKILLs its own raftra-server, like
# pulling the power plug.
#
# The three nodes' timers fire up to ~1 s apart, and a new leader is elected in
# ~0.2-0.3 s. So a node only acts if it is leader at :05:00 in the SAME term it
# saw at :04:55: the leader elected right after the kill has a newer term and
# is spared, so exactly one node dies per round.
set -euo pipefail

status() { curl -fsS --max-time 2 http://127.0.0.1:8001/status 2>/dev/null || true; }
term() { sed -nE 's/.*"term":([0-9]+).*/\1/p'; }

before=$(status | term)
# Sleep until the full minute; never more than 10 s if the timer fired late.
sleep "$(awk -v s="$(date +%S.%N)" 'BEGIN { d = 60 - s; if (d > 10) d = 0; printf "%.3f", d }')"
now=$(status)

if echo "$now" | grep -q '"is_leader":true'; then
	if [ -n "$before" ] && [ "$(echo "$now" | term)" = "$before" ]; then
		logger -t raftra-chaos "this node is the leader; sending SIGKILL (systemd restarts it in 45 s)"
		systemctl kill --signal=SIGKILL raftra
	else
		logger -t raftra-chaos "this node became leader during this chaos round (term $before -> $(echo "$now" | term)); sparing it"
	fi
fi
