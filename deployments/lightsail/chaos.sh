#!/usr/bin/env bash
# Runs on every node at :05, :15, ... :55. Only the current leader acts:
# it SIGKILLs its own raftra-server (like pulling the power plug).
set -euo pipefail
if curl -fsS --max-time 2 http://127.0.0.1:8001/status 2>/dev/null | grep -q '"is_leader":true'; then
	logger -t raftra-chaos "this node is the leader; sending SIGKILL (systemd restarts it in 45 s)"
	systemctl kill --signal=SIGKILL raftra
fi
