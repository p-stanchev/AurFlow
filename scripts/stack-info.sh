#!/usr/bin/env bash
set -euo pipefail

host=${STACK_HOST:-localhost}

cat <<EOF
ORLB API:           http://$host:8080
Prometheus UI:      http://$host:9090
Alertmanager UI:    http://$host:9093
Grafana UI:         http://$host:3000

Credentials
-----------
Grafana: admin / admin
EOF
