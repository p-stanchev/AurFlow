#!/usr/bin/env bash
set -euo pipefail

ORLB_URL=${ORLB_URL:-http://localhost:8080}
PROM_URL=${PROM_URL:-http://localhost:9090}

echo "▶ Checking ORLB metrics at ${ORLB_URL}/metrics"
curl -fsS "${ORLB_URL}/metrics" >/dev/null

echo "▶ Checking Prometheus health at ${PROM_URL}/-/healthy"
curl -fsS "${PROM_URL}/-/healthy" >/dev/null

echo "▶ Checking Prometheus target health for job=orlb"
python - "$PROM_URL" <<'PY'
import json
import sys
from urllib.request import urlopen

prom = sys.argv[1]
with urlopen(f"{prom}/api/v1/targets") as resp:
    payload = json.load(resp)

active = payload.get("data", {}).get("activeTargets", [])
healthy = [
    target for target in active
    if target.get("labels", {}).get("job") == "orlb"
    and target.get("health") == "up"
]

if not healthy:
    print("No healthy Prometheus targets found for job=orlb", file=sys.stderr)
    sys.exit(1)

print(f"Found {len(healthy)} healthy Prometheus target(s) for job=orlb.")
PY

echo "✔ Smoke checks passed."
