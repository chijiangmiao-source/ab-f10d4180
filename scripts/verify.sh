#!/usr/bin/env bash
# One-shot acceptance for the verify Compose service.
# Runs unit tests, build/import checks and an HTTP smoke against the app
# container. Exits non-zero on the first failure.
set -euo pipefail

cd "${WORKDIR:-/srv}"

BASE_URL="${BASE_URL:-http://app:8080}"

echo "== Python version =="
python --version

echo "== Build check: byte-compile all sources =="
python -m compileall -q app tests scripts

echo "== Build check: installed dependencies consistent =="
pip check

echo "== Build check: application imports and routes load =="
python - <<'PY'
from app.main import app
routes = {getattr(r, "path", None) for r in app.routes}
required = {"/health", "/readings", "/windows", "/windows/{window_start}",
            "/progress", "/violations"}
missing = required - routes
assert not missing, f"missing routes: {missing}"
print("routes OK:", sorted(r for r in routes if r))
PY

echo "== Wait for application health =="
for i in $(seq 1 30); do
  if python - "$BASE_URL" <<'PY'
import sys, json, urllib.request
try:
    with urllib.request.urlopen(sys.argv[1] + "/health", timeout=2) as r:
        sys.exit(0 if json.loads(r.read())["status"] == "ok" else 1)
except Exception:
    sys.exit(1)
PY
  then
    echo "app healthy"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "app did not become healthy" >&2
    exit 1
  fi
done

echo "== Unit / integration tests (retransmission, OoO sealing, recovery) =="
python -m pytest tests -q

echo "== HTTP smoke against ${BASE_URL} =="
python scripts/smoke.py "$BASE_URL"

echo "== ALL ACCEPTANCE STAGES PASSED =="
