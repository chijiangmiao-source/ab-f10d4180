#!/usr/bin/env python3
"""Container health check: exit 0 iff /health answers status=ok."""

import json
import os
import sys
import urllib.request

port = os.environ.get("APP_PORT", "8080")
try:
    with urllib.request.urlopen(
        f"http://127.0.0.1:{port}/health", timeout=2
    ) as resp:
        body = json.loads(resp.read().decode())
    sys.exit(0 if body.get("status") == "ok" else 1)
except Exception:
    sys.exit(1)
