#!/usr/bin/env python3
# workflows/get_plant_power.py
"""Print rows from public.plant_power_10min and insert a sample point.

Usage examples
--------------
python -m workflows.get_plant_power              # all rows + insert sample
python -m workflows.get_plant_power --limit 20   # first 20 rows + insert sample
python -m workflows.get_plant_power -v           # verbose logging
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import List, Dict, Any

from clients.supabase.client import session
from clients.supabase.tables.plant_power_10min import get_all_points, insert_point , upsert_point

# ── Logging setup ────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
for noisy in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ── Main helper ──────────────────────────────────────────────────────────────

def run(limit: int | None = None) -> None:
    """Fetch existing rows, print them, then insert a sample point."""
    uid = getattr(session.user, "id", None)
    log.info("Authenticated as %s", uid or "<service-role>")

    # Fetch and print rows
    try:
        resp = get_all_points(limit)
    except Exception as exc:
        log.error("Error fetching data from Supabase: %s", exc)
        return

    rows: List[Dict[str, Any]] = resp.data if hasattr(resp, 'data') else []
    if not rows:
        print("<no rows>")
    else:
        for row in rows:
            print(row)

    # Insert sample point
    sample = {
        "id": "60144fc9-4177-46c0-bca0-15a24e80ca87",
        "plant_id": 482711,
        "ts": "2025-05-08T22:00:00+00:00",
        "metric": "PV",
        "value": 1.0,
        "inserted_at": "2025-05-09T13:18:55.209911+00:00",
        "user_id": uid,
    }
    try:
        ins_resp = insert_point(sample)
    except Exception as exc:
        log.error("Error inserting sample point: %s", exc)
        return

    if hasattr(ins_resp, 'error') and ins_resp.error:
        log.error("Supabase insert error: %s", ins_resp.error)
    else:
        print("Inserted sample point:", ins_resp.data)

# ── CLI entrypoint ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print and insert sample row for plant_power_10min")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows returned")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("-q", "--quiet", action="store_true", help="Only WARNING+ logs")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    elif args.quiet:
        log.setLevel(logging.WARNING)

    run(args.limit)
