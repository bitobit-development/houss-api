#!/usr/bin/env python3
# workflows/update_estate_plant_daily_report.py
"""
Hourly snapshot of estate_plant → estate_plant_daily_report
Runs safely in GitHub Actions with minimal log noise.
"""

from __future__ import annotations
import os, math, logging
from typing import Any, Dict, List

# ──────────────────────── logging first! ────────────────────────────
LOG_LEVEL = os.getenv("SNAPSHOT_LOG_LEVEL", "WARNING").upper()
NUMERIC = getattr(logging, LOG_LEVEL, logging.WARNING)

logging.basicConfig(
    level=NUMERIC,
    format="%(asctime)s %(levelname)s %(message)s",
)
# If you chose WARNING/ERROR/CRITICAL, cut off everything below it
if NUMERIC >= logging.WARNING:
    logging.disable(logging.INFO)            # hides INFO & DEBUG globally

# Silence common noisy libs
for noisy in ("supabase_py", "httpcore", "httpx", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

log = logging.getLogger(__name__)
log.info("↪︎ estate_plant_daily_report snapshot started")  # shows only when LOG_LEVEL=INFO


# ───────────────────────── app imports ──────────────────────────────
# (import AFTER configuring logging so their INFO logs obey our rules)
from clients.supabase.client import supabase, session
from clients.supabase.tables.estate_plant_daily_report import insert_daily_report


# ─────────────────────── runtime knobs ──────────────────────────────
PAGE_SIZE = int(os.getenv("SNAPSHOT_PAGE_SIZE", "1000"))
USER_ID   = getattr(session.user, "id", None) if session else None


# ───────────────────────── helpers ──────────────────────────────────
def build_payload(src: Dict[str, Any]) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "plant_id":   src["id"],
        "name":       src["name"],
        "status":     src["status"],
        "pac":        float(src["pac"] or 0),
        "efficiency": float(src["efficiency"] or 0),
        "etoday":     float(src["etoday"] or 0),
        "etotal":     float(src["etotal"] or 0),
        "update_at":  src["update_at"],
        "create_at":  src["create_at"],
        "type":       src["type"],
        "master_id":  src["master_id"],
        "estate_id":  src["estate_id"],
    }
    if USER_ID:
        p["user_id"] = USER_ID
    return p


def fetch_page(offset: int, limit: int) -> List[Dict[str, Any]]:
    r = (
        supabase
        .table("estate_plant")
        .select(
            """
            id, name, status, pac, efficiency, etoday, etotal,
            update_at, create_at, type, master_id, estate_id
            """
        )
        .range(offset, offset + limit - 1)
        .execute()
    )
    return getattr(r, "data", []) or []


# ───────────────────────── main loop ────────────────────────────────
def main() -> None:
    total = (
        supabase
        .table("estate_plant")
        .select("id", count="exact")
        .limit(0)
        .execute()
        .count
        or 0
    )

    pages     = math.ceil(total / PAGE_SIZE) if total else 0
    inserted  = 0
    skipped   = 0

    for page in range(pages):
        for raw in fetch_page(page * PAGE_SIZE, PAGE_SIZE):
            try:
                insert_daily_report(build_payload(raw))
                inserted += 1
            except Exception:
                skipped += 1      # duplicates / RLS rejects / any failure

    log.warning("estate_plant_daily_report ➜ inserted %d, skipped %d", inserted, skipped)


if __name__ == "__main__":
    main()
