#!/usr/bin/env python3
"""
Populate `public.estate_plant_daily_report` with a daily snapshot of every
row in `estate_plant`.

• Adds `user_id` automatically when running under an authenticated Supabase
  session (so rows pass RLS).
• Skips rows that violate unique constraints or RLS.
• Designed for GitHub Actions: all credentials come from the environment.

Run locally with

    python -m workflows.update_estate_plant_daily_report
"""

from __future__ import annotations

import os
import math
import logging
from typing import Any, Dict, List

from clients.supabase.client import supabase, session
from clients.supabase.tables.estate_plant_daily_report import insert_daily_report

# ───────────────────────────── logging ──────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────── env / runtime config ───────────────────────
PAGE_SIZE = int(os.getenv("SNAPSHOT_PAGE_SIZE", "1000"))

# current user (None when using the service-role key in CI)
USER_ID = getattr(session.user, "id", None) if session else None
log.debug("estate_plant_daily_report user_id=%s", USER_ID)

# ───────────────────────── helpers ──────────────────────────────────
def build_payload(src: Dict[str, Any]) -> Dict[str, Any]:
    """Map an estate_plant row → insert-ready payload."""
    payload: Dict[str, Any] = {
        "plant_id":   src["id"],
        "name":       src["name"],
        "status":     src["status"],
        "pac":        float(src["pac"]        or 0),
        "efficiency": float(src["efficiency"] or 0),
        "etoday":     float(src["etoday"]     or 0),
        "etotal":     float(src["etotal"]     or 0),
        "update_at":  src["update_at"],
        "create_at":  src["create_at"],
        "type":       src["type"],
        "master_id":  src["master_id"],
        "estate_id":  src["estate_id"],
    }
    if USER_ID:
        payload["user_id"] = USER_ID
    return payload


def fetch_page(offset: int, limit: int) -> List[Dict[str, Any]]:
    """Fetch a slice of estate_plant rows (only required columns)."""
    resp = (
        supabase
        .table("estate_plant")
        .select(
            """
            id, name, status, pac, efficiency, etoday, etotal,
            update_at, create_at, type, master_id, estate_id
            """
        )
        .range(offset, offset + limit - 1)        # PostgREST inclusive
        .execute()
    )
    return getattr(resp, "data", []) or []

# ───────────────────────── main workflow ────────────────────────────
def main() -> None:
    # Determine total row count once (head request with count)
    total_resp = (
        supabase
        .table("estate_plant")
        .select("id", count="exact")
        .limit(0)        # HEAD-like: no data
        .execute()
    )
    total_rows = getattr(total_resp, "count", 0)
    pages      = math.ceil(total_rows / PAGE_SIZE) if total_rows else 0
    # log.info("estate_plant rows=%d → pages=%d (page_size=%d)", total_rows, pages, PAGE_SIZE)

    inserted = skipped = 0
    for page in range(pages):
        offset = page * PAGE_SIZE
        rows   = fetch_page(offset, PAGE_SIZE)

        for raw in rows:
            try:
                insert_daily_report(build_payload(raw))
                inserted += 1
            except Exception:
                skipped += 1

        # log.info("Processed page %d/%d", page + 1, pages)

    log.info("estate_plant_daily_report ➜ inserted %d, skipped %d", inserted, skipped)


if __name__ == "__main__":
    main()
