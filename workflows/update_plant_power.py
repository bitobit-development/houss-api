#!/usr/bin/env python3
# workflows/update_plant_power.py
"""
Synchronise Sunsynk 10-minute power data into Supabase `plant_power_10min`.

This workflow:
 1. Authenticates to Sunsynk and pages through all plants.
 2. Fetches either 10-minute energy or realtime data.
 3. Buffers all rows, then bulk-inserts in 500-row chunks.
 4. Uses the unified `supabase` client (carries JWT or service key).
 5. Re-authenticates to Sunsynk every 25 min.
 6. Refreshes Supabase user session whenever chunk insert begins if older than 25 min.
 7. Silently skips duplicate-key errors (PostgreSQL 23505). test
"""

from __future__ import annotations
import os
import math
import time
import logging
import argparse
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List

import pytz
import postgrest.exceptions as pgerr
from clients.sunsynk.plants import PlantAPI
from clients.supabase.client import supabase, session, refresh_session

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
PLANT_TABLE = "plant_power_10min"
CHUNK_SIZE = int(os.getenv("BULK_CHUNK", "500"))
REAUTH_INTERVAL = 25 * 60  # 25 minutes in seconds
SA_TZ = pytz.timezone("Africa/Johannesburg")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("SUNSYNK_USERNAME and SUNSYNK_PASSWORD must be set")

# track last supabase session refresh
supabase_last_refresh = time.time()

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    for lib in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(lib).setLevel(logging.WARNING)

_configure_logging()
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _retry_call(
    func: Callable[..., Any], *args: Any,
    retries: int = 3, backoff: float = 1.5, **kwargs: Any
) -> Any:
    """Retry a function with exponential backoff."""
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            attempt += 1
            if attempt > retries:
                log.error("%s failed after %d attempts: %s", func.__name__, retries, exc)
                raise
            delay = backoff ** attempt
            log.warning(
                "%s failed (%d/%d): %s; retrying in %.1fs",
                func.__name__, attempt, retries, exc, delay
            )
            time.sleep(delay)

# ──────────────────────────────────────────────────────────────────────────────
# Row builders
# ──────────────────────────────────────────────────────────────────────────────

def _base_row(pid: int, ts_iso: str, metric: str, value: float) -> Dict[str, Any]:
    return {
        "plant_id": pid,
        "ts": ts_iso,
        "metric": metric,
        "value": value,
        "user_id": session.user.id if session else None,
    }


def _rows_energy(pid: int, channel: Dict[str, Any]) -> List[Dict[str, Any]]:
    metric = channel.get("label", "unknown")
    today = date.today()
    rows: List[Dict[str, Any]] = []
    for rec in channel.get("records", []):
        hh, mm = map(int, rec["time"].split(':'))
        local_dt = datetime.combine(today, datetime.min.time()).replace(hour=hh, minute=mm)
        utc_ts = SA_TZ.localize(local_dt).astimezone(timezone.utc).isoformat()
        rows.append(_base_row(pid, utc_ts, metric, float(rec["value"])))
    return rows


def _rows_realtime(pid: int, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    now_iso = datetime.utcnow().replace(second=0, microsecond=0,
                                        tzinfo=timezone.utc).isoformat()
    mapping = {"pac": "PV", "battery": "Battery", "load": "Load",
               "grid": "Grid", "soc": "SOC"}
    return [_base_row(pid, now_iso, m, float(snap[k]))
            for k, m in mapping.items() if k in snap]

# ──────────────────────────────────────────────────────────────────────────────
# Refresh Supabase session helper
# ──────────────────────────────────────────────────────────────────────────────

def _maybe_refresh_supabase() -> None:
    """Refresh Supabase JWT if the last refresh was over the interval."""
    global supabase_last_refresh
    if not session:
        return
    now = time.time()
    if now - supabase_last_refresh < REAUTH_INTERVAL:
        return
    try:
        refresh_session()
        supabase_last_refresh = now
        log.info("Supabase session refreshed")
    except Exception as exc:
        log.warning("Failed to refresh Supabase session: %s", exc)

# ──────────────────────────────────────────────────────────────────────────────
# Bulk insert
# ──────────────────────────────────────────────────────────────────────────────

def _insert_chunk(rows: List[Dict[str, Any]]) -> int:
    """Insert a chunk; skip duplicates and refresh session if needed."""
    if not rows:
        return 0
    _maybe_refresh_supabase()
    try:
        supabase.table(PLANT_TABLE).insert(rows, upsert=False).execute()
        return len(rows)
    except pgerr.APIError as exc:
        msg = str(exc)
        if "23505" in msg:
            count = 0
            for r in rows:
                _maybe_refresh_supabase()
                try:
                    supabase.table(PLANT_TABLE).insert(r, upsert=False).execute()
                    count += 1
                except pgerr.APIError as e2:
                    if "23505" in str(e2):
                        continue
                    raise
            return count
        raise

# ──────────────────────────────────────────────────────────────────────────────
# Main ingest workflow
# ──────────────────────────────────────────────────────────────────────────────

def ingest(mode: str = "energy") -> int:
    # Authenticate Sunsynk
    api = _retry_call(lambda: PlantAPI(username=USERNAME, password=PASSWORD), retries=5)
    first = _retry_call(api.list, page=1, limit=100)
    infos = first.get("data", {}).get("infos", [])
    total = first.get("data", {}).get("total", 0)
    size = first.get("data", {}).get("pageSize", 100)
    pages = max(1, math.ceil(total / size))
    plants = [p["id"] for p in infos]
    for pg in range(2, pages + 1):
        resp = _retry_call(api.list, page=pg, limit=size)
        plants.extend([p["id"] for p in resp.get("data", {}).get("infos", [])])
    log.info("Found %d plants over %d pages", len(plants), pages)

    # Fetch data rows
    all_rows: List[Dict[str, Any]] = []
    sk_start = time.time()
    for pid in plants:
        # Refresh Sunsynk auth periodically
        if time.time() - sk_start > REAUTH_INTERVAL:
            api = _retry_call(lambda: PlantAPI(username=USERNAME, password=PASSWORD), retries=3)
            sk_start = time.time()

        resp = _retry_call(
            api.energy if mode == "energy" else api.realtime,
            plant_id=pid
        )
        if resp.get("code") != 0 or "data" not in resp:
            continue
        data = resp["data"]
        if mode == "energy":
            for ch in data.get("infos", []):
                all_rows.extend(_rows_energy(pid, ch))
        else:
            all_rows.extend(_rows_realtime(pid, data))

    log.info("Collected %d rows total", len(all_rows))

    # Bulk insert rows in chunks
    inserted = 0
    for i in range(0, len(all_rows), CHUNK_SIZE):
        batch = all_rows[i:i+CHUNK_SIZE]
        inserted += _insert_chunk(batch)
    log.info("Ingest complete: inserted %d of %d rows", inserted, len(all_rows))
    return inserted

# ──────────────────────────────────────────────────────────────────────────────
# CLI entrypoint
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Sunsynk power → Supabase")
    parser.add_argument("--mode", choices=["energy", "realtime"], default="energy")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)
    elif args.quiet:
        log.setLevel(logging.WARNING)
    log.info("Session user_id=%s", getattr(session.user, "id", None) if session else None)
    count = ingest(args.mode)
    print(f"Inserted {count} rows in {args.mode} mode.")
