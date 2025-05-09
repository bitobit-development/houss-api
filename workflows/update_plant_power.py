#!/usr/bin/env python3
# workflows/update_plant_power.py
"""
Synchronise Sunsynk 10-minute power data into Supabase `plant_power_10min`.

Improvements:
* Robust login & retry via `_retry_call`.
* Paginated plant listing using `limit` and `page`.
* Graceful retries on transient API failures.
* Detailed logging of inserted rows.
"""

import os
import math
import time
import logging
import argparse
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Callable

import pytz
from clients.sunsynk.plants import PlantAPI
from clients.supabase.client import session
from clients.supabase.tables.plant_power_10min import insert_point

# ──────────────────────────────────────────────────────────────────────────────
# Logging configuration
# ──────────────────────────────────────────────────────────────────────────────
def _configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    for name in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return logging.getLogger(__name__)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
log = _configure_logging(LOG_LEVEL)

# ──────────────────────────────────────────────────────────────────────────────
# Environment / Auth
# ──────────────────────────────────────────────────────────────────────────────
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("Environment variables SUNSYNK_USERNAME and SUNSYNK_PASSWORD are required")

SA_TZ = pytz.timezone("Africa/Johannesburg")

# ──────────────────────────────────────────────────────────────────────────────
# Retry helper
# ──────────────────────────────────────────────────────────────────────────────
def _retry_call(func: Callable[..., Any], *args, retries: int = 3,
                backoff: float = 1.5, **kwargs) -> Any:
    """
    Retry `func(*args, **kwargs)` with exponential backoff.
    """
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
            log.warning("%s failed (attempt %d/%d): %s – retrying in %.1fs",
                        func.__name__, attempt, retries, exc, delay)
            time.sleep(delay)

# ──────────────────────────────────────────────────────────────────────────────
# Row constructors
# ──────────────────────────────────────────────────────────────────────────────
def base_row(pid: int, ts_iso: str, metric: str, value: float) -> Dict[str, Any]:
    uid = getattr(session.user, "id", None)
    if not uid:
        raise RuntimeError("Authenticated Supabase session missing user_id")
    return {
        "plant_id": pid,
        "ts": ts_iso,
        "metric": metric,
        "value": value,
        "user_id": uid,
    }

def rows_energy(pid: int, channel: Dict[str, Any]) -> List[Dict[str, Any]]:
    metric = channel.get("label", "unknown")
    today = date.today()
    rows: List[Dict[str, Any]] = []
    for rec in channel.get("records", []):
        hh, mm = map(int, rec["time"].split(":"))
        local_dt = datetime.combine(today, datetime.min.time()).replace(hour=hh, minute=mm)
        utc_ts = SA_TZ.localize(local_dt).astimezone(timezone.utc).isoformat()
        rows.append(base_row(pid, utc_ts, metric, float(rec["value"])))
    return rows

def rows_realtime(pid: int, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    now_iso = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
    mapping = {"pac": "PV", "battery": "Battery", "load": "Load", "grid": "Grid", "soc": "SOC"}
    return [
        base_row(pid, now_iso, metric, float(snap[key]))
        for key, metric in mapping.items() if key in snap
    ]

# ──────────────────────────────────────────────────────────────────────────────
# Main ingest workflow
# ──────────────────────────────────────────────────────────────────────────────
def ingest(mode: str = "energy") -> int:
    """
    Fetch data from Sunsynk and upsert into Supabase.
    Returns number of rows upserted.
    """
    # Authenticate (with retry)
    try:
        api = _retry_call(lambda: PlantAPI(username=USERNAME, password=PASSWORD), retries=5, backoff=2)
    except Exception as exc:
        log.error("Authentication failed: %s", exc)
        return 0

    # Fetch first page to get meta
    first = _retry_call(api.list, page=1, limit=100)
    if first.get("code") != 0 or "data" not in first:
        log.error("Unexpected list response (page 1): %s", first)
        return 0
    meta = first["data"]
    total, page_size = meta.get("total", 0), meta.get("pageSize", 100)
    pages = max(1, math.ceil(total / page_size))
    log.info("Total plants=%d across %d pages", total, pages)

    # Collect plant IDs
    plants = [p["id"] for p in meta.get("infos", [])]
    for pg in range(2, pages + 1):
        try:
            resp = _retry_call(api.list, page=pg, limit=page_size)
            if resp.get("code") != 0 or "data" not in resp or "infos" not in resp["data"]:
                log.warning("Skipping malformed page %d: %s", pg, resp)
                continue
            plants.extend(p["id"] for p in resp["data"]["infos"])
        except Exception as exc:
            log.warning("Failed to fetch page %d: %s", pg, exc)

    # Ingest data per plant
    total_rows = 0
    for pid in plants:
        try:
            resp = _retry_call(api.energy if mode == "energy" else api.realtime, plant_id=pid)
        except Exception as exc:
            log.warning("Fetch failed for plant %d: %s", pid, exc)
            continue
        if resp.get("code") != 0 or "data" not in resp:
            log.warning("Bad response for plant %d: %s", pid, resp)
            continue

        rows = (sum((rows_energy(pid, ch) for ch in resp["data"].get("infos", [])), [])
                if mode == "energy" else rows_realtime(pid, resp["data"]))
        if not rows:
            continue

        for row in rows:
            if total_rows == 0:
                log.debug("Inserting first row: %r", row)
            insert_point(row)
            total_rows += 1

    log.info("Ingest complete – total rows inserted=%d", total_rows)
    return total_rows

# ──────────────────────────────────────────────────────────────────────────────
# CLI entrypoint
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Sunsynk power → Supabase")
    parser.add_argument("--mode", choices=["energy", "realtime"], default="energy",
                        help="Choose 'energy' or 'realtime'")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logs")
    parser.add_argument("-q", "--quiet", action="store_true", help="WARNING logs")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    elif args.quiet:
        log.setLevel(logging.WARNING)

    log.info("Session user_id=%s", getattr(session.user, "id", None))
    count = ingest(args.mode)
    print(f"Upserted {count} rows from {args.mode} mode.")
