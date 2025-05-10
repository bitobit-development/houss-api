#!/usr/bin/env python3
# workflows/update_plant_power.py
"""
Synchronise Sunsynk 10-minute power data into Supabase `plant_power_10min`.

Changes (May 10 2025)
---------------------
* **Always `INSERT`** instead of upsert.
* Silently **skip duplicates** (PostgreSQL error 23505 – unique-constraint
  violation) so the job never fails when a row already exists.
* Retains exponential-back-off retry logic and concise logging.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import time
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List

import postgrest.exceptions as pgerr
import pytz
from clients.sunsynk.plants import PlantAPI
from clients.supabase.client import session
from clients.supabase.tables.plant_power_10min import insert_point

# ════════════════════════════════════════════════════════════════════════════
# Logging
# ════════════════════════════════════════════════════════════════════════════

def _configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(__name__)

log = _configure_logging(os.getenv("LOG_LEVEL", "INFO").upper())

# ════════════════════════════════════════════════════════════════════════════
# Environment / Auth
# ════════════════════════════════════════════════════════════════════════════

USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("Set SUNSYNK_USERNAME and SUNSYNK_PASSWORD in env vars")

SA_TZ = pytz.timezone("Africa/Johannesburg")

# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def _retry_call(
    func: Callable[..., Any],
    *args: Any,
    retries: int = 3,
    backoff: float = 1.5,
    **kwargs: Any,
) -> Any:
    """Retry a call with exponential back-off."""

    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > retries:
                log.error("%s failed after %d attempts: %s", func.__name__, retries, exc)
                raise
            delay = backoff**attempt
            log.warning(
                "%s failed (attempt %d/%d): %s – retrying in %.1fs",
                func.__name__,
                attempt,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)


def _base_row(pid: int, ts_iso: str, metric: str, value: float) -> Dict[str, Any]:
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

# ════════════════════════════════════════════════════════════════════════════
# Row builders
# ════════════════════════════════════════════════════════════════════════════


def _rows_energy(pid: int, channel: Dict[str, Any]) -> List[Dict[str, Any]]:
    metric = channel.get("label", "unknown")
    today = date.today()
    rows: List[Dict[str, Any]] = []
    for rec in channel.get("records", []):
        hh, mm = map(int, rec["time"].split(":"))
        local_dt = datetime.combine(today, datetime.min.time()).replace(hour=hh, minute=mm)
        utc_ts = SA_TZ.localize(local_dt).astimezone(timezone.utc).isoformat()
        rows.append(_base_row(pid, utc_ts, metric, float(rec["value"])))
    return rows


def _rows_realtime(pid: int, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    now_iso = (
        datetime.utcnow().replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
    )
    mapping = {
        "pac": "PV",
        "battery": "Battery",
        "load": "Load",
        "grid": "Grid",
        "soc": "SOC",
    }
    return [
        _base_row(pid, now_iso, metric, float(snap[key]))
        for key, metric in mapping.items()
        if key in snap
    ]

# ════════════════════════════════════════════════════════════════════════════
# Safe insert helper
# ════════════════════════════════════════════════════════════════════════════

def _safe_insert(row: Dict[str, Any]) -> None:
    """Insert a row; ignore duplicate-key errors (23505)."""

    try:
        insert_point(row)
    except pgerr.APIError as exc:  # type: ignore[attr-defined]
        if "23505" in str(exc):
            log.debug("Duplicate row ignored: %r", row)
            # row already exists – skip
        else:
            raise

# ════════════════════════════════════════════════════════════════════════════
# Main ingest
# ════════════════════════════════════════════════════════════════════════════

def ingest(mode: str = "energy") -> int:
    """Fetch from Sunsynk and write to Supabase. Returns row-count."""

    # 1. Authenticate to Sunsynk
    try:
        api = _retry_call(lambda: PlantAPI(username=USERNAME, password=PASSWORD), retries=5, backoff=2)
    except Exception as exc:  # noqa: BLE001
        log.error("Sunsynk authentication failed: %s", exc)
        return 0

    # 2. Build full list of plant IDs (paginated)
    first = _retry_call(api.list, page=1, limit=100)
    if first.get("code") != 0 or "data" not in first:
        log.error("Unexpected list response (page 1): %s", first)
        return 0

    meta = first["data"]
    total, page_size = meta.get("total", 0), meta.get("pageSize", 100)
    pages = max(1, math.ceil(total / page_size))
    plants = [p["id"] for p in meta.get("infos", [])]

    for pg in range(2, pages + 1):
        try:
            resp = _retry_call(api.list, page=pg, limit=page_size)
            if resp.get("code") == 0 and "infos" in resp.get("data", {}):
                plants.extend(p["id"] for p in resp["data"]["infos"])
            else:
                log.warning("Skipping malformed page %d: %s", pg, resp)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to fetch page %d: %s", pg, exc)

    log.info("Found %d plants across %d pages", len(plants), pages)

    # 3. Ingest per plant id
    
    total_rows = 0
    for pid in plants:
        try:
            resp = _retry_call(api.energy if mode == "energy" else api.realtime, plant_id=pid)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fetch failed for plant %d: %s", pid, exc)
            continue

        if resp.get("code") != 0 or "data" not in resp:
            log.warning("Bad response for plant %d: %s", pid, resp)
            continue

        rows: List[Dict[str, Any]]
        if mode == "energy":
            rows = sum((_rows_energy(pid, ch) for ch in resp["data"].get("infos", [])), [])
        else:
            rows = _rows_realtime(pid, resp["data"])

        if not rows:
            continue

        for row in rows:
            if total_rows == 0:
                log.debug("Processing first row: %r", row)
            _safe_insert(row)
            total_rows += 1

    log.info("Ingest complete – %d new rows written", total_rows)
    return total_rows

# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Sync Sunsynk power → Supabase")
    ap.add_argument("--mode", choices=["energy", "realtime"], default="energy",
                    help="Choose 'energy' (default) or 'realtime'")
    ap.add_argument("-v", "--verbose", action="store_true", help="DEBUG logs")
    ap.add_argument("-q", "--quiet", action="store_true", help="WARNING logs")
    args = ap.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    elif args.quiet:
        log.setLevel(logging.WARNING)

    log.info("Session user_id=%s", getattr(session.user, "id", None))
    rows = ingest(args.mode)
    print(f"Upserted {rows} rows from {args.mode} mode.")