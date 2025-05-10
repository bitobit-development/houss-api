#!/usr/bin/env python3
# workflows/update_plant_power.py
"""
Synchronise Sunsynk 10‑minute power data into Supabase `plant_power_10min`.

Changes (May 10 2025)
---------------------
* Switch to **single global bulk insert** (default 1 000 row chunks) after
  collecting data from all plants – reduces HTTP/2 streams from tens of
  thousands to < 20.
* Silently skip duplicate‑key violations (23505) and log summary only.
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
from clients.supabase.client import session, supabase  # assuming supabase object

PLANT_TABLE = "plant_power_10min"

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
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > retries:
                raise
            log.warning("%s failed (%d/%d): %s – retrying in %.1fs",
                        func.__name__, attempt, retries, exc, backoff**attempt)
            time.sleep(backoff**attempt)


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


def _rows_energy(pid: int, channel: Dict[str, Any]) -> List[Dict[str, Any]]:
    metric = channel.get("label", "unknown")
    today = date.today()
    rows = []
    for rec in channel.get("records", []):
        hh, mm = map(int, rec["time"].split(":"))
        local_dt = datetime.combine(today, datetime.min.time()).replace(hour=hh, minute=mm)
        utc_ts = SA_TZ.localize(local_dt).astimezone(timezone.utc).isoformat()
        rows.append(_base_row(pid, utc_ts, metric, float(rec["value"])))
    return rows


def _rows_realtime(pid: int, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    now_iso = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
    mapping = {"pac": "PV", "battery": "Battery", "load": "Load", "grid": "Grid", "soc": "SOC"}
    return [_base_row(pid, now_iso, metric, float(snap[k])) for k, metric in mapping.items() if k in snap]

# ════════════════════════════════════════════════════════════════════════════
# Bulk insert
# ════════════════════════════════════════════════════════════════════════════

def _bulk_insert(rows: List[Dict[str, Any]], chunk: int = 1000) -> int:
    if not rows:
        return 0

    inserted = 0
    tbl = supabase.table(PLANT_TABLE)
    for i in range(0, len(rows), chunk):
        chunk_rows = rows[i : i + chunk]
        try:
            tbl.insert(chunk_rows, upsert=False, count="exact", ignore_duplicates=True).execute()
            inserted += len(chunk_rows)
        except pgerr.APIError as exc:  # type: ignore[attr-defined]
            if "23505" in str(exc):
                # fallback: skip duplicates
                log.debug("Duplicate(s) in batch %d–%d skipped", i, i + chunk)
            else:
                raise
    return inserted

# ════════════════════════════════════════════════════════════════════════════
# Main ingest
# ════════════════════════════════════════════════════════════════════════════

def ingest(mode: str = "energy") -> int:
    api = _retry_call(lambda: PlantAPI(username=USERNAME, password=PASSWORD), retries=5, backoff=2)

    # collect plant ids
    first = _retry_call(api.list, page=1, limit=100)
    meta = first["data"]
    total, page_size = meta.get("total", 0), meta.get("pageSize", 100)
    pages = max(1, math.ceil(total / page_size))
    plants = [p["id"] for p in meta.get("infos", [])]
    for pg in range(2, pages + 1):
        resp = _retry_call(api.list, page=pg, limit=page_size)
        if resp.get("code") == 0:
            plants.extend(p["id"] for p in resp["data"].get("infos", []))

    all_rows: List[Dict[str, Any]] = []

    for pid in plants:
        resp = _retry_call(api.energy if mode == "energy" else api.realtime, plant_id=pid)
        if resp.get("code") != 0 or "data" not in resp:
            continue
        rows = (sum((_rows_energy(pid, ch) for ch in resp["data"].get("infos", [])), [])
                if mode == "energy" else _rows_realtime(pid, resp["data"]))
        all_rows.extend(rows)

    inserted = _bulk_insert(all_rows)
    log.info("Inserted %d/%d rows", inserted, len(all_rows))
    return inserted

# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["energy", "realtime"], default="energy")
    args = ap.parse_args()

    ingest(args.mode)
