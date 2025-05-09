#!/usr/bin/env python3
# workflows/update_estate_plant.py
"""Synchronise Sunsynk plants to Supabase `public.estate_plant`.

Improvements (2025‑05‑09)
-------------------------
* Robust login & retry logic (see try/except + `_retry_call` helper).
* Graceful handling of malformed Sunsynk responses.
* Per‑run summary now reports **new**, **updated**, and **failed** counts.
* Quieter logs: HTTPX, urllib3, etc. set to WARNING.
* CLI ``--verbose`` / ``--quiet`` retained.
"""

import os
import math
import time
import logging
import argparse
from datetime import datetime
from typing import Any, Dict, List, Tuple

from clients.sunsynk.plants import PlantAPI
from clients.supabase.client import supabase, session
from clients.supabase.tables.estate_plant import EstatePlant

# ─────────────────────────────────────────────
# Logging configuration
# ─────────────────────────────────────────────

def _configure_logging(level: str) -> logging.Logger:
    """Centralised logging config. Returns module logger."""
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Silence extra‑chatty third‑party loggers
    for name in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)

    return logging.getLogger(__name__)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = _configure_logging(LOG_LEVEL)

# ─────────────────────────────────────────────
# Environment / auth
# ─────────────────────────────────────────────
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("SUNSYNK_USERNAME / SUNSYNK_PASSWORD are required")

CURRENT_USER_ID = getattr(session.user, "id", None) if session else None
logger.debug("estate_plant sync user_id=%s", CURRENT_USER_ID)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _serialize(model: EstatePlant) -> Dict[str, Any]:
    """Convert Pydantic model to a plain dict suitable for Supabase."""
    data = model.model_dump()
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    if CURRENT_USER_ID:
        data["user_id"] = CURRENT_USER_ID
    return data


def _retry_call(func, *args, retries: int = 3, backoff: float = 1.5, **kwargs):
    """Retry helper with exponential back‑off."""
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            attempt += 1
            if attempt > retries:
                raise
            sleep = backoff ** attempt
            logger.warning("%s failed (attempt %d/%d): %s – retrying in %.1fs",
                           func.__name__, attempt, retries, exc, sleep)
            time.sleep(sleep)


# ─────────────────────────────────────────────
# Main workflow
# ─────────────────────────────────────────────

def update_estate_plant() -> Tuple[int, int]:
    """Insert / update plants.

    Returns
    -------
    tuple
        (inserted_count, updated_count)
    """

    # ── Authenticate ─────────────────────────
    try:
        api = PlantAPI(username=USERNAME, password=PASSWORD)
    except Exception as auth_exc:
        logger.error("Failed to authenticate with Sunsynk API: %s", auth_exc)
        return (0, 0)

    # ── Fetch first page (for metadata) ──────
    try:
        first_page = _retry_call(api.list, page=1, limit=30)
    except Exception as exc:
        logger.error("Sunsynk API unreachable: %s", exc)
        return (0, 0)

    if first_page.get("code") != 0 or "data" not in first_page:
        logger.error("Unexpected response from Sunsynk API (page 1): %s", first_page)
        return (0, 0)

    meta = first_page["data"]
    total, page_size = meta.get("total", 0), meta.get("pageSize", 30)
    pages = max(1, math.ceil(total / page_size))
    logger.info("Sunsynk plants total=%d  pages=%d", total, pages)

    inserted_rows, updated_rows, failed_rows = 0, 0, 0

    # ── Iterate through pages ────────────────
    for page in range(1, pages + 1):
        try:
            resp = _retry_call(api.list, page=page, limit=page_size)
        except Exception as exc:
            logger.warning("Failed to fetch page %d: %s", page, exc)
            failed_rows += page_size
            continue

        if resp.get("code") != 0 or "data" not in resp or "infos" not in resp["data"]:
            logger.warning("Malformed response page %d: %s", page, resp)
            failed_rows += page_size
            continue

        infos: List[dict] = resp["data"]["infos"]
        logger.info("Page %d/%d – %d plants", page, pages, len(infos))

        for raw in infos:
            try:
                plant_model = EstatePlant.model_validate(raw)
                payload = _serialize(plant_model)
            except Exception as val_exc:
                logger.warning("Validation failed for plant raw=%s – %s", raw, val_exc)
                failed_rows += 1
                continue

            try:
                # Fast path: attempt INSERT
                supabase.table("estate_plant").insert(payload, upsert=False).execute()
                inserted_rows += 1
                logger.debug("Inserted plant %d (%s)", plant_model.id, plant_model.name)
                continue
            except Exception as ins_exc:
                # Ignore duplicate key errors, log other issues
                if "duplicate key value" not in str(ins_exc):
                    failed_rows += 1
                    logger.warning("Insert error plant %d: %s", plant_model.id, ins_exc)
                    continue  # skip – don't attempt update

            # Existing row – perform guarded UPDATE
            try:
                update_payload = payload.copy()
                update_payload.pop("id", None)  # keep PK immutable
                q = (
                    supabase.table("estate_plant")
                    .update(update_payload)
                    .eq("id", plant_model.id)
                )
                if CURRENT_USER_ID:
                    q = q.eq("user_id", CURRENT_USER_ID)
                q.execute()
                updated_rows += 1
                logger.debug("Updated plant %d (%s)", plant_model.id, plant_model.name)
            except Exception as upd_exc:
                failed_rows += 1
                logger.warning("Update failed plant %d: %s", plant_model.id, upd_exc)

    logger.info(
        "Sync complete. new=%d  updated=%d  failed=%d",
        inserted_rows, updated_rows, failed_rows
    )
    return inserted_rows, updated_rows


# ─────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Sunsynk plants → Supabase.")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Set log level to DEBUG"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Set log level to WARNING"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.quiet:
        logger.setLevel(logging.WARNING)

    inserted, updated = update_estate_plant()
    print(f"Inserted {inserted}, updated {updated} estate_plant records.")

# Example:  python -m workflows.update_estate_plant -q
