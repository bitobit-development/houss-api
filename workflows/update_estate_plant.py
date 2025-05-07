#!/usr/bin/env python3
# workflows/update_estate_plant.py
"""Synchronise Sunsynk plants to Supabase `public.estate_plant`.

Key behaviour
-------------
* INSERT new rows owned by the current user (`user_id`) so that RLS passes.
* UPDATE only rows we already own (or all rows when running with a service‑role key).
* Quiet by default – per‑row chatter demoted to DEBUG.
* `LOG_LEVEL` env var or `--verbose/--quiet` CLI flag controls verbosity.
* Designed to be called from GitHub Actions on an hourly cron without flooding logs.
"""

import os
import math
import logging
import argparse
from datetime import datetime

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

def _serialize(model: EstatePlant) -> dict:
    """Convert Pydantic model to a plain dict suitable for Supabase."""
    data = model.model_dump()
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    if CURRENT_USER_ID:
        data["user_id"] = CURRENT_USER_ID
    return data

# ─────────────────────────────────────────────
# Main workflow
# ─────────────────────────────────────────────

def update_estate_plant() -> int:
    """Insert / update plants. Return number of new rows inserted."""
    api = PlantAPI(username=USERNAME, password=PASSWORD)

    first_page = api.list(page=1, limit=30)
    meta = first_page["data"]
    total, page_size = meta["total"], meta["pageSize"]
    pages = math.ceil(total / page_size)
    logger.info("Sunsynk plants total=%d  pages=%d", total, pages)

    new_rows, failed_rows = 0, 0

    for page in range(1, pages + 1):
        resp = api.list(page=page, limit=page_size)
        infos = resp["data"]["infos"]
        logger.info("Page %d/%d – %d plants", page, pages, len(infos))

        for raw in infos:
            plant_model = EstatePlant.model_validate(raw)
            payload = _serialize(plant_model)

            try:
                # Fast path: attempt INSERT
                supabase.table("estate_plant").insert(payload, upsert=False).execute()
                new_rows += 1
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
                logger.debug("Updated plant %d (%s)", plant_model.id, plant_model.name)
            except Exception as upd_exc:
                failed_rows += 1
                logger.warning("Update failed plant %d: %s", plant_model.id, upd_exc)

    logger.info(
        "Sync complete. new=%d  failed=%d  remaining=%d",
        new_rows,
        failed_rows,
        total - new_rows,
    )
    return new_rows

# ─────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Sunsynk plants → Supabase.")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Set log level to DEBUG",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Set log level to WARNING",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.quiet:
        logger.setLevel(logging.WARNING)

    inserted = update_estate_plant()
    print(f"Inserted {inserted} new estate_plant records.")

# Example:  python -m workflows.update_estate_plant -q
