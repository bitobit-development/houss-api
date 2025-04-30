#!/usr/bin/env python3
"""Synchronise Sunsynk plants with Supabase `public.estate_plant`.

• Inserts new rows with `user_id` so they pass RLS (`auth.uid() = user_id`).
• If a plant already exists, only UPDATE when we already own the row.
• Works as‑is when run under a service‑role key (then RLS is bypassed and
  `user_id` is omitted).
"""

import os
import math
import logging
from datetime import datetime

from clients.sunsynk.plants import PlantAPI
from clients.supabase.client import supabase, session
from clients.supabase.tables.estate_plant import EstatePlant

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Sunsynk creds (env)
# ─────────────────────────────────────────────────────────────
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("SUNSYNK_USERNAME / SUNSYNK_PASSWORD not set")

# UUID of current user when authenticated; None if running under service key
CURRENT_USER_ID = getattr(session.user, "id", None) if session else None
logger.debug("estate_plant sync user_id=%s", CURRENT_USER_ID)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _serialize(model: EstatePlant) -> dict:
    """Convert Pydantic model to DB‑ready dict (ISO datetimes)."""
    data = model.model_dump()
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()
    if CURRENT_USER_ID:
        data["user_id"] = CURRENT_USER_ID
    return data

# ─────────────────────────────────────────────────────────────
# Main workflow
# ─────────────────────────────────────────────────────────────

def update_estate_plant() -> int:
    """Insert / update estate plants, returning count of new inserts."""

    api = PlantAPI(username=USERNAME, password=PASSWORD)

    first = api.list(page=1, limit=30)
    meta = first["data"]
    total = meta["total"]
    page_size = meta["pageSize"]
    pages = math.ceil(total / page_size)
    logger.info("Sunsynk plants total=%d, pages=%d", total, pages)

    new_rows = 0

    for page in range(1, pages + 1):
        resp = api.list(page=page, limit=page_size)
        infos = resp["data"]["infos"]
        logger.info("Page %d/%d – %d plants", page, pages, len(infos))

        for raw in infos:
            plant_model = EstatePlant.model_validate(raw)
            payload = _serialize(plant_model)

            # Try pure INSERT first (fast‑path)
            try:
                supabase.table("estate_plant").insert(payload, upsert=False).execute()
                new_rows += 1
                logger.info("  Inserted plant %d (%s)", plant_model.id, plant_model.name)
                continue
            except Exception as ins_exc:
                if "duplicate key value" not in str(ins_exc):
                    logger.error("  Insert error plant %d: %s", plant_model.id, ins_exc)
                    continue  # skip

            # Attempt conditional UPDATE (only if we own the row)
            try:
                update_payload = payload.copy()
                update_payload.pop("id", None)  # ID immutable
                q = supabase.table("estate_plant").update(update_payload).eq("id", plant_model.id)
                if CURRENT_USER_ID:
                    q = q.eq("user_id", CURRENT_USER_ID)
                q.execute()
                logger.info("  Updated plant %d (%s)", plant_model.id, plant_model.name)
            except Exception as upd_exc:
                logger.error("  Update failed plant %d: %s", plant_model.id, upd_exc)

    logger.info("Sync complete. New inserts: %d", new_rows)
    return new_rows


if __name__ == "__main__":
    inserted = update_estate_plant()
    print(f"Inserted {inserted} new estate_plant records.")

# python -m workflows.update_estate_plant
