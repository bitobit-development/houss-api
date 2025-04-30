#!/usr/bin/env python3
# workflows/update_plant_inverters.py
"""Fetch every plant, fetch its inverters from Sunsynk, and upsert them into
Supabase `public.inverters`. Adds `user_id` so RLS (`auth.uid() = user_id`) passes.
"""

import os
import logging
from datetime import datetime

from clients.sunsynk.inverters import InverterAPI
from clients.supabase.client import supabase, session  # session carries the JWT

# ─────────────────────────────────────────────────────────────
# Configure logging
# ─────────────────────────────────────────────────────────────
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Sunsynk credentials (env)
# ─────────────────────────────────────────────────────────────
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("Missing SUNSYNK_USERNAME or SUNSYNK_PASSWORD in env")

# UUID of the currently‑authenticated user (for user_id column)
CURRENT_USER_ID = getattr(session.user, "id", None) if session else None
logger.debug("Current user_id for upsert: %s", CURRENT_USER_ID)

# ─────────────────────────────────────────────────────────────
# Main workflow
# ─────────────────────────────────────────────────────────────

def update_plant_inverters() -> None:
    """Upsert all inverters for all plants into Supabase while respecting RLS.

    If the inverter row already exists but belongs to another user (different
    `user_id`), the UPDATE will fail due to the RLS USING expression.  We work
    around this by:
    1.  Attempting an INSERT first (prefer fast‑path).
    2.  On conflict error (HTTP 409 / 42501), running a separate UPDATE that
        includes `user_id = CURRENT_USER_ID` in the WHERE clause so it only
        succeeds when we are the owner.
    """

    api = InverterAPI(username=USERNAME, password=PASSWORD)

    # 1) Fetch plant IDs
    try:
        resp = supabase.table("estate_plant").select("id").execute()
        plant_rows = resp.data or []
        logger.info("Found %d plants", len(plant_rows))
    except Exception as exc:
        logger.error("Failed to fetch plants: %s", exc)
        return

    for row in plant_rows:
        plant_id = row["id"]
        logger.info("Processing plant %d", plant_id)
        try:
            inv_resp = api.list_by_plant(plant_id=plant_id)
            inverter_infos = inv_resp.get("data", {}).get("infos", [])
        except Exception as exc:
            logger.error("Error fetching inverters for plant %d: %s", plant_id, exc)
            continue

        for inv in inverter_infos:
            payload = {
                "id": inv.get("id"),
                "sn": inv.get("sn"),
                "alias": inv.get("alias"),
                "gsn": inv.get("gsn"),
                "status": inv.get("status"),
                "type": inv.get("type"),
                "comm_type_name": inv.get("commTypeName"),
                "cust_code": inv.get("custCode"),
                "version": inv.get("version"),
                "model": inv.get("model"),
                "equip_mode": inv.get("equipMode"),
                "pac": inv.get("pac"),
                "etoday": inv.get("etoday"),
                "etotal": inv.get("etotal"),
                "update_at": inv.get("updateAt"),
                "opened": inv.get("opened"),
                "gateway_vo": inv.get("gatewayVO"),
                "sunsynk_equip": inv.get("sunsynkEquip"),
                "protocol_identifier": inv.get("protocolIdentifier"),
                "equip_type": inv.get("equipType"),
                "plant_id": plant_id,
            }

            if CURRENT_USER_ID:
                payload["user_id"] = CURRENT_USER_ID

            # First try insert‑only (prefer fast path)
            try:
                res = (
                    supabase.table("inverters")
                    .insert(payload, upsert=False)  # pure insert
                    .execute()
                )
                logger.info("  Inserted inverter %s", payload["sn"])
                continue  # move to next inverter
            except Exception as insert_exc:
                # Duplicate id → fall through to update attempt
                if "duplicate key value" not in str(insert_exc):
                    logger.error("  Insert error for %s: %s", payload["sn"], insert_exc)
                    continue

            # Update path: only succeed when we already own the row
            try:
                update_payload = payload.copy()
                update_payload.pop("id")  # id not updatable
                (
                    supabase.table("inverters")
                    .update(update_payload)
                    .eq("id", payload["id"])
                    .eq("user_id", CURRENT_USER_ID)  # satisfy RLS USING
                    .execute()
                )
                logger.info("  Updated inverter %s", payload["sn"])
            except Exception as update_exc:
                logger.error("  Update failed for %s: %s", payload["sn"], update_exc)

if __name__ == "__main__":
    update_plant_inverters()
