#!/usr/bin/env python3
# workflows/update_plant_inverters.py
"""
Upserts all Sunsynk inverters for every plant into Supabase `public.inverters`.

• Logs a single INFO line at start and one summary line at the end
  (inserted / updated / failed, plant count, duration).
• Set DEBUG=1 in the environment to see per-plant and per-inverter messages.
• Exits with status 1 when any inverter operation fails so CI can flag the job.
"""

from __future__ import annotations

import os
import sys
import time
import logging
from typing import Dict, Any

from clients.sunsynk.inverters import InverterAPI
from clients.supabase.client import supabase, session   # `session` carries the JWT

# ───────────────────────── logging ─────────────────────────
DEBUG = os.getenv("DEBUG", "false").lower() in {"1", "true"}

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname).1s %(message)s",
    force=True,  # ensure our configuration overrides any earlier setup
)

# Silence chatty third-party libraries unless we explicitly need their DEBUG
for noisy in ("supabase_py", "httpx", "httpcore", "urllib3", "asyncio"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ─────────────── env / runtime configuration ───────────────
USERNAME = os.getenv("SUNSYNK_USERNAME")
PASSWORD = os.getenv("SUNSYNK_PASSWORD")
if not USERNAME or not PASSWORD:
    raise RuntimeError("SUNSYNK_USERNAME and SUNSYNK_PASSWORD must be set")

USER_ID = getattr(session.user, "id", None) if session else None
log.debug("Authenticated user_id=%s", USER_ID)

# ───────────────────── helper functions ────────────────────
def upsert_inverter(raw: Dict[str, Any], plant_id: int) -> str:
    """
    INSERT first; on duplicate, UPDATE (respecting RLS).
    Returns 'inserted' | 'updated' | 'failed'.
    """
    payload = {
        "id": raw.get("id"),
        "sn": raw.get("sn"),
        "alias": raw.get("alias"),
        "gsn": raw.get("gsn"),
        "status": raw.get("status"),
        "type": raw.get("type"),
        "comm_type_name": raw.get("commTypeName"),
        "cust_code": raw.get("custCode"),
        "version": raw.get("version"),
        "model": raw.get("model"),
        "equip_mode": raw.get("equipMode"),
        "pac": raw.get("pac"),
        "etoday": raw.get("etoday"),
        "etotal": raw.get("etotal"),
        "update_at": raw.get("updateAt"),
        "opened": raw.get("opened"),
        "gateway_vo": raw.get("gatewayVO"),
        "sunsynk_equip": raw.get("sunsynkEquip"),
        "protocol_identifier": raw.get("protocolIdentifier"),
        "equip_type": raw.get("equipType"),
        "plant_id": plant_id,
    }
    if USER_ID:
        payload["user_id"] = USER_ID

    # INSERT fast path
    try:
        supabase.table("inverters").insert(payload, upsert=False).execute()
        log.debug("inserted %s", payload["sn"])
        return "inserted"
    except Exception as exc:
        if "duplicate key value" not in str(exc):
            log.error("insert %s failed: %s", payload["sn"], exc)
            return "failed"

    # UPDATE fallback
    try:
        update_payload = payload.copy()
        update_payload.pop("id", None)  # id is immutable
        (
            supabase.table("inverters")
            .update(update_payload)
            .eq("id", payload["id"])
            .eq("user_id", USER_ID)      # satisfy RLS
            .execute()
        )
        log.debug("updated %s", payload["sn"])
        return "updated"
    except Exception as exc:
        log.error("update %s failed: %s", payload["sn"], exc)
        return "failed"

# ─────────────────────── main workflow ─────────────────────
def main() -> None:
    api = InverterAPI(username=USERNAME, password=PASSWORD)
    start_ts = time.perf_counter()
    log.info("update_plant_inverters started")

    try:
        resp = supabase.table("estate_plant").select("id").execute()
        plants = resp.data or []
    except Exception as exc:
        log.error("failed to fetch plants: %s", exc)
        sys.exit(1)

    inserted = updated = failed = 0

    for row in plants:
        plant_id = row["id"]
        log.debug("plant %s", plant_id)

        try:
            inv_resp = api.list_by_plant(plant_id=plant_id)
            infos = inv_resp.get("data", {}).get("infos", [])
        except Exception as exc:
            log.error("fetch inverters plant %s error: %s", plant_id, exc)
            failed += 1
            continue

        for inv in infos:
            outcome = upsert_inverter(inv, plant_id)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated":
                updated += 1
            else:
                failed += 1

    duration = time.perf_counter() - start_ts
    log.info(
        "inverters ➜ inserted=%d updated=%d failed=%d plants=%d duration=%.1fs",
        inserted, updated, failed, len(plants), duration,
    )

    if failed:
        sys.exit(1)  # make CI fail so issues are visible


if __name__ == "__main__":
    main()
