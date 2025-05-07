#!/usr/bin/env python3
# workflows/update_plant_inverters.py
"""
Upserts all Sunsynk inverters for every plant into Supabase `public.inverters`.

Run options
-----------
--quiet / --ignore-failures   Suppress DEBUG logs and always exit 0
--debug                       Show DEBUG logs even if DEBUG env var is not set
"""

from __future__ import annotations

import os
import sys
import time
import argparse
import logging
from typing import Dict, Any

from clients.sunsynk.inverters import InverterAPI
from clients.supabase.client   import supabase, session

# ────────────────────────── CLI & env ──────────────────────────
def get_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upsert Sunsynk inverters→Supabase")
    p.add_argument("--quiet", action="store_true",
                   help="Hide DEBUG output and never fail the pipeline")
    p.add_argument("--ignore-failures", action="store_true",
                   help="Alias for --quiet (kept for clarity)")
    p.add_argument("--debug", action="store_true",
                   help="Force DEBUG output even if DEBUG env is not set")
    return p.parse_args()

ARGS     = get_args()
QUIET    = ARGS.quiet or ARGS.ignore_failures
DEBUG    = (ARGS.debug or os.getenv("DEBUG", "false").lower() in {"1", "true"}) and not QUIET

# ───────────────────────── logging ─────────────────────────
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname).1s %(message)s",
    force=True,        # our settings supersede any previous config
)
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
    INSERT first; on duplicate, UPDATE. Returns 'inserted' | 'updated' | 'failed'.
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
        **({"user_id": USER_ID} if USER_ID else {}),
    }

    # INSERT path
    try:
        supabase.table("inverters").insert(payload, upsert=False).execute()
        log.debug("inserted %s", payload["sn"])
        return "inserted"
    except Exception as exc:
        if "duplicate key value" not in str(exc):
            log.error("insert %s failed: %s", payload["sn"], exc)
            return "failed"

    # UPDATE path
    try:
        upd = payload.copy()
        upd.pop("id", None)                       # id immutable
        (
            supabase.table("inverters")
            .update(upd)
            .eq("id", payload["id"])
            .eq("user_id", USER_ID)               # satisfy RLS, if present
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
        plant_rows = supabase.table("estate_plant").select("id").execute().data or []
    except Exception as exc:
        log.error("failed to fetch plants: %s", exc)
        sys.exit(1 if not QUIET else 0)

    inserted = updated = failed = 0

    for row in plant_rows:
        pid = row["id"]
        log.debug("plant %s", pid)

        try:
            resp = api.list_by_plant(plant_id=pid)
            if not resp or "data" not in resp:
                raise ValueError("empty response")
            infos = resp["data"].get("infos", []) or []
        except Exception as exc:
            log.error("fetch inverters plant %s error: %s", pid, exc)
            failed += 1
            continue

        for inv in infos:
            match upsert_inverter(inv, pid):
                case "inserted":
                    inserted += 1
                case "updated":
                    updated += 1
                case _:
                    failed += 1

    dur = time.perf_counter() - start_ts
    log.info(
        "inverters ➜ inserted=%d updated=%d failed=%d plants=%d duration=%.1fs",
        inserted, updated, failed, len(plant_rows), dur,
    )

    if failed and not QUIET:
        sys.exit(1)


if __name__ == "__main__":
    main()
