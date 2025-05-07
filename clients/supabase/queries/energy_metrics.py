# clients/supabase/queries/energy_metrics.py
# ──────────────────────────────────────────────────────────────────────────────
"""
Typed helper for the `public.fetch_hourly_energy_metrics()` RPC.

SQL definition (2025-05-03):

    create or replace function public.fetch_hourly_energy_metrics(
      _estate_ids  int[],            -- required
      _day_offset  int default 0     -- optional, 0 = today
    ) returns table (
      estate_id   int,
      hour        int,
      sample_time timestamptz,
      etoday      numeric,
      efficiency  numeric,
      etotal      numeric
    )

This wrapper:

* Accepts keyword-only arguments (`estate_ids`, `day_offset=0`).
* Calls the RPC with a **single JSON object**, so PostgREST matches by **name**
  (argument order no longer matters).
* Converts Supabase client errors → `RuntimeError` for FastAPI to handle.
"""

from __future__ import annotations

from typing import List, TypedDict, Any
from clients.supabase.client import supabase


class HourlyMetric(TypedDict):
    estate_id:   int
    hour:        int
    sample_time: str    # ISO 8601
    etoday:      float
    efficiency:  float
    etotal:      float


def fetch_hourly_energy_metrics(
    *,
    estate_ids: List[int] | tuple[int, ...],
    day_offset: int = 0,
) -> List[HourlyMetric]:
    """
    Return hourly energy metrics for one or more estates.

    Parameters
    ----------
    estate_ids : list[int] | tuple[int, ...]
        Residential-estate IDs to include in the query.
    day_offset : int, default 0
        0 = today, 1 = yesterday, 2 = two days ago … (UTC+2 boundary inside SQL).

    Returns
    -------
    list[HourlyMetric]
        One row per estate × hour bucket, sorted by estate, hour.
    """
    # Payload must match SQL arg names exactly:
    payload: dict[str, Any] = {
        "_estate_ids": estate_ids,
        "_day_offset": day_offset,
    }

    resp = supabase.rpc("fetch_hourly_energy_metrics", payload).execute()

    if getattr(resp, "error", None):
        raise RuntimeError(resp.error.message)

    # resp.data is already list[dict]; let TypedDict inform the IDE
    return resp.data  # type: ignore[return-value]
