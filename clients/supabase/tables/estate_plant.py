"""
Supabase helpers for the `estate_plant` table.

Includes:
• EstatePlant Pydantic model
• insert_estate_plant()
• get_estate_plant()            – paginated list + exact total
• get_estate_plant_totals()     – kW / kWh / counts / efficiency
• get_offline_plants()          – detail list for modal
"""

from datetime import datetime
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict
from clients.supabase.client import supabase

# ──────────────────────────────────────────────────────────────────────────
# Pydantic model (matches table schema)
# ──────────────────────────────────────────────────────────────────────────
class EstatePlant(BaseModel):
    id: int
    name: str
    thumb_url: str = Field(..., alias="thumbUrl")
    status: int
    address: str
    pac: float
    efficiency: float
    etoday: float
    etotal: float
    update_at: datetime = Field(..., alias="updateAt")
    create_at: datetime = Field(..., alias="createAt")
    type: int
    master_id: int = Field(..., alias="masterId")
    share: bool
    exist_camera: bool = Field(..., alias="existCamera")
    email: str
    phone: Optional[str]
    product_warranty_registered: int = Field(
        ..., alias="productWarrantyRegistered"
    )
    user_id: Optional[str] = None  # UUID of owner (RLS)

    model_config = ConfigDict(populate_by_name=True)

# ──────────────────────────────────────────────────────────────────────────
# Insert helper
# ──────────────────────────────────────────────────────────────────────────
def insert_estate_plant(plant: EstatePlant) -> dict:
    """Insert one row; returns the inserted record."""
    payload = plant.model_dump()
    for k, v in payload.items():
        if isinstance(v, datetime):
            payload[k] = v.isoformat()

    resp = supabase.table("estate_plant").insert(payload).execute()
    rows = getattr(resp, "data", None)
    if not rows:
        raise RuntimeError(f"Insert failed: {resp}")
    return rows[0]

# ──────────────────────────────────────────────────────────────────────────
# Paginated fetch with total-count
# ──────────────────────────────────────────────────────────────────────────
def get_estate_plant(page: int = 1, page_size: int = 30) -> Dict[str, Any]:
    if page < 1 or page_size < 1:
        raise ValueError("page and page_size must be positive integers")

    start = (page - 1) * page_size
    end   = start + page_size - 1  # inclusive

    resp = (
        supabase
        .table("estate_plant")
        .select(
            """
            id, name, thumb_url, status, address, pac,
            efficiency, etoday, etotal,
            residential_estates:estate_id (
                id,
                estate_name,
                physical_address,
                estate_type,
                estate_description,
                estate_area
            )
            """,
            count="exact"
        )
        .order("name")
        .range(start, end)
        .execute()
    )

    data   = getattr(resp, "data", None)
    total  = getattr(resp, "count", None)
    error  = getattr(resp, "error", None)

    if error or data is None or total is None:
        raise RuntimeError(error.message if error else "Unknown Supabase error")

    return {
        "rows":       data,
        "total":      total,
        "pageSize":   page_size,
        "pageNumber": page,
    }

# ──────────────────────────────────────────────────────────────────────────
# Estate-level KPI totals (with online/offline counts)
# ──────────────────────────────────────────────────────────────────────────
def get_estate_plant_totals(estate_id: int) -> dict:
    resp = (
        supabase
        .table("estate_plant")
        .select("pac, etoday, etotal, status, efficiency, update_at")
        .eq("estate_id", estate_id)
        .execute()
    )

    rows  = getattr(resp, "data", None) or []
    error = getattr(resp, "error", None)
    if error:
        raise RuntimeError(error.message)

    # ── numeric aggregates ─────────────────────────────────────
    total_w       = sum(r["pac"]    or 0 for r in rows)
    total_today   = sum(r["etoday"] or 0 for r in rows)
    total_total   = sum(r["etotal"] or 0 for r in rows)
    offline_count = sum(1 for r in rows if r.get("status") == 0)
    online_count  = len(rows) - offline_count

    efficiency_pct = (
        sum((r.get("efficiency") or 0) for r in rows) / len(rows) * 100
        if rows else 0
    )

    # ── most-recent update_at (ISO string) ─────────────────────
    latest_iso = None
    if rows:
        # ISO-8601 strings sort chronologically, so max() gives newest
        latest_iso = max(r["update_at"] for r in rows if r.get("update_at"))

    return {
        "total_kw":       total_w / 1000,
        "total_today":    total_today,
        "total_total":    total_total,
        "offline_count":  offline_count,
        "online_count":   online_count,
        "efficiency_pct": efficiency_pct,
        "last_update":    latest_iso,   # ← now the newest update_at
    }

# ──────────────────────────────────────────────────────────────────────────
# Offline plants list (for modal)
# ──────────────────────────────────────────────────────────────────────────
def get_offline_plants(estate_id: int) -> List[dict]:
    resp = (
        supabase
        .table("estate_plant")
        .select("id, name, pac, etoday, etotal")
        .eq("estate_id", estate_id)
        .eq("status", 0)
        .order("name")
        .execute()
    )

    data  = getattr(resp, "data", None) or []
    error = getattr(resp, "error", None)
    if error:
        raise RuntimeError(error.message)
    return data
