# clients/supabase/tables/residential_estates.py
# -----------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, ConfigDict
from clients.supabase.client import supabase


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic model – mirrors the DB structure
# ─────────────────────────────────────────────────────────────────────────────
class ResidentialEstate(BaseModel):
    id:               Optional[int]      = None  # generated identity
    created_at:       Optional[datetime] = None  # default now()
    estate_name:      Optional[str]      = None
    physical_address: Optional[str]      = None
    estate_type:      Optional[str]      = None
    estate_description: Optional[str]    = None
    estate_area:      Optional[str]      = None

    # allow extra keys from Supabase without validation errors
    model_config = ConfigDict(extra='ignore')


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_structure():
    """Return column definitions for residential_estates."""
    return (
        supabase
        .table('information_schema.columns')
        .select('column_name,data_type,is_nullable,column_default')
        .eq('table_name', 'residential_estates')
        .execute()
    )


def get_all_residential_estates():
    """Fetch all rows."""
    return (
        supabase
        .table('residential_estates')
        .select('*')
        .order('id', desc=False)
        .execute()
    )


def insert_residential_estate(data: Dict[str, Any] | ResidentialEstate):
    """Insert one row and return it."""
    payload = (
        data.model_dump(exclude_none=True)
        if isinstance(data, ResidentialEstate) else data
    )
    return (
        supabase
        .table('residential_estates')
        .insert(payload)
        .execute()
    )


def update_residential_estate(estate_id: int, data: Dict[str, Any] | ResidentialEstate):
    """Update one row by PK and return it."""
    payload = (
        data.model_dump(exclude_none=True)
        if isinstance(data, ResidentialEstate) else data
    )
    return (
        supabase
        .table('residential_estates')
        .update(payload)
        .eq('id', estate_id)
        .execute()
    )


def delete_residential_estate(estate_id: int):
    """Delete one row by PK."""
    return (
        supabase
        .table('residential_estates')
        .delete()
        .eq('id', estate_id)
        .execute()
    )
