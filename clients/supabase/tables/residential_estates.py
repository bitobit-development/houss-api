# clients/supabase/tables/residential_estates.py

from typing import Optional
from pydantic import BaseModel
from clients.supabase.client import supabase

class EstateIn(BaseModel):
    """Schema for creating or updating a residential estate."""
    # replace these with your real column names + types:
    name: str
    address: str
    num_units: Optional[int] = None
    active: Optional[bool] = True

def get_structure():
    """Retrieve the column definitions for the `residential_estates` table."""
    return (
        supabase
        .from_('information_schema.columns')
        .select('column_name,data_type,is_nullable,column_default')
        .eq('table_name', 'residential_estates')
        .execute()
    )

def get_all_residential_estates():
    """Fetch all rows from the `residential_estates` table."""
    return (
        supabase
        .from_('residential_estates')
        .select('*')
        .execute()
    )

def insert_residential_estate(data: dict):
    """Insert a new record into the `residential_estates` table."""
    return (
        supabase
        .from_('residential_estates')
        .insert(data)
        .execute()
    )

def update_residential_estate(estate_id: int, data: dict):
    """Update an existing `residential_estates` record."""
    return (
        supabase
        .from_('residential_estates')
        .update(data)
        .eq('id', estate_id)
        .execute()
    )

def delete_residential_estate(estate_id: int):
    """Delete a `residential_estates` record by its primary key."""
    return (
        supabase
        .from_('residential_estates')
        .delete()
        .eq('id', estate_id)
        .execute()
    )
