# clients/supabase/tables/estate_plant.py

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from clients.supabase.client import supabase


class EstatePlant(BaseModel):
    """
    Pydantic model for a Sunsynk plant, matching the `estate_plant` table schema.
    """
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
    product_warranty_registered: int = Field(..., alias="productWarrantyRegistered")
    user_id: Optional[str] = None   # ← new (UUID string)

    # Allow population by both field names and aliases
    model_config = ConfigDict(populate_by_name=True)


def insert_estate_plant(plant: EstatePlant) -> dict:
    """
    Insert an EstatePlant into Supabase `estate_plant` table.

    :param plant:   Validated EstatePlant instance
    :return:        The newly inserted row as a dict
    :raises RuntimeError: if Supabase returns no data
    """
    # Dump using field names (snake_case) so keys match your table columns
    payload = plant.model_dump()

    # Serialize any datetime values to ISO 8601 strings
    for key, val in payload.items():
        if isinstance(val, datetime):
            payload[key] = val.isoformat()

    # Perform the insert
    response = supabase.table("estate_plant").insert(payload).execute()

    # Supabase v2 client returns an object with .data but no .error
    inserted_rows = getattr(response, "data", None)
    if not inserted_rows:
        raise RuntimeError(f"Insert failed, no data returned: {response}")

    # Return the first (and only) inserted row
    return inserted_rows[0]
