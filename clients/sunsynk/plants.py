# clients/sunsynk/plants.py
# -----------------------------------------------------------------------------
# Sunsynk plant API wrapper — now uses /plant/energy/{id}/day endpoint.
# -----------------------------------------------------------------------------

import requests
from datetime import datetime
from typing import Optional
import pytz

from .client import SunsynkClient

TENANT_UID = 344476  # fixed tenant for HOUSS
SA_TZ = pytz.timezone("Africa/Johannesburg")


class PlantAPI(SunsynkClient):
    """High‑level helper around Sunsynk REST plant endpoints."""

    # ────────────────────────────────
    # Collections
    # ────────────────────────────────
    @SunsynkClient.ensure_token
    def list(self, page: int = 1, limit: int = 30, lan: str = "en"):
        return requests.get(
            f"{self.BASE_URL}/plants",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    @SunsynkClient.ensure_token
    def count(self):
        return requests.get(
            f"{self.BASE_URL}/user/{TENANT_UID}/plantCount",
            params={"id": TENANT_UID},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    # ────────────────────────────────
    # Single plant
    # ────────────────────────────────
    @SunsynkClient.ensure_token
    def detail(self, plant_id: int, lan: str = "en"):
        return requests.get(
            f"{self.BASE_URL}/plant/{plant_id}",
            params={"lan": lan},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    @SunsynkClient.ensure_token
    def realtime(self, plant_id: int, lan: str = "en"):
        return requests.get(
            f"{self.BASE_URL}/plant/{plant_id}/realtime",
            params={"lan": lan, "id": plant_id},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    # ---- Day chart (10‑minute resolution) ------------------------------------
    @SunsynkClient.ensure_token
    def energy(self, plant_id: int, date_str: Optional[str] = None, lan: str = "en"):
        """Fetch /plant/energy/{plant_id}/day"""
        if date_str is None:
            date_str = datetime.now(SA_TZ).strftime("%Y-%m-%d")

        params = {
            "lan": lan,
            "date": date_str,
            "id": plant_id,
        }
        return requests.get(
            f"{self.BASE_URL}/plant/energy/{plant_id}/day",
            params=params,
            headers=self._get_headers(),
            timeout=20,
        ).json()
