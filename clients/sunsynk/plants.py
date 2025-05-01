# clients/sunsynk/plants.py
# -----------------------------------------------------------------------------
# Thin wrapper around Sunsynk REST endpoints.
# Methods:
#   list(page)      – GET /plants
#   count()         – GET /user/344476/plantCount (summary counters)
#   detail(id)      – GET /plant/{id}
#   realtime(id)    – GET /plant/{id}/realtime
# -----------------------------------------------------------------------------

import requests
from .client import SunsynkClient

# Sunsynk tenant UID is fixed for this project
TENANT_UID = 344476

class PlantAPI(SunsynkClient):
    """High-level access to Sunsynk plant endpoints."""

    # ------------------------------------------------------------------
    # Collections / summaries
    # ------------------------------------------------------------------
    @SunsynkClient.ensure_token
    def list(self, page: int = 1, limit: int = 30, lan: str = "en"):
        """Paginated list of plants (30 per page by default)."""
        return requests.get(
            f"{self.BASE_URL}/plants",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    @SunsynkClient.ensure_token
    def count(self):
        """Return aggregate counters (total / normal / offline / warnings …).

        Endpoint is fixed to the HOUSS account (UID 344476).
        """
        return requests.get(
            f"{self.BASE_URL}/user/{TENANT_UID}/plantCount",
            params={"id": TENANT_UID},
            headers=self._get_headers(),
            timeout=15,
        ).json()

    # ------------------------------------------------------------------
    # Single-plant detail / telemetry
    # ------------------------------------------------------------------
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
            params={"lan": lan},
            headers=self._get_headers(),
            timeout=15,
        ).json()
