# clients/sunsynk/inverters.py
# -----------------------------------------------------------------------------
# High‑level wrapper for Sunsynk inverter endpoints.
# Existing methods:
#   list()           – GET /inverters
#   list_by_plant()  – GET /plant/{id}/inverters
#   realtime_output  – GET /inverter/{sn}/realtime/output
# NEW:
#   count()          – GET /inverters/count (aggregate status counters)
# -----------------------------------------------------------------------------

import requests
from .client import SunsynkClient

class InverterAPI(SunsynkClient):
    """API client for Sunsynk inverter resources."""

    # ------------------------------------------------------------------
    # Aggregate counters (total / normal / offline / warning / fault ...)
    # ------------------------------------------------------------------
    @SunsynkClient.ensure_token
    def count(self) -> dict:
        """Return overall inverter summary for the authenticated account."""
        response = requests.get(
            f"{self.BASE_URL}/inverters/count",
            headers=self._get_headers(),
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Paginated list
    # ------------------------------------------------------------------
    @SunsynkClient.ensure_token
    def list(self, page: int = 1, limit: int = 10, lan: str = "en") -> dict:
        response = requests.get(
            f"{self.BASE_URL}/inverters",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Inverters scoped to a plant
    # ------------------------------------------------------------------
    @SunsynkClient.ensure_token
    def list_by_plant(
        self,
        plant_id: int,
        page: int = 1,
        limit: int = 10,
        status: int = -1,
        sn: str = "",
        type: int = -2,
        lan: str = "en",
    ) -> dict:
        url = f"{self.BASE_URL}/plant/{plant_id}/inverters"
        params = {
            "page": page,
            "limit": limit,
            "status": status,
            "sn": sn,
            "id": plant_id,
            "type": type,
            "lan": lan,
        }
        response = requests.get(url, params=params, headers=self._get_headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Real‑time telemetry
    # ------------------------------------------------------------------
    @SunsynkClient.ensure_token
    def realtime_output(self, sn: str) -> dict:
        response = requests.get(
            f"{self.BASE_URL}/inverter/{sn}/realtime/output",
            headers=self._get_headers(),
            timeout=15,
        )
        response.raise_for_status()
        return response.json()
