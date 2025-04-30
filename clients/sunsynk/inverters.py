# clients/sunsynk/inverters.py
import requests
from .client import SunsynkClient


class InverterAPI(SunsynkClient):
    """
    API client for interacting with Sunsynk inverter endpoints.
    """
    @SunsynkClient.ensure_token
    def list(self, page: int = 1, limit: int = 10, lan: str = "en") -> dict:
        """
        Get a paginated list of all inverters.
        """
        response = requests.get(
            f"{self.BASE_URL}/inverters",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
        )
        response.raise_for_status()
        return response.json()

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
        """
        Get inverters for a specific plant by plant_id.

        :param plant_id: ID of the plant to fetch inverters for
        :param page:     Page number (default: 1)
        :param limit:    Number of items per page (default: 10)
        :param status:   Inverter status filter (-1 for all)
        :param sn:       Serial number filter (empty for all)
        :param type:     Inverter type filter (-2 for all)
        :param lan:      Language code (default: "en")
        :returns:        JSON response dict
        """
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
        response = requests.get(url, params=params, headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    @SunsynkClient.ensure_token
    def realtime_output(self, sn: str) -> dict:
        """
        Get real-time output data for a specific inverter.
        """
        response = requests.get(
            f"{self.BASE_URL}/inverter/{sn}/realtime/output",
            headers=self._get_headers(),
        )
        response.raise_for_status()
        return response.json()
