# clients/sunsynk/plants.py
import requests
from .client import SunsynkClient

class PlantAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def list(self, page=1, limit=30, lan="en"):
        return requests.get(
            f"{self.BASE_URL}/plants",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
        ).json()

    @SunsynkClient.ensure_token
    def detail(self, plant_id: int, lan="en"):
        return requests.get(
            f"{self.BASE_URL}/plant/{plant_id}",
            params={"lan": lan},
            headers=self._get_headers(),
        ).json()

    @SunsynkClient.ensure_token
    def realtime(self, plant_id: int, lan="en"):
        return requests.get(
            f"{self.BASE_URL}/plant/{plant_id}/realtime",
            params={"lan": lan},
            headers=self._get_headers(),
        ).json()