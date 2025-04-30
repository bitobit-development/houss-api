# clients/sunsynk/gateways.py
import requests
from .client import SunsynkClient

class GatewayAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def list(self, page=1, limit=10, lan="en"):
        return requests.get(
            f"{self.BASE_URL}/gateways",
            params={"page": page, "limit": limit, "lan": lan},
            headers=self._get_headers(),
        ).json()

    @SunsynkClient.ensure_token
    def count(self):
        return requests.get(
            f"{self.BASE_URL}/gateways/count",
            headers=self._get_headers(),
        ).json()