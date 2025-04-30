# clients/sunsynk/events.py
import requests
from .client import SunsynkClient

class EventAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def list(self, **params):
        return requests.get(
            f"{self.BASE_URL}/events",
            params=params,
            headers=self._get_headers(),
        ).json()