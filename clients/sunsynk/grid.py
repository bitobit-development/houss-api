# clients/sunsynk/grid.py
import requests
from .client import SunsynkClient

class GridAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def realtime(self, sn: str):
        return requests.get(
            f"{self.BASE_URL}/inverter/grid/{sn}/realtime",
            headers=self._get_headers(),
        ).json()

    # add day/month/year/total methods similarly