# clients/sunsynk/workdata.py
import requests
from .client import SunsynkClient

class WorkDataAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def list(self, **params):
        return requests.get(
            f"{self.BASE_URL}/workdata/dynamic",
            params=params,
            headers=self._get_headers(),
        ).json()