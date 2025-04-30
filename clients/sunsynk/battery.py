# clients/sunsynk/battery.py
import requests
from .client import SunsynkClient

class BatteryAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def realtime(self, sn: str):
        return requests.get(
            f"{self.BASE_URL}/inverter/battery/{sn}/realtime",
            headers=self._get_headers(),
        ).json()

    # add day/month/year/total methods similarly

# clients/sunsynk/load.py
from .client import SunsynkClient

class LoadAPI(SunsynkClient):
    @SunsynkClient.ensure_token
    def realtime(self, sn: str):
        return requests.get(
            f"{self.BASE_URL}/inverter/load/{sn}/realtime",
            headers=self._get_headers(),
        ).json()

    # add day/month/year/total methods similarly
