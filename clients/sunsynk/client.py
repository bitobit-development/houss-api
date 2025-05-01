# clients/sunsynk/client.py
from functools import wraps
import requests
import time

class SunsynkClient:
    BASE_URL = "https://api.sunsynk.net/api/v1"
    AUTH_URL = "https://api.sunsynk.net/oauth/token"

    def __init__(self, username: str, password: str, client_id: str = "csp-web", source: str = "sunsynk"):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.source = source
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self._authenticate()

    def _authenticate(self):
        payload = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "client_id": self.client_id,
            "source": self.source,
            "areaCode": "sunsynk",
        }
        resp = requests.post(self.AUTH_URL, json=payload)
        # print("DEBUG login payload →", resp.status_code, resp.text)   # add this
        resp.raise_for_status()
        data = resp.json()["data"]
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = time.time() + data["expires_in"] - 10

    def _refresh(self):
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "source": self.source,
        }
        resp = requests.post(self.AUTH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()["data"]
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = time.time() + data["expires_in"] - 10

    def ensure_token(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if time.time() >= self.token_expiry:
                self._refresh()
            return func(self, *args, **kwargs)
        return wrapper

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }