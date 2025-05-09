# clients/sunsynk/client.py

import time
import threading
import logging
from functools import wraps

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

class SunsynkClient:
    """
    Robust Sunsynk API client with automatic retry, token refresh, and backoff.
    """

    BASE_URL = "https://api.sunsynk.net/api/v1"
    AUTH_URL = "https://api.sunsynk.net/oauth/token"
    DEFAULT_TIMEOUT = 10
    AUTH_RETRIES = 5
    REQUEST_RETRIES = 3
    BACKOFF_FACTOR = 0.5
    STATUS_FORCELIST = (500, 502, 503, 504)

    def __init__(
        self,
        username: str,
        password: str,
        client_id: str = "csp-web",
        source: str = "sunsynk",
    ):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.source = source

        # thread‐safe lock for refreshing
        self._lock = threading.Lock()

        # session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=self.REQUEST_RETRIES,
            status_forcelist=self.STATUS_FORCELIST,
            backoff_factor=self.BACKOFF_FACTOR,
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # auth tokens
        self.access_token: str = ""
        self.refresh_token: str = ""
        self.token_expiry: float = 0

        # initial authentication
        self._authenticate_with_retry()

    def _authenticate_with_retry(self) -> None:
        """
        Attempt to authenticate up to AUTH_RETRIES times with exponential backoff.
        """
        for attempt in range(1, self.AUTH_RETRIES + 1):
            try:
                self._authenticate()
                log.info("Sunsynk authentication succeeded on attempt %d", attempt)
                return
            except Exception as exc:
                log.warning(
                    "Sunsynk auth attempt %d/%d failed: %s",
                    attempt, self.AUTH_RETRIES, exc
                )
                time.sleep(self.BACKOFF_FACTOR * (2 ** (attempt - 1)))
        raise RuntimeError(f"Failed to authenticate after {self.AUTH_RETRIES} attempts")

    def _authenticate(self) -> None:
        """
        Perform password grant authentication.
        """
        payload = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "client_id": self.client_id,
            "source": self.source,
            "areaCode": "sunsynk",
        }
        resp = self.session.post(self.AUTH_URL, json=payload, timeout=self.DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        # expire slightly before actual expiry
        self.token_expiry = time.time() + data.get("expires_in", 0) - 10

    def _refresh_token(self) -> None:
        """
        Refresh the access token using the refresh token.
        """
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "source": self.source,
        }
        resp = self.session.post(self.AUTH_URL, json=payload, timeout=self.DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = time.time() + data.get("expires_in", 0) - 10
        log.info("Sunsynk token refreshed, next expiry at %s", time.ctime(self.token_expiry))

    @staticmethod
    def ensure_token(func):
        """
        Decorator to refresh token if expired before making API calls.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with self._lock:
                if time.time() >= self.token_expiry:
                    log.info("Access token expired, refreshing...")
                    self._refresh_token()
            return func(self, *args, **kwargs)
        return wrapper

    def _get_headers(self) -> dict:
        """
        Standard headers including Bearer token.
        """
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    @ensure_token
    def _request(self, method: str, path: str, **kwargs) -> dict:
        """
        Internal helper for GET/POST to Sunsynk API.
        """
        url = f"{self.BASE_URL}{path}"
        headers = kwargs.pop("headers", {})
        headers.update(self._get_headers())
        resp = self.session.request(
            method, url, headers=headers, timeout=self.DEFAULT_TIMEOUT, **kwargs
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("code") != 0:
            msg = body.get("msg") or body
            raise RuntimeError(f"Sunsynk API error on {path}: {msg!r}")
        return body.get("data", {})

    def list(self, page: int = 1, pageSize: int = 100) -> dict:
        """
        List all plants, paginated.
        """
        return self._request("GET", f"/plant?page={page}&pageSize={pageSize}")

    @ensure_token
    def energy(self, plant_id: int) -> dict:
        """
        Fetch 10-minute energy records for a plant.
        """
        return self._request("GET", f"/plant/{plant_id}/energy")

    @ensure_token
    def realtime(self, plant_id: int) -> dict:
        """
        Fetch realtime snapshot for a plant.
        """
        return self._request("GET", f"/plant/{plant_id}/realtime")
