# clients/clickatell/clickatell_client.py
# ──────────────────────────────────────────────────────────────────────────────
"""
Typed helper for sending a **single SMS** through Clickatell’s *Platform* API.

Endpoint used  (2025-05-07):
    POST https://platform.clickatell.com/v1/message

Environment variable required:
    CLICKATELL_API_KEY   – your Platform API key (starts with `CAPI-...`)

Design notes
------------
* Synchronous helper – matches the blocking style of other `clients/` utilities.
* Converts any Clickatell / network error → `RuntimeError` so FastAPI or the
  caller can decide how to map it to HTTP responses.
* Phone numbers are normalised to South-African MSISDN (`27xxxxxxxxx`) but the
  helper will happily accept numbers that already start with `27` or `+27`.
"""

from __future__ import annotations

import os
import re
import requests
from typing import TypedDict, Any, Final
from pydantic import BaseModel          

# ── Publicly re-exported symbols for easy import ─────────────────────────────
__all__ = ["SmsPayload", "send_sms"]

# ── Pydantic request payload (exported) ──────────────────────────────────────
class SmsPayload(BaseModel):
    phone: str      # e.g. "0821234567" or "+27821234567"
    message: str    # body text (≈160 chars for single-part SMS)

# ── Constants & configuration ────────────────────────────────────────────────
CLICKATELL_API_URL: Final[str] = "https://platform.clickatell.com/v1/message"
CLICKATELL_API_KEY: Final[str | None] = os.getenv("CLICKATELL_API_KEY")

if not CLICKATELL_API_KEY:
    raise RuntimeError(
        "CLICKATELL_API_KEY environment variable is not set.  "
        "Export it in your shell or add it to Replit Secrets."
    )

HEADERS: Final[dict[str, str]] = {
    "Content-Type": "application/json",
    "Authorization": CLICKATELL_API_KEY,
}


# ── Type hints for IDE autocomplete ───────────────────────────────────────────
class ClickatellSuccess(TypedDict):
    messages: list[dict[str, Any]]  # messageId, acceptedTimestamp, etc.


class ClickatellError(TypedDict):
    error: str
    errorCode: int | None


# ── Helper functions ─────────────────────────────────────────────────────────
_PHONE_RE = re.compile(r"\D+")


def _format_sa_msisdn(number: str) -> str:
    """
    Convert common SA local formats → Clickatell-ready MSISDN (27XXXXXXXXX).

    * Strips all non-digits.
    * If the result starts with '27', returns as-is.
    * If it starts with a single leading '0', replace it with '27'.
    * Otherwise raises `ValueError`.
    """
    digits = _PHONE_RE.sub("", number)

    if digits.startswith("27"):
        return digits
    if digits.startswith("0") and len(digits) == 10:
        return "27" + digits[1:]

    raise ValueError(f"Unsupported phone format: {number!r}")


# ── Public API ────────────────────────────────────────────────────────────────
def send_sms(*, phone: str, message: str) -> ClickatellSuccess:
    """
    Send a single SMS via Clickatell Platform.

    Parameters
    ----------
    phone : str
        Recipient phone – local SA formats allowed (e.g. '0821234567').
    message : str
        Body text, max 160 chars for pure GSM, but Clickatell will split
        multipart automatically.

    Returns
    -------
    dict
        Parsed JSON on success – see Clickatell docs.  Example::

            {
              "messages": [
                {
                  "acceptedTimestamp": "2025-05-07T19:45:02.123+0000",
                  "apiMessageId":      "56b2e05a-...",
                  "to":               "27821234567",
                  "status":           "Accepted"
                }
              ]
            }

    Raises
    ------
    RuntimeError
        Network error, Clickatell returns HTTP ≥400, or unexpected payload.
    """
    try:
        msisdn = _format_sa_msisdn(phone)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    payload = {
        "messages": [
            {
                "channel": "sms",
                "to": msisdn,
                "content": message,
            }
        ]
    }

    try:
        resp = requests.post(CLICKATELL_API_URL, json=payload, headers=HEADERS, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError(f"Network error contacting Clickatell: {exc}") from exc

    # Successful?  Clickatell returns 202 Accepted (or 200) on success
    if 200 <= resp.status_code < 300:
        try:
            data: ClickatellSuccess = resp.json()  # type: ignore[assignment]
        except ValueError:
            raise RuntimeError("Clickatell response is not valid JSON")
        return data

    # Error branch – include message if present
    try:
        err: ClickatellError = resp.json()  # type: ignore[assignment]
        detail = f"{err.get('error', 'Unknown error')} (code={err.get('errorCode')})"
    except ValueError:
        detail = resp.text or f"HTTP {resp.status_code}"

    raise RuntimeError(f"Clickatell error: {detail}")
