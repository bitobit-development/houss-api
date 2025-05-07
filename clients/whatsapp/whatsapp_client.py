# clients/whatsapp/whatsapp_client.py
# ──────────────────────────────────────────────────────────────────────────────
"""
Typed helper for sending a **plain-text WhatsApp message** via Meta's
*WhatsApp Business Cloud API*.

API reference (v19.0 – May 2025):
  POST https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages

Required env vars
-----------------
WHATSAPP_PHONE_ID     – numeric phone-number ID from FB app dashboard
WHATSAPP_ACCESS_TOKEN – long-lived user token with `whatsapp_business_messaging`
"""

from __future__ import annotations

import os
import re
import requests
from typing import TypedDict, Final
from pydantic import BaseModel, Field

__all__ = ["WhatsappPayload", "send_whatsapp"]


# ── Config ───────────────────────────────────────────────────────────────────
PHONE_ID: Final[str | None] = os.getenv("WHATSAPP_PHONE_ID")
TOKEN:    Final[str | None] = os.getenv("WHATSAPP_ACCESS_TOKEN")

if not PHONE_ID or not TOKEN:
    raise RuntimeError(
        "WHATSAPP_PHONE_ID or WHATSAPP_ACCESS_TOKEN not set. "
        "Add them to Replit Secrets or your shell."
    )

API_URL: Final[str] = f"https://graph.facebook.com/v19.0/{PHONE_ID}/messages"

HEADERS: Final[dict[str, str]] = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
}


# ── Pydantic model exported for FastAPI payloads ─────────────────────────────
class WhatsappPayload(BaseModel):
    phone: str = Field(..., example="0821234567")
    message: str = Field(..., example="Hello from HOUSS-API WhatsApp!")


# ── Successful Meta response shape (subset) ──────────────────────────────────
class WhatsappSuccess(TypedDict):
    messaging_product: str   # "whatsapp"
    contacts: list[dict]
    messages: list[dict]


# ── Helpers ──────────────────────────────────────────────────────────────────
def _normalize_sa_msisdn(num: str) -> str:
    """Convert local SA numbers → E.164 (2782…); pass through if already +27/27."""
    digits = re.sub(r"\D+", "", num)
    if digits.startswith("27"):
        return digits
    if digits.startswith("0") and len(digits) == 10:
        return "27" + digits[1:]
    raise ValueError(f"Unsupported phone format: {num!r}")


# ── Public API ───────────────────────────────────────────────────────────────
def send_whatsapp(*, phone: str, message: str) -> WhatsappSuccess:
    """
    Send a text message via WhatsApp Business Cloud.

    Returns Meta's JSON on success; raises RuntimeError on any failure.
    """
    try:
        msisdn = _normalize_sa_msisdn(phone)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    payload = {
        "messaging_product": "whatsapp",
        "to": msisdn,
        "type": "text",
        "text": {"body": message},
    }

    try:
        resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError(f"Network error contacting WhatsApp API: {exc}") from exc

    if 200 <= resp.status_code < 300:
        return resp.json()  # type: ignore[return-value]

    # Error branch
    try:
        detail = resp.json().get("error", {}).get("message", resp.text)
    except ValueError:
        detail = resp.text

    raise RuntimeError(f"WhatsApp API error ({resp.status_code}): {detail}")
