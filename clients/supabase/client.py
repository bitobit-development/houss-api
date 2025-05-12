# clients/supabase/client.py
"""Unified Supabase client with HTTP/1.1 enforced by monkey-patching HTTPX

• If `USER_EMAIL` / `USER_PASSWORD` are set, signs in as that user and uses their JWT (for RLS).
• Otherwise falls back to a service-role key (bypasses RLS) or anon key.
• Disables HTTP/2 globally via HTTPX monkey-patch to avoid stream-limit errors.
"""
from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Monkey-patch HTTPX to disable HTTP/2 by default for both sync and async clients
import httpx as _httpx
_orig_client = _httpx.Client
_orig_async_client = getattr(_httpx, 'AsyncClient', None)

class _NoHTTP2Client(_orig_client):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('http2', False)
        super().__init__(*args, **kwargs)
_httpx.Client = _NoHTTP2Client

if _orig_async_client:
    class _NoHTTP2AsyncClient(_orig_async_client):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault('http2', False)
            super().__init__(*args, **kwargs)
    _httpx.AsyncClient = _NoHTTP2AsyncClient

import os
import logging
from supabase import create_client

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Environment variables
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_URL          = os.environ["SUPABASE_URL"]
ANON_KEY              = os.environ.get("SUPABASE_KEY")
SERVICE_ROLE_KEY      = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
USER_EMAIL            = os.environ.get("USER_EMAIL")
USER_PASSWORD         = os.environ.get("USER_PASSWORD")

if not (ANON_KEY or SERVICE_ROLE_KEY):
    raise RuntimeError(
        "Set at least SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY in your environment."
    )

# ──────────────────────────────────────────────────────────────────────────────
# Client initialization
# ──────────────────────────────────────────────────────────────────────────────
if USER_EMAIL and USER_PASSWORD:
    # Authenticated user flow (RLS-aware)
    key = ANON_KEY or SERVICE_ROLE_KEY
    client = create_client(SUPABASE_URL, key)
    try:
        res = client.auth.sign_in_with_password({
            "email": USER_EMAIL,
            "password": USER_PASSWORD,
        })
        session = getattr(res, 'session', res)
        logger.info("Signed in as %s", USER_EMAIL)
    except Exception as exc:
        logger.error("Supabase sign-in failed: %s", exc)
        raise
    supabase = client
else:
    # Service-role or anon (no sign-in)
    key = SERVICE_ROLE_KEY or ANON_KEY
    mode = "service_role" if SERVICE_ROLE_KEY else "anon"
    logger.info("Creating %s client (no sign-in)", mode)
    supabase = create_client(SUPABASE_URL, key)
    session = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Session refresh helper
# ──────────────────────────────────────────────────────────────────────────────
def refresh_session() -> None:
    """Refresh JWT if we have a signed-in session."""
    global session, supabase
    if not session:
        logger.debug("No user session to refresh.")
        return
    try:
        new = supabase.auth.refresh_session(session.refresh_token)
        session = getattr(new, 'session', new)
        logger.info("Supabase session refreshed")
    except Exception as exc:
        logger.error("Session refresh failed: %s", exc)
        raise
