# clients/supabase/client.py
"""Unified Supabase client with HTTP/1.1 enforced via HTTPX env var

• If `USER_EMAIL` / `USER_PASSWORD` are set, signs in as that user and uses their JWT so RLS (`auth.uid()`) works.
• Otherwise falls back to a service-role key (bypasses RLS) or anon key.
• Disables HTTP/2 by setting HTTPX_HTTP2=0 in the environment before client init.
"""
from __future__ import annotations

import os
import logging
# Disable HTTP/2 for all HTTPX requests
os.environ.setdefault("HTTPX_HTTP2", "0")
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment
SUPABASE_URL            = os.environ["SUPABASE_URL"]
ANON_KEY                = os.environ.get("SUPABASE_KEY")
SERVICE_ROLE_KEY        = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
USER_EMAIL              = os.environ.get("USER_EMAIL")
USER_PASSWORD           = os.environ.get("USER_PASSWORD")

if not (ANON_KEY or SERVICE_ROLE_KEY):
    raise RuntimeError("Set at least SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY in your environment.")

# Decide which key & auth strategy to use
if USER_EMAIL and USER_PASSWORD:
    # Authenticated user flow (RLS-aware)
    base_key = ANON_KEY or SERVICE_ROLE_KEY
    _base_client = create_client(SUPABASE_URL, base_key)
    try:
        res = _base_client.auth.sign_in_with_password({
            "email": USER_EMAIL,
            "password": USER_PASSWORD,
        })
        session = getattr(res, 'session', res)
        logger.info("Signed in as %s", USER_EMAIL)
    except Exception as exc:
        logger.error("Supabase sign-in failed: %s", exc)
        raise

    supabase = _base_client  # carries the JWT internally
else:
    # Service-role or anon client (no sign-in)
    key_to_use = SERVICE_ROLE_KEY or ANON_KEY
    mode = "service_role" if SERVICE_ROLE_KEY else "anon"
    logger.info("Creating %s client (no user sign-in)", mode)
    supabase = create_client(SUPABASE_URL, key_to_use)
    session = None  # no JWT session

# ---------------------------------------------------------------------------
# Helper to refresh the JWT session
# ---------------------------------------------------------------------------
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