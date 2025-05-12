# clients/supabase/client.py
"""Unified Supabase client

• If `USER_EMAIL` / `USER_PASSWORD` are set, sign in as that user and attach the
  JWT so RLS (`auth.uid()`) works.
• Otherwise fall back to an unauthenticated or service-role client (bypassing RLS).
"""
from __future__ import annotations

import os
import logging
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
    raise RuntimeError("Set at least SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY in your env")

# Choose key & auth strategy
if USER_EMAIL and USER_PASSWORD:
    # Authenticated user flow
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
    supabase = _base_client  # carries JWT in headers
else:
    # Service-role or anon (no sign-in)
    key_to_use = SERVICE_ROLE_KEY or ANON_KEY
    mode = "service_role" if SERVICE_ROLE_KEY else "anon"
    logger.info("Creating %s client (no user sign-in)", mode)
    supabase = create_client(SUPABASE_URL, key_to_use)
    session = None  # no user session

# ---------------------------------------------------------------------------
# Refresh helper
# ---------------------------------------------------------------------------
def refresh_session() -> None:
    """Refresh JWT if we have a signed-in session."""
    global session, supabase
    if not session:
        logger.debug("No user session to refresh.")
        return
    try:
        # Supabase GoTrue expects a refresh_token string
        new = supabase.auth.refresh_session(session.refresh_token)
        # supabase-py may return a dict or object with `session`
        session = getattr(new, 'session', new)
        logger.info("Supabase session refreshed")
    except Exception as exc:
        logger.error("Session refresh failed: %s", exc)
        raise
