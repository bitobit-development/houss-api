# clients/supabase/client.py
"""Unified Supabase client

• If `USER_EMAIL` / `USER_PASSWORD` are set, sign in as that user and attach the
  JWT to every request so RLS (`auth.uid()`) works.
• Otherwise fall back to an unauthenticated client **or** a `SERVICE_ROLE` client
  (if `SUPABASE_SERVICE_ROLE_KEY` is provided). This keeps the rest of the code
  working even when you only need anonymous access or are running background
  jobs that bypass RLS.
"""

from __future__ import annotations

import os
import logging
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL  = os.environ["SUPABASE_URL"]
ANON_KEY      = os.environ.get("SUPABASE_KEY")
SERVICE_KEY   = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # optional
USER_EMAIL    = os.environ.get("USER_EMAIL")
USER_PASSWORD = os.environ.get("USER_PASSWORD")

if not (ANON_KEY or SERVICE_KEY):
    raise RuntimeError("Set at least SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY in your env")

# ---------------------------------------------------------------------------
# 1) Decide which key & auth strategy to use
# ---------------------------------------------------------------------------

if USER_EMAIL and USER_PASSWORD:
    # ――― Authenticated user flow (RLS aware) ―――
    _base_client: Client = create_client(SUPABASE_URL, ANON_KEY or SERVICE_KEY)
    try:
        res = _base_client.auth.sign_in_with_password({
            "email": USER_EMAIL,
            "password": USER_PASSWORD,
        })
        session = res.session
        logger.info("Signed in as %s", USER_EMAIL)
    except Exception as exc:
        logger.error("Supabase sign‑in failed: %s", exc)
        raise

    supabase: Client = _base_client  # already carries the JWT internally

else:
    # ――― Service role or anon client (no sign‑in) ―――
    key_to_use = SERVICE_KEY or ANON_KEY
    mode = "service_role" if SERVICE_KEY else "anon"
    logger.info("Creating %s client (no user sign‑in)", mode)
    supabase: Client = create_client(SUPABASE_URL, key_to_use)
    session = None  # type: ignore

# ---------------------------------------------------------------------------
# 2) Optional helper to refresh session (only when signed‑in)
# ---------------------------------------------------------------------------

def refresh_session() -> None:
    """Refresh JWT if we have a signed‑in session."""
    global session
    if session is None:
        logger.debug("No user session to refresh.")
        return
    try:
        new = supabase.auth.refresh_session({
            "refresh_token": session.refresh_token,
        })
        session = new.session
        logger.info("Supabase session refreshed")
    except Exception as exc:
        logger.error("Session refresh failed: %s", exc)
        raise
