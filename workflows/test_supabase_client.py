# workflows/test_supabase_client.py
# ─────────────────────────────────────────────────────────────────────────────

"""
Standalone test for Supabase authentication and session refresh.

Place this file in the `workflows/` directory and run:

    python -m workflows.test_supabase_client

It will:
 1. Print the current session access token (if any).
 2. Pause for you to inspect.
 3. Attempt to refresh the session and print the new token.
"""

import logging
import os
from clients.supabase.client import supabase, session, refresh_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("\n▶ Testing Supabase Client Authentication and Refresh\n")

    if session:
        log.info("Initial access token: %s", session.access_token)
    else:
        log.info("No user session: running as anon/service role key.")

    input("\nPress Enter to call refresh_session() and display new token... ")

    try:
        refresh_session()
        if session:
            log.info("Refreshed access token: %s", session.access_token)
        else:
            log.info("Still no session after refresh (anon/service). No action taken.")
    except Exception as e:
        log.error("refresh_session() failed: %s", e)


if __name__ == "__main__":
    main()
