# workflows/match_es_to_pl.py

import os
import logging
import re

from clients.supabase.client import supabase

# ------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Utility: normalize text into set of lowercase words
# ------------------------------------------------------------
WORD_RE = re.compile(r"\b\w+\b")

def extract_words(text: str) -> set:
    """
    Extract alphanumeric words from text, lowercased.
    """
    return set(m.group(0).lower() for m in WORD_RE.finditer(text or ""))

# ------------------------------------------------------------
# Main workflow: match estate_plant to residential_estates
# ------------------------------------------------------------
def match_estates_to_plants():
    """
    For each plant in `estate_plant`, try to match one of its name words
    to any word in `residential_estates.estate_name`. If matched, update
    `estate_id`. At end, log unmatched plant IDs and their count.
    """
    # 1️⃣ Fetch all residential estates
    try:
        resp = supabase.table("residential_estates").select("id,estate_name").execute()
        estates = getattr(resp, "data", []) or []
        logger.info("Loaded %d residential estates", len(estates))
    except Exception as e:
        logger.error("Failed to fetch residential_estates: %s", e)
        return

    # Precompute estate words
    estate_map = {}
    for es in estates:
        eid = es.get("id")
        name = es.get("estate_name", "")
        words = extract_words(name)
        estate_map[eid] = words

    # 2️⃣ Fetch all plants
    try:
        resp = supabase.table("estate_plant").select("id,name").execute()
        plants = getattr(resp, "data", []) or []
        logger.info("Loaded %d plants", len(plants))
    except Exception as e:
        logger.error("Failed to fetch estate_plant: %s", e)
        return

    unmatched = []

    # 3️⃣ Attempt matching
    for pl in plants:
        pid = pl.get("id")
        pname = pl.get("name", "")
        pwords = extract_words(pname)

        matched_eid = None
        # Search for any overlapping word
        for eid, ewords in estate_map.items():
            if pwords & ewords:
                matched_eid = eid
                break

        if matched_eid:
            # Update estate_id on estate_plant
            try:
                up = (supabase
                      .table("estate_plant")
                      .update({"estate_id": matched_eid})
                      .eq("id", pid)
                      .execute())
                logger.info("Plant %d matched to estate %d", pid, matched_eid)
            except Exception as e:
                logger.error("Error updating plant %d: %s", pid, e)
        else:
            unmatched.append(pid)
            logger.warning("No match for Plant %d ('%s')", pid, pname)

    # 4️⃣ Summary
    if unmatched:
        logger.info("Total unmatched plants: %d", len(unmatched))
        logger.info("Unmatched plant IDs: %s", unmatched)
    else:
        logger.info("All plants matched successfully.")

if __name__ == "__main__":
    match_estates_to_plants()

# python -m workflows.match_es_to_pl