#clients/utilis/utils_functions.py
# ─────────────────────────────────────────────────────────────────────────────
"""
Common helper utilities for HOUSS-API

- clean_msisdn …  normalise SA mobile numbers to +27…  (handles Excel quirks)
- make_vcard  …  build a vCard-3 text block for iOS / Android import
"""

import re


def clean_msisdn(raw: str) -> str:
    """
    Accepts anything that might have come from Excel – including:
      • a text cell prefixed with an apostrophe  ('0829215785)
      • an integer cell where the leading 0 was dropped (829215785)
      • an already-international number (+27 82 921 5785)

    Returns a clean +E.164 string ready for WhatsApp / Contacts.
    """
    txt = str(raw).lstrip().lstrip("'").strip()            # drop Excel apostrophe
    digits = re.sub(r"\D", "", txt)                        # keep only digits

    # 9-digit case → Excel stored 082… as 829…
    if len(digits) == 9 and not digits.startswith("0"):
        digits = "0" + digits

    # ZA local → international
    if digits.startswith("0") and len(digits) == 10:
        digits = "27" + digits[1:]

    # Ensure leading +
    if not digits.startswith("+"):
        digits = f"+{digits}"

    return digits


def make_vcard(
    full_name: str,
    tel: str,
    email: str | None = None,
    note: str | None = None,
    phone_type: str = "iphone",
) -> str:
    """
    Build a vCard-3 block.

    phone_type:
      • "iphone" / "ios" → TEL;TYPE=IPHONE
      • anything else    → TEL;TYPE=CELL
    """
    tel_label = "IPHONE" if phone_type.lower() in {"iphone", "ios"} else "CELL"

    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"N:{full_name};;;;",
        f"FN:{full_name}",
        f"TEL;TYPE={tel_label}:{tel}",
    ]
    if email:
        lines.append(f"EMAIL;TYPE=INTERNET:{email}")
    if note:
        note_clean = note.replace("\n", "\r\n")   # ← pre-compute, no backslash in f-string
        lines.append(f"NOTE:{note_clean}")
    lines.append("END:VCARD")
    return "\r\n".join(lines)