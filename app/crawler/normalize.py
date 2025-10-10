from __future__ import annotations

import re
import unicodedata
from typing import Optional


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    # Normalize width (e.g., half-width kana to full-width), combine characters
    value = unicodedata.normalize("NFKC", value)
    # Collapse whitespace
    value = re.sub(r"\s+", " ", value)
    # Normalize common historical forms
    value = value.replace("株式會社", "株式会社")
    return value.strip().lower()
