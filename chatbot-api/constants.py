"""
@file constants.py
@brief Single source of truth for domain constants shared across services.

All location aliases, work-mode keywords, and city name mappings live HERE.
Services import from this file — never define duplicates locally.

Why centralised?
  Previously the same data was copy-pasted in 4-5 places with minor
  inconsistencies (missing aliases, different city name strings).
  One change here now propagates to every service automatically.
"""

# ── Location aliases ───────────────────────────────────────────────────────────
# Canonical key → all accepted user-typed aliases (Vietnamese + English + abbrev).
# Keys ("hcm", "hanoi", "danang") are the internal identifiers used throughout
# the codebase.  Add new cities or aliases HERE only.

LOCATION_ALIASES: dict[str, list[str]] = {
    "hcm": [
        "hồ chí minh", "ho chi minh", "hcm", "tp.hcm", "tphcm",
        "sài gòn", "saigon", "sg", "hcmc",
    ],
    "hanoi": [
        "hà nội", "ha noi", "hanoi", "hn",
    ],
    "danang": [
        "đà nẵng", "da nang", "danang", "đn",
    ],
}

# Canonical key → Gold layer city name (used in Trino SQL WHERE clauses).
# Must match exact values stored in iceberg.gold.dim_location.city_name.
LOCATION_CITY_NAME: dict[str, str] = {
    "hcm":    "Ho Chi Minh",
    "hanoi":  "Ha Noi",
    "danang": "Da Nang",
}

# Flat reverse map: any alias string → canonical key.
# Derived automatically from LOCATION_ALIASES — do not edit manually.
# Used by query_processor for O(1) alias normalisation.
LOCATION_ALIAS_TO_KEY: dict[str, str] = {
    alias: key
    for key, aliases in LOCATION_ALIASES.items()
    for alias in aliases
}

# Flat map: any alias → display city name (for query expansion / search variants).
# e.g. "hcm" → "Ho Chi Minh", "sài gòn" → "Ho Chi Minh"
LOCATION_ALIAS_TO_NAME: dict[str, str] = {
    alias: LOCATION_CITY_NAME[key]
    for key, aliases in LOCATION_ALIASES.items()
    for alias in aliases
}

# ── Work mode keywords ─────────────────────────────────────────────────────────
# Mode key → all trigger phrases (Vietnamese + English).
# Used for detecting and filtering by work mode in user queries and job text.

WORK_MODE_KEYWORDS: dict[str, list[str]] = {
    "remote": ["remote", "từ xa", "làm tại nhà", "work from home", "wfh"],
    "hybrid": ["hybrid", "kết hợp"],
    "onsite": ["onsite", "tại văn phòng", "on-site", "office"],
}
