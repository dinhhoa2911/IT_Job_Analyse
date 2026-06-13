"""
@file market_context.py
@brief Market Context Service — bridge between the Gold Data Lake and the RAG pipeline.

When search_job retrieves jobs from Milvus (Silver layer), this service
runs targeted Trino queries against the Gold star schema to enrich the
response with real-time market intelligence:

  ┌──────────────────────────────────────────────────────┐
  │  User: "tìm việc Python HCM"                         │
  │                                                      │
  │  RAG:    Milvus → 5 job cards   (Silver layer)       │
  │  HERE:   Trino  → market stats  (Gold  layer)  ← NEW │
  │                                                      │
  │  Combined LLM response:                              │
  │    [5 job cards]                                     │
  │    + Thị trường Python tại HCM: 127 vị trí,         │
  │      top công ty VNG/Tiki, 45% remote, ...          │
  └──────────────────────────────────────────────────────┘

Queries executed (all against Gold star schema):
  Q1 – total jobs          : fact ⋈ dim_skill [+ optional dims]
  Q2 – top companies       : fact ⋈ dim_skill ⋈ dim_company [+ optional dims]
  Q3 – work mode spread    : fact ⋈ dim_skill ⋈ dim_work_mode [+ optional dims]
  Q4 – related skills      : subquery finds jobs requiring primary skill,
                             then counts co-occurring skills in same job set
  Q5 – job category dist   : fact ⋈ dim_skill ⋈ dim_job_category [+ optional dims]
  Q6 – regional dist       : fact ⋈ dim_skill ⋈ dim_location [+ optional dims]

Supported filter combinations (all optional, combinable):
  - location    : city-level filter (Ho Chi Minh / Ha Noi / Da Nang)
  - region      : region filter (South / North / Central) via dim_location
  - work_mode   : onsite / remote / hybrid via dim_work_mode
  - level       : seniority level via job_title LIKE
  - date        : year / quarter / month via dim_date
  - category    : job category via dim_job_category
  - company     : company name fuzzy match via dim_company

Failure contract:
  All queries are wrapped in a single try/except.  If Trino is unavailable
  or any query fails, this service returns None silently.  The pipeline
  continues without market context rather than blocking the user.
"""

import logging
import re
from collections import Counter

import trino

from config import settings
from constants import LOCATION_CITY_NAME
from models.schemas import JobResult, MarketInsight

logger = logging.getLogger(__name__)

# ── Work mode canonical name map ──────────────────────────────────────────────
# Maps internal key (from constants.WORK_MODE_KEYWORDS) → Gold dim_work_mode.work_mode value
_WORK_MODE_CANONICAL: dict[str, str] = {
    "onsite": "At Office",
    "remote": "Remote",
    "hybrid": "Hybrid",
}

# ── Category keyword map ───────────────────────────────────────────────────────
# Maps lowercased query keyword → exact category_name in dim_job_category
_CATEGORY_KEYWORDS: dict[str, str] = {
    # Backend Development
    "backend":          "Backend Development",
    "back-end":         "Backend Development",
    "back end":         "Backend Development",
    "lập trình backend":"Backend Development",
    # Frontend Development
    "frontend":         "Frontend Development",
    "front-end":        "Frontend Development",
    "front end":        "Frontend Development",
    "lập trình frontend":"Frontend Development",
    # Fullstack Development
    "fullstack":        "Fullstack Development",
    "full-stack":       "Fullstack Development",
    "full stack":       "Fullstack Development",
    "lập trình fullstack":"Fullstack Development",
    # Testing & QA
    "testing":          "Testing & QA",
    "qa":               "Testing & QA",
    "qc":               "Testing & QA",
    "quality assurance":"Testing & QA",
    "quality control":  "Testing & QA",
    "kiểm thử":         "Testing & QA",
    "tester":           "Testing & QA",
    # DevOps & Infrastructure
    "devops":           "DevOps & Infrastructure",
    "dev-ops":          "DevOps & Infrastructure",
    "infrastructure":   "DevOps & Infrastructure",
    "hạ tầng":          "DevOps & Infrastructure",
    "sysadmin":         "DevOps & Infrastructure",
    "system admin":     "DevOps & Infrastructure",
    # Mobile Development
    "mobile":           "Mobile Development",
    "android":          "Mobile Development",
    "ios":              "Mobile Development",
    "react native":     "Mobile Development",
    "flutter":          "Mobile Development",
    "lập trình mobile": "Mobile Development",
    # AI & Machine Learning
    "ai":               "AI & Machine Learning",
    "machine learning": "AI & Machine Learning",
    "ml":               "AI & Machine Learning",
    "deep learning":    "AI & Machine Learning",
    "data science":     "AI & Machine Learning",
    "trí tuệ nhân tạo": "AI & Machine Learning",
    "học máy":          "AI & Machine Learning",
    # Data Engineering
    "data engineer":    "Data Engineering",
    "data pipeline":    "Data Engineering",
    "etl":              "Data Engineering",
    "big data":         "Data Engineering",
    "dữ liệu":          "Data Engineering",
    "kỹ thuật dữ liệu": "Data Engineering",
    # Data Analytics
    "data analyst":     "Data Analytics",
    "analytics":        "Data Analytics",
    "phân tích dữ liệu":"Data Analytics",
    "business intelligence":"Data Analytics",
    "bi analyst":       "Data Analytics",
    # Cyber Security
    "security":         "Cyber Security",
    "cybersecurity":    "Cyber Security",
    "cyber security":   "Cyber Security",
    "bảo mật":          "Cyber Security",
    "an ninh mạng":     "Cyber Security",
    "pentest":          "Cyber Security",
    # Embedded & IoT
    "embedded":         "Embedded & IoT",
    "iot":              "Embedded & IoT",
    "firmware":         "Embedded & IoT",
    "nhúng":            "Embedded & IoT",
    # ERP & CRM
    "erp":              "ERP & CRM",
    "crm":              "ERP & CRM",
    "sap":              "ERP & CRM",
    "oracle erp":       "ERP & CRM",
    # Game Development
    "game":             "Game Development",
    "unity":            "Game Development",
    "unreal":           "Game Development",
    "phát triển game":  "Game Development",
    # Management
    "management":       "Management",
    "manager":          "Management",
    "quản lý":          "Management",
    "team lead":        "Management",
    "project manager":  "Management",
    # Product & Business Analysis
    "product":          "Product & Business Analysis",
    "business analyst": "Product & Business Analysis",
    "ba":               "Product & Business Analysis",
    "product manager":  "Product & Business Analysis",
    "product owner":    "Product & Business Analysis",
    "po":               "Product & Business Analysis",
    "phân tích nghiệp vụ":"Product & Business Analysis",
    # Software Engineering
    "software engineer":"Software Engineering",
    "kỹ sư phần mềm":  "Software Engineering",
    "lập trình viên":   "Software Engineering",
    # Other
    "other":            "Other",
    "khác":             "Other",
}

# Location abbreviations that must NOT be mistaken for company names (strategy 2
# of _build_company_clauses uses a capitalised-word heuristic after trigger words
# like "tại/at"; these abbreviations look like company names but are locations).
_LOCATION_ABBREVS: frozenset[str] = frozenset({
    "hcm", "hcmc", "tphcm", "tp.hcm", "sg",
    "hn",
    "dn",
})

# ── Region keyword map ────────────────────────────────────────────────────────
# Maps query keyword → Gold dim_location.region value
_REGION_KEYWORDS: dict[str, str] = {
    # South
    "miền nam":  "South",
    "mien nam":  "South",
    "phía nam":  "South",
    "south":     "South",
    # North
    "miền bắc":  "North",
    "mien bac":  "North",
    "phía bắc":  "North",
    "north":     "North",
    # Central
    "miền trung":"Central",
    "mien trung":"Central",
    "phía trung":"Central",
    "central":   "Central",
}

# ── Month name map ─────────────────────────────────────────────────────────────
_MONTH_NAMES: dict[str, int] = {
    "january": 1,  "jan": 1,  "tháng 1": 1,  "tháng một": 1,
    "february": 2, "feb": 2,  "tháng 2": 2,  "tháng hai": 2,
    "march": 3,    "mar": 3,  "tháng 3": 3,  "tháng ba": 3,
    "april": 4,    "apr": 4,  "tháng 4": 4,  "tháng tư": 4,
    "may": 5,                 "tháng 5": 5,  "tháng năm": 5,
    "june": 6,     "jun": 6,  "tháng 6": 6,  "tháng sáu": 6,
    "july": 7,     "jul": 7,  "tháng 7": 7,  "tháng bảy": 7,
    "august": 8,   "aug": 8,  "tháng 8": 8,  "tháng tám": 8,
    "september": 9,"sep": 9,  "tháng 9": 9,  "tháng chín": 9,
    "october": 10, "oct": 10, "tháng 10": 10,"tháng mười": 10,
    "november": 11,"nov": 11, "tháng 11": 11,"tháng mười một": 11,
    "december": 12,"dec": 12, "tháng 12": 12,"tháng mười hai": 12,
}

# ── SQL templates ─────────────────────────────────────────────────────────────
# Placeholder naming convention:
#   {skill}            — escaped skill name literal
#   {location_join}    — "JOIN dim_location dl ON ..." or ""
#   {location_where}   — "AND LOWER(dl.city_name) LIKE ..." or ""
#   {region_where}     — "AND LOWER(dl.region) LIKE ..." or ""  (needs location_join)
#   {work_mode_join}   — "JOIN dim_work_mode dwm ON ..." or ""  (omit if already joined)
#   {work_mode_where}  — "AND LOWER(dwm.work_mode) = ..." or ""
#   {date_join}        — "JOIN dim_date d ON ..." or ""
#   {date_where}       — "AND d.year = ... AND d.month = ..." or ""
#   {level_where}      — "AND LOWER(f.job_title) LIKE ..." or ""
#   {category_join}    — "JOIN dim_job_category djc ON ..." or ""  (omit if already joined)
#   {category_where}   — "AND LOWER(djc.category_name) = ..." or ""
#   {company_join}     — "JOIN dim_company dc ON ..." or ""  (omit if already joined)
#   {company_where}    — "AND LOWER(dc.company_name) LIKE ..." or ""

_SQL_TOTAL_JOBS = """
SELECT COUNT(DISTINCT f.job_link) AS total
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
{location_join}
{work_mode_join}
{date_join}
{category_join}
{company_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
{region_where}
{work_mode_where}
{date_where}
{level_where}
{category_where}
{company_where}
"""

_SQL_TOP_COMPANIES = """
SELECT
    dc.company_name,
    COUNT(DISTINCT f.job_link) AS job_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill    ds ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_company  dc ON f.company_id  = dc.company_id
{location_join}
{work_mode_join}
{date_join}
{category_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
{region_where}
{work_mode_where}
{date_where}
{level_where}
{category_where}
{company_where}
GROUP BY dc.company_name
ORDER BY job_count DESC
LIMIT 50
"""

# NOTE: dim_work_mode is already JOINed in this template (hardcoded) so we
# do NOT use {work_mode_join} here. We also do NOT filter by work_mode because
# we are grouping BY work_mode to show the full distribution.
_SQL_WORK_MODE = """
SELECT
    dwm.work_mode,
    COUNT(DISTINCT f.job_link) AS cnt
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill     ds  ON f.skill_id = ds.skill_id
JOIN iceberg.gold.dim_work_mode dwm ON f.mode_id  = dwm.mode_id
{location_join}
{date_join}
{category_join}
{company_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
{region_where}
{date_where}
{level_where}
{category_where}
{company_where}
GROUP BY dwm.work_mode
ORDER BY cnt DESC
"""

# NOTE: For _SQL_RELATED_SKILLS the outer query applies all filters to the
# co-occurring jobs so results respect location/mode/level/date/etc.
# The inner subquery finds job_links that have the primary skill (unfiltered)
# so the co-occurrence pool is still broad; outer WHERE then narrows the
# context to only the relevant filtered jobs.
_SQL_RELATED_SKILLS = """
SELECT
    ds.skill_name,
    COUNT(DISTINCT f.job_link) AS co_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
{location_join}
{work_mode_join}
{date_join}
{category_join}
{company_join}
WHERE f.job_link IN (
    SELECT DISTINCT f2.job_link
    FROM iceberg.gold.fact_job_posting f2
    JOIN iceberg.gold.dim_skill ds2 ON f2.skill_id = ds2.skill_id
    WHERE LOWER(ds2.skill_name) = LOWER('{skill}')
)
AND LOWER(ds.skill_name) != LOWER('{skill}')
{location_where}
{region_where}
{work_mode_where}
{date_where}
{level_where}
{category_where}
{company_where}
GROUP BY ds.skill_name
ORDER BY co_count DESC
LIMIT 6
"""

# NOTE: dim_job_category is already JOINed in this template (hardcoded) and
# grouped BY category_name, so we do NOT add {category_join} or {category_where}
# (filtering by category while grouping by category would be circular/meaningless).
_SQL_CATEGORY_DIST = """
SELECT
    djc.category_name,
    COUNT(DISTINCT f.job_link) AS job_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill        ds  ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
{location_join}
{work_mode_join}
{date_join}
{company_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
{region_where}
{work_mode_where}
{date_where}
{level_where}
{company_where}
GROUP BY djc.category_name
ORDER BY job_count DESC
LIMIT 5
"""

_SQL_REGION_DIST = """
SELECT
    dl.region,
    COUNT(DISTINCT f.job_link) AS job_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill    ds ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id
{work_mode_join}
{date_join}
{category_join}
{company_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{work_mode_where}
{date_where}
{level_where}
{category_where}
{company_where}
GROUP BY dl.region
ORDER BY job_count DESC
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _escape(s: str) -> str:
    """
    @brief Escape single quotes in a string for safe embedding in Trino SQL literals.

    @param s  Raw string value to escape.
    @return   String with single quotes doubled (SQL standard escaping).
    """
    return s.replace("'", "''")


def _build_location_clauses(city: str | None) -> tuple[str, str]:
    """
    @brief Build SQL JOIN and WHERE clauses for optional city-level location filtering.

    @param city  Canonical city name (e.g. "Ho Chi Minh"), or None to skip filtering.
    @return      Tuple of (JOIN clause string, WHERE AND clause string); both empty if city is None.
    """
    if not city:
        return "", ""
    safe = _escape(city)
    join  = "JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id"
    where = f"AND LOWER(dl.city_name) LIKE LOWER('%{safe}%')"
    return join, where


def _build_region_clause(query: str, location_join_already: bool = False) -> tuple[str, str]:
    """
    @brief Detect region filter from query and return (join_clause, where_clause).

    Detects Vietnamese/English region keywords (miền Nam, miền Bắc, miền Trung,
    South, North, Central) and builds the appropriate SQL fragments.

    @param query                 User query string.
    @param location_join_already Whether dim_location is already JOINed (avoids duplicate JOIN).
    @return                      Tuple of (JOIN clause or "", WHERE AND clause or "").
                                 JOIN clause is empty when location is already joined or no region found.
    """
    q = query.lower()
    # Check longest keys first to avoid partial matches.
    # ASCII region keywords ("south", "north", "central") use word-boundary matching
    # so they don't fire on "southern", "northern", "northeast", etc.
    for keyword in sorted(_REGION_KEYWORDS, key=len, reverse=True):
        if keyword.isascii():
            if not re.search(r"\b" + re.escape(keyword) + r"\b", q):
                continue
        elif keyword not in q:
            continue
        region = _REGION_KEYWORDS[keyword]
        safe = _escape(region)
        where = f"AND LOWER(dl.region) LIKE LOWER('%{safe}%')"
        if location_join_already:
            return "", where
        join = "JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id"
        return join, where
    return "", ""


def _build_work_mode_clauses(work_mode_key: str | None) -> tuple[str, str]:
    """
    @brief Build SQL JOIN and WHERE clauses for optional work mode filtering.

    @param work_mode_key  Internal key: 'onsite', 'remote', or 'hybrid' (from WORK_MODE_KEYWORDS),
                          or None to skip filtering.
    @return               Tuple of (JOIN clause, WHERE AND clause); both empty if work_mode_key is None.

    IMPORTANT: Callers must pass work_mode_join="" for _SQL_WORK_MODE since that
    template already has dim_work_mode hardcoded. Pass work_mode_where="" to that
    template as well (we show full distribution, not filter by one mode).
    """
    if not work_mode_key:
        return "", ""
    canonical = _WORK_MODE_CANONICAL.get(work_mode_key)
    if not canonical:
        return "", ""
    safe  = _escape(canonical)
    join  = "JOIN iceberg.gold.dim_work_mode dwm ON f.mode_id = dwm.mode_id"
    where = f"AND LOWER(dwm.work_mode) = LOWER('{safe}')"
    return join, where


def _build_date_clauses(query: str) -> tuple[str, str]:
    """
    @brief Detect date filter from query and build dim_date JOIN + WHERE clauses.

    Detects:
      - Year: 2025 or 2026
      - Quarter: Q1-Q4, quý 1-4, quarter 1-4
      - Month: tháng 1-12, January-December, Jan-Dec

    @param query  User query string.
    @return       Tuple of (JOIN clause for dim_date, WHERE AND clause string).
                  Both empty if no date expression detected in query.
    """
    q = query.lower()
    conditions: list[str] = []

    # Detect year (2025 or 2026)
    year_match = re.search(r"\b(202[0-9])\b", q)
    if year_match:
        conditions.append(f"AND d.year = {year_match.group(1)}")

    # Detect quarter: Q1/Q2/Q3/Q4 or "quý 1" / "quarter 1"
    quarter_match = re.search(
        r"\b(?:q([1-4])|qu[yý]\s*([1-4])|quarter\s*([1-4]))\b", q
    )
    if quarter_match:
        qnum = quarter_match.group(1) or quarter_match.group(2) or quarter_match.group(3)
        conditions.append(f"AND d.quarter = {qnum}")

    # Detect month — check named months first (longest first to avoid "may" partial)
    month_found = False
    for name in sorted(_MONTH_NAMES, key=len, reverse=True):
        # Use word boundary for ASCII names; substring for Vietnamese multi-word phrases
        if name.isascii():
            if re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", q):
                conditions.append(f"AND d.month = {_MONTH_NAMES[name]}")
                month_found = True
                break
        else:
            if name in q:
                conditions.append(f"AND d.month = {_MONTH_NAMES[name]}")
                month_found = True
                break

    # Numeric month: "tháng 3" already caught above, but also "month 3"
    if not month_found:
        num_month = re.search(r"\btháng\s+(\d{1,2})\b", q)
        if not num_month:
            num_month = re.search(r"\bmonth\s+(\d{1,2})\b", q)
        if num_month:
            m = int(num_month.group(1))
            if 1 <= m <= 12:
                conditions.append(f"AND d.month = {m}")

    if not conditions:
        return "", ""

    join  = "JOIN iceberg.gold.dim_date d ON f.date_id = d.date_id"
    where = "\n".join(conditions)
    return join, where


def _build_category_clauses(query: str) -> tuple[str, str]:
    """
    @brief Detect job category from query text and build dim_job_category JOIN + WHERE.

    Scans query for Vietnamese and English category keywords (longest match first).

    @param query  User query string.
    @return       Tuple of (JOIN clause for dim_job_category, WHERE AND clause).
                  Both empty if no category keyword detected.

    NOTE: Callers must pass category_join="" and category_where="" for _SQL_CATEGORY_DIST
    since that template already JOINs dim_job_category and groups BY it.
    """
    q = query.lower()
    # Longest-key-first to catch multi-word keywords like "machine learning" before "ml".
    # ASCII-only keywords use word-boundary matching to prevent false positives from
    # substring collisions: "ml" inside "html", "ba" inside "database",
    # "po" inside "component", "ai" inside "email", "bi" inside "mobile".
    # Non-ASCII (Vietnamese) keywords keep simple substring matching because Python's
    # \b word boundary is unreliable for accented Unicode characters.
    for keyword in sorted(_CATEGORY_KEYWORDS, key=len, reverse=True):
        if keyword.isascii():
            if not re.search(r"\b" + re.escape(keyword) + r"\b", q):
                continue
        elif keyword not in q:
            continue
        category = _CATEGORY_KEYWORDS[keyword]
        safe = _escape(category)
        join  = "JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id"
        where = f"AND LOWER(djc.category_name) = LOWER('{safe}')"
        return join, where
    return "", ""


def _build_company_clauses(query: str, top_companies: list[str]) -> tuple[str, str]:
    """
    @brief Detect company name from query and build dim_company JOIN + WHERE.

    Detection strategy:
      1. Check if any known top-company name appears (case-insensitive) in the query.
      2. If not found in top list, look for capitalised sequences (2+ title-case words)
         that follow trigger phrases like "tại", "ở", "at", "công ty", "company".

    @param query          User query string.
    @param top_companies  List of known company names (e.g. from cached dim_company).
    @return               Tuple of (JOIN clause for dim_company, WHERE AND clause using LOWER LIKE).
                          Both empty if no company detected.
    """
    q_lower = query.lower()

    # Strategy 1 — match against known company list
    for company in sorted(top_companies, key=len, reverse=True):
        if company.lower() in q_lower:
            safe = _escape(company)
            join  = "JOIN iceberg.gold.dim_company dc ON f.company_id = dc.company_id"
            where = f"AND LOWER(dc.company_name) LIKE LOWER('%{safe}%')"
            return join, where

    # Strategy 2 — extract capitalised company name after trigger words.
    # Guard: skip known location abbreviations (HCM, HCMC, HN, DN, …) that look
    # like capitalised company names but are city shortcuts — without this guard
    # "tại HCM" would add company_where='%HCM%' and collapse total_jobs to 0.
    trigger_pattern = (
        r"(?:tại|ở|at|công ty|company|firm|corp|corporation)\s+"
        r"([A-Z][A-Za-z0-9&\.\-]{1,}(?:\s+[A-Z][A-Za-z0-9&\.\-]{0,}){0,3})"
    )
    m = re.search(trigger_pattern, query)
    if m:
        company_candidate = m.group(1).strip()
        if len(company_candidate) >= 2 and company_candidate.lower() not in _LOCATION_ABBREVS:
            safe  = _escape(company_candidate)
            join  = "JOIN iceberg.gold.dim_company dc ON f.company_id = dc.company_id"
            where = f"AND LOWER(dc.company_name) LIKE LOWER('%{safe}%')"
            return join, where

    return "", ""


# Ordered so multi-word tokens (e.g. "lead") don't swallow "leader".
_LEVEL_TOKENS: list[str] = [
    "fresher", "intern", "junior", "entry",
    "middle", "mid-level", "mid level",
    "senior", "lead", "principal", "staff", "manager",
]

def _build_level_clause(query: str) -> str:
    """Return an AND clause filtering job_title by seniority level, or '' if none detected."""
    q = query.lower()
    for token in _LEVEL_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", q):
            return f"AND LOWER(f.job_title) LIKE '%{token}%'"
    return ""


def _extract_level_label(query: str) -> str | None:
    """Return human-readable level label ('Senior', 'Junior', …) detected in query, or None."""
    q = query.lower()
    for token in _LEVEL_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", q):
            return token.capitalize()
    return None


def _extract_region_label(query: str) -> str | None:
    """Return region label ('South', 'North', 'Central') detected in query, or None."""
    q = query.lower()
    for keyword in sorted(_REGION_KEYWORDS, key=len, reverse=True):
        if keyword.isascii():
            if re.search(r"\b" + re.escape(keyword) + r"\b", q):
                return _REGION_KEYWORDS[keyword]
        elif keyword in q:
            return _REGION_KEYWORDS[keyword]
    return None


def _extract_date_label(query: str) -> str | None:
    """Return human-readable date label e.g. '2025', 'Q1/2025', 'tháng 3/2025', or None."""
    q = query.lower()
    parts: list[str] = []

    year_match = re.search(r"\b(202[0-9])\b", q)
    if year_match:
        parts.append(year_match.group(1))

    quarter_match = re.search(r"\b(?:q([1-4])|qu[yý]\s*([1-4])|quarter\s*([1-4]))\b", q)
    if quarter_match:
        qnum = quarter_match.group(1) or quarter_match.group(2) or quarter_match.group(3)
        parts.insert(0, f"Q{qnum}")

    month_found = False
    for name in sorted(_MONTH_NAMES, key=len, reverse=True):
        if name.isascii():
            if re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", q):
                parts.insert(0, f"tháng {_MONTH_NAMES[name]}")
                month_found = True
                break
        else:
            if name in q:
                parts.insert(0, f"tháng {_MONTH_NAMES[name]}")
                month_found = True
                break
    if not month_found:
        num_m = re.search(r"\btháng\s+(\d{1,2})\b", q) or re.search(r"\bmonth\s+(\d{1,2})\b", q)
        if num_m:
            m = int(num_m.group(1))
            if 1 <= m <= 12:
                parts.insert(0, f"tháng {m}")

    return "/".join(parts) if parts else None


def _extract_category_label(query: str) -> str | None:
    """Return category label ('Backend Development', …) detected in query, or None."""
    q = query.lower()
    for keyword in sorted(_CATEGORY_KEYWORDS, key=len, reverse=True):
        if keyword.isascii():
            if not re.search(r"\b" + re.escape(keyword) + r"\b", q):
                continue
        elif keyword not in q:
            continue
        return _CATEGORY_KEYWORDS[keyword]
    return None


def _extract_company_label(query: str, top_companies: list[str]) -> str | None:
    """Return company name detected in query, or None."""
    q_lower = query.lower()
    for company in sorted(top_companies, key=len, reverse=True):
        if company.lower() in q_lower:
            return company
    trigger_pattern = (
        r"(?:tại|ở|at|công ty|company|firm|corp|corporation)\s+"
        r"([A-Z][A-Za-z0-9&\.\-]{1,}(?:\s+[A-Z][A-Za-z0-9&\.\-]{0,}){0,3})"
    )
    m = re.search(trigger_pattern, query)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) >= 2 and candidate.lower() not in _LOCATION_ABBREVS:
            return candidate
    return None


# Skills that look like job titles or are too generic to be useful as market context filters.
# These appear in dim_skill but picking them as "primary skill" produces wrong market stats.
_SKILL_BLOCKLIST: frozenset[str] = frozenset({
    # Job titles stored as skills
    "data engineer", "software engineer", "backend developer", "frontend developer",
    "fullstack developer", "mobile developer", "devops engineer", "qa engineer",
    "data analyst", "data scientist", "machine learning engineer", "ai engineer",
    "cloud engineer", "embedded engineer", "game developer", "security engineer",
    "product manager", "business analyst",
    # Generic / non-actionable
    "ai", "cloud", "database", "it", "oop", "software", "technology",
    "english", "japanese", "korean",  # languages — not useful for market skill context
    "agile", "scrum",                 # methodologies — too ubiquitous
    "team management", "project management",  # soft skills
})


def _is_noise_skill(skill: str) -> bool:
    return skill.strip().lower() in _SKILL_BLOCKLIST


def _extract_from_jobs(jobs: list[JobResult]) -> str | None:
    """
    @brief Pick the primary skill by frequency across returned job text_content fields.

    Parses the "Skills:" section of each JobResult's text_content, tallies
    occurrences, and returns the most common non-noise skill name.

    @param jobs  Reranked JobResult list from hybrid search.
    @return      Most frequent skill string, or None if no Skills section is found.
    """
    skill_counter: Counter = Counter()
    for job in jobs:
        match = re.search(r"Skills:\s*([^.]+)", job.text_content, re.IGNORECASE)
        if match:
            for s in match.group(1).split(","):
                s = s.strip()
                if s and not _is_noise_skill(s):
                    skill_counter[s] += 1
    if skill_counter:
        top = skill_counter.most_common(1)[0][0]
        logger.debug("Primary skill from job results: '%s'", top)
        return top
    return None


def _run(conn: trino.dbapi.Connection, sql: str) -> list[dict]:
    """
    @brief Execute a SQL statement on an open Trino connection and return rows as dicts.

    @param conn  Active Trino DBAPI connection.
    @param sql   SQL query string to execute.
    @return      List of dicts mapping column names to row values.
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── Main service ──────────────────────────────────────────────────────────────

_SQL_LOAD_SKILLS    = "SELECT skill_name FROM iceberg.gold.dim_skill"
_SQL_LOAD_COMPANIES = "SELECT DISTINCT company_name FROM iceberg.gold.dim_company"


class MarketContextService:
    """
    @class MarketContextService
    @brief Query the Gold star schema (Trino/Iceberg) and return real-time market intelligence.

    Runs up to six focused Trino queries (total jobs, top companies, work-mode
    distribution, related co-occurring skills, category distribution, region
    distribution) and packages the results into a MarketInsight object.

    All queries are wrapped in a single try/except block — on any Trino failure
    the service returns None silently so the RAG pipeline can continue without
    market context.

    At construction time, the full skill and company lists are loaded from the
    Gold layer and cached for filter-detection purposes.
    """

    def __init__(self) -> None:
        # lowercase → original-case mapping built from Gold dim_skill.
        self._skill_lookup:    dict[str, str] = {}
        self._company_list:    list[str]      = []
        self._skills_loaded:   bool           = False
        self._companies_loaded: bool          = False

    def _load_skills(self) -> None:
        """
        @brief Load all skill_name values from iceberg.gold.dim_skill into memory.

        Called once at startup.  Fails silently if Trino is unavailable.
        """
        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=5,
            )
            rows = _run(conn, _SQL_LOAD_SKILLS)
            conn.close()
            self._skill_lookup = {
                r["skill_name"].lower(): r["skill_name"]
                for r in rows
                if r.get("skill_name")
            }
            logger.info(
                "MarketContextService: loaded %d skills from dim_skill.",
                len(self._skill_lookup),
            )
        except Exception as exc:
            logger.warning(
                "MarketContextService: could not load skills from Trino at startup: %s. "
                "Query fallback will be unavailable.",
                exc,
            )

    def _load_companies(self) -> None:
        """
        @brief Load all company_name values from iceberg.gold.dim_company into memory.

        Used by _build_company_clauses for company-name detection in queries.
        Fails silently if Trino is unavailable.
        """
        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=5,
            )
            rows = _run(conn, _SQL_LOAD_COMPANIES)
            conn.close()
            self._company_list = [
                r["company_name"] for r in rows if r.get("company_name")
            ]
            logger.info(
                "MarketContextService: loaded %d companies from dim_company.",
                len(self._company_list),
            )
        except Exception as exc:
            logger.warning(
                "MarketContextService: could not load companies from Trino at startup: %s.",
                exc,
            )

    def _extract_primary_skill(self, jobs: list[JobResult], query: str) -> str | None:
        """
        @brief Detect the primary skill for Gold-layer market queries.

        Priority order (highest first):
          Strategy 0 — query intent (user explicitly named a skill):
            Scan the query against dim_skill. Longest match wins so "React Native"
            beats "React" and "Spring Boot" beats "Spring".
            This ensures "tìm job React remote" → React market context, NOT
            JavaScript (which is more frequent in React job results but irrelevant).

          Strategy 1 — job result frequency (fallback for skill-agnostic queries):
            Parse 'Skills:' from returned job text_content and pick the most
            frequent skill.  Used when the query has no explicit skill mention,
            e.g. "tìm job senior remote HCM".

        @param jobs   Reranked JobResult list from hybrid search.
        @param query  Original user query string.
        @return       Primary skill string with original casing, or None.
        """
        # Strategy 0 — user intent: scan query against dim_skill (highest priority)
        if self._skill_lookup:
            query_lower = query.lower()
            for skill_lower in sorted(self._skill_lookup, key=len, reverse=True):
                if skill_lower in _SKILL_BLOCKLIST:
                    continue
                if re.search(r"(?<!\w)" + re.escape(skill_lower) + r"(?!\w)", query_lower):
                    matched = self._skill_lookup[skill_lower]
                    logger.debug("Primary skill from query intent: '%s'", matched)
                    return matched

        # Strategy 1 — frequency from job results (fallback when query has no skill)
        skill = _extract_from_jobs(jobs)
        if skill:
            return skill

        return None

    def get_insight(
        self,
        jobs:    list[JobResult],
        query:   str,
        filters: dict,
    ) -> "MarketInsight | None":
        """
        @brief Main entry point — called from RAGPipeline after vector search completes.

        Supported filter keys in `filters` dict:
          - location  : canonical location key ('hcm', 'hanoi', 'danang') or None
          - work_mode : work mode key ('onsite', 'remote', 'hybrid') or None

        Additional filters are extracted from the `query` string itself:
          - date      : year / quarter / month expressions
          - category  : job category keywords
          - company   : company name mentions
          - region    : regional keywords (miền Nam/Bắc/Trung, South/North/Central)
          - level     : seniority level keywords (senior, junior, etc.)

        @param jobs     Reranked job results used to detect the primary skill.
        @param query    Original user query string (skill detection + filter extraction).
        @param filters  Dict with keys ``work_mode`` and ``location`` (values may be None).
        @return         Populated MarketInsight, or None if Trino unavailable or no skill found.
        """
        # Lazy-load skill and company lists on first real request
        if not self._skills_loaded:
            self._load_skills()
            self._skills_loaded = True
        if not self._companies_loaded:
            self._load_companies()
            self._companies_loaded = True

        primary_skill = self._extract_primary_skill(jobs, query)
        if not primary_skill:
            logger.info("Market context skipped: no primary skill detected.")
            return None

        # ── Build all filter clause sets ──────────────────────────────────────

        # 1. Location (city) — from filters dict
        location_key = filters.get("location")
        city_name    = LOCATION_CITY_NAME.get(location_key) if location_key else None
        loc_join, loc_where = _build_location_clauses(city_name)

        # 2. Region — from query text
        #    If city-level location is already joined, pass location_join_already=True
        #    so we reuse the existing dl alias instead of generating a duplicate JOIN.
        has_location_join = bool(loc_join)
        region_join, region_where = _build_region_clause(query, location_join_already=has_location_join)

        # Merge: if region needs its own JOIN and city does NOT have one, use region's join
        # If city already joined, region_join is "" (shares the same dl alias).
        # If BOTH city and region are active, city already added the JOIN, region adds only WHERE.
        final_location_join = loc_join or region_join

        # 3. Work mode — from filters dict
        work_mode_key = filters.get("work_mode")
        wm_join, wm_where = _build_work_mode_clauses(work_mode_key)

        # 4. Date — from query text
        date_join, date_where = _build_date_clauses(query)

        # 5. Category — from query text
        cat_join, cat_where = _build_category_clauses(query)

        # 6. Company — from query text
        comp_join, comp_where = _build_company_clauses(query, self._company_list)

        # 7. Level — from query text
        level_where = _build_level_clause(query)

        safe_skill = _escape(primary_skill)

        # ── Shared placeholder dict helpers ──────────────────────────────────
        # Base set of placeholders used by most queries
        def _base_params(*, skip_category_join=False, skip_category_where=False,
                         skip_company_join=False,
                         skip_work_mode_join=False, skip_work_mode_where=False) -> dict:
            return {
                "skill":          safe_skill,
                "location_join":  final_location_join,
                "location_where": loc_where,
                "region_where":   region_where,
                "work_mode_join": "" if skip_work_mode_join else wm_join,
                "work_mode_where":"" if skip_work_mode_where else wm_where,
                "date_join":      date_join,
                "date_where":     date_where,
                "level_where":    level_where,
                "category_join":  "" if skip_category_join else cat_join,
                "category_where": "" if skip_category_where else cat_where,
                "company_join":   "" if skip_company_join else comp_join,
                "company_where":  comp_where,
            }

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=8,
            )

            # Q1 — total jobs for this skill (+ all active filters)
            total_rows = _run(conn, _SQL_TOTAL_JOBS.format(**_base_params()))
            total_jobs = int(total_rows[0]["total"]) if total_rows else 0

            if total_jobs == 0:
                logger.info(
                    "Market context: 0 jobs found for skill='%s' — skipping.",
                    primary_skill,
                )
                conn.close()
                return None

            # Q2 — top companies
            # dim_company is already joined inside _SQL_TOP_COMPANIES, so skip comp_join
            # (it would cause a duplicate JOIN if comp_join is also set from company filter).
            # Instead, company_where still applies because the alias dc is always present.
            company_rows = _run(conn, _SQL_TOP_COMPANIES.format(
                **_base_params(skip_company_join=True)
            ))
            top_companies = [
                f"{r['company_name']} ({r['job_count']})"
                for r in company_rows
            ]

            # Q3 — work mode distribution
            # dim_work_mode is already JOINed inside _SQL_WORK_MODE (hardcoded).
            # We skip work_mode_join (duplicate) AND skip work_mode_where (show full distribution).
            mode_rows = _run(conn, _SQL_WORK_MODE.format(
                **_base_params(skip_work_mode_join=True, skip_work_mode_where=True)
            ))
            work_mode_dist = {
                r["work_mode"]: int(r["cnt"])
                for r in mode_rows
                if r["work_mode"]
            }

            # Q4 — related / co-occurring skills
            skill_rows = _run(conn, _SQL_RELATED_SKILLS.format(**_base_params()))
            related_skills = [r["skill_name"] for r in skill_rows]

            # Q5 — job category distribution
            # dim_job_category is already JOINed inside _SQL_CATEGORY_DIST (hardcoded).
            # Skip category_join (duplicate) AND category_where (circular — grouped BY category).
            cat_rows = _run(conn, _SQL_CATEGORY_DIST.format(
                **_base_params(skip_category_join=True, skip_category_where=True)
            ))
            category_dist = {r["category_name"]: int(r["job_count"]) for r in cat_rows}

            # Q6 — regional distribution
            # _SQL_REGION_DIST already JOINs dim_location internally (hardcoded).
            # We must NOT pass location_join or region_join (would create duplicate dl alias).
            # We also skip region_where here (we're grouping by region anyway) but
            # keep location_where so city filter can still narrow the region breakdown.
            region_params = {
                "skill":          safe_skill,
                "work_mode_join": wm_join,
                "work_mode_where": wm_where,
                "date_join":      date_join,
                "date_where":     date_where,
                "level_where":    level_where,
                "category_join":  cat_join,
                "category_where": cat_where,
                "company_join":   comp_join,
                "company_where":  comp_where,
            }
            region_rows = _run(conn, _SQL_REGION_DIST.format(**region_params))
            region_dist = {r["region"]: int(r["job_count"]) for r in region_rows if r["region"]}

            conn.close()

            insight = MarketInsight(
                primary_skill    = primary_skill,
                total_jobs       = total_jobs,
                top_companies    = top_companies,
                work_mode_dist   = work_mode_dist,
                related_skills   = related_skills,
                location_filter  = city_name,
                work_mode_filter = _WORK_MODE_CANONICAL.get(work_mode_key) if work_mode_key else None,
                level_filter     = _extract_level_label(query),
                region_filter    = _extract_region_label(query),
                date_filter      = _extract_date_label(query),
                category_filter  = _extract_category_label(query),
                company_filter   = _extract_company_label(query, self._company_list),
                category_dist    = category_dist,
                region_dist      = region_dist,
            )

            logger.info(
                "Market context ready | skill=%s | total=%d | companies=%d | modes=%s",
                primary_skill, total_jobs, len(top_companies),
                list(work_mode_dist.keys()),
            )
            return insight

        except Exception as exc:
            # Non-fatal — RAG pipeline continues without market context
            logger.warning("Market context unavailable (Trino error): %s", exc)
            return None


def format_market_block(insight: MarketInsight) -> str:
    """
    @brief Serialise a MarketInsight into a plain-text block for LLM prompt injection.

    Uses structured plain text (not Markdown) so the LLM can incorporate the
    data naturally into its response without rendering artefacts.

    @param insight  Populated MarketInsight returned by MarketContextService.get_insight().
    @return         Formatted string block delimited by === markers, ready for prompt injection.
    """
    # Build human-readable filter label so LLM knows exactly what total_jobs represents.
    # Order: location → region → work_mode → level → category → company → date
    active_filters: list[str] = []
    if insight.location_filter:
        active_filters.append(insight.location_filter)
    if insight.region_filter:
        _region_vi = {"South": "miền Nam", "North": "miền Bắc", "Central": "miền Trung"}
        active_filters.append(_region_vi.get(insight.region_filter, insight.region_filter))
    if insight.work_mode_filter:
        active_filters.append(insight.work_mode_filter)
    if insight.level_filter:
        active_filters.append(insight.level_filter)
    if insight.category_filter:
        active_filters.append(insight.category_filter)
    if insight.company_filter:
        active_filters.append(f"công ty {insight.company_filter}")
    if insight.date_filter:
        active_filters.append(insight.date_filter)
    filter_desc = " · ".join(active_filters) if active_filters else "toàn quốc"

    loc_str = f" tại {insight.location_filter}" if insight.location_filter else ""

    mode_parts = [
        f"{mode} {cnt}"
        for mode, cnt in sorted(
            insight.work_mode_dist.items(), key=lambda x: x[1], reverse=True
        )
    ]

    lines = [
        f"=== MARKET CONTEXT — {insight.primary_skill}{loc_str} (Gold layer, Iceberg) ===",
        f"Tổng vị trí trong database (filter đang áp dụng: {filter_desc}): {insight.total_jobs}",
    ]
    if insight.top_companies:
        co_note = f" (chỉ công ty '{insight.company_filter}')" if insight.company_filter else ""
        lines.append(f"Top công ty tuyển dụng{co_note}: {', '.join(insight.top_companies[:5])}")
    if mode_parts:
        wm_note = " — phân bổ toàn thị trường, không filter work_mode" if insight.work_mode_filter else ""
        lines.append(f"Work mode distribution{wm_note}: {' | '.join(mode_parts)}")
    if insight.category_dist:
        cat_note = " — phân bổ toàn ngành, không filter category" if insight.category_filter else ""
        cat_parts = [f"{k}: {v}" for k, v in list(insight.category_dist.items())[:4]]
        lines.append(f"Ngành nghề chính{cat_note}: {' | '.join(cat_parts)}")
    if insight.region_dist:
        region_note = " — phân bổ toàn quốc, không filter region/city" if (insight.location_filter or insight.region_filter) else ""
        region_parts = [
            f"{k}: {v}"
            for k, v in sorted(insight.region_dist.items(), key=lambda x: x[1], reverse=True)
        ]
        lines.append(f"Phân bổ khu vực{region_note}: {' | '.join(region_parts)}")
    if insight.related_skills:
        lines.append(f"Kỹ năng hay đi kèm: {', '.join(insight.related_skills[:6])}")

    # Build LLM instruction with full context awareness
    example_desc = f"{insight.primary_skill}"
    if insight.level_filter:
        example_desc = f"{insight.level_filter} {example_desc}"
    if insight.location_filter:
        example_desc += f" tại {insight.location_filter}"
    elif insight.region_filter:
        _region_vi2 = {"South": "miền Nam", "North": "miền Bắc", "Central": "miền Trung"}
        example_desc += f" tại {_region_vi2.get(insight.region_filter, insight.region_filter)}"
    if insight.work_mode_filter:
        example_desc += f" ({insight.work_mode_filter})"
    if insight.category_filter:
        example_desc += f" [{insight.category_filter}]"
    if insight.company_filter:
        example_desc += f" tại công ty {insight.company_filter}"
    if insight.date_filter:
        example_desc += f" — {insight.date_filter}"

    lines.append(
        "INSTRUCTION: After the job cards, add a market summary using ONLY the numbers above. "
        f"The total ({insight.total_jobs}) already applies ALL active filters ({filter_desc}). "
        f"Describe it as the count for that specific context, e.g. 'Có {insight.total_jobs} vị trí {example_desc}'. "
        "Distribution fields (Work mode, Ngành nghề, Phân bổ khu vực) show the BROADER market without their respective filter applied — "
        "use them to give context ('phần lớn là at-office', 'chủ yếu ở Backend Development') but do NOT add their counts to total. "
        "NEVER invent salary figures or company names not listed above. 2-3 sentences max."
    )
    lines.append("=== END MARKET CONTEXT ===")

    return "\n".join(lines)
