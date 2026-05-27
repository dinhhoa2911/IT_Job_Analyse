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

Three Trino queries are executed (all against Gold star schema):
  Q1 – top companies     : fact ⋈ dim_skill ⋈ dim_company   → top 5 companies by job count
  Q2 – work mode spread  : fact ⋈ dim_skill ⋈ dim_work_mode → distribution (remote/hybrid/office)
  Q3 – related skills    : subquery  finds jobs requiring primary skill,
                           then counts co-occurring skills in same job set

Why three separate queries instead of one big JOIN?
  The fact table is exploded (1 row per skill × location combination).
  Joining skill + company + work_mode in one query causes row multiplication
  that inflates counts. Three focused queries on DISTINCT job_link are clean
  and correct.

Failure contract:
  All three queries are wrapped in a single try/except.  If Trino is
  unavailable or any query fails, this service returns None silently.
  The pipeline continues without market context rather than blocking the user.
"""

import logging
import re
from collections import Counter

import trino

from config import settings
from constants import LOCATION_CITY_NAME
from models.schemas import JobResult, MarketInsight

logger = logging.getLogger(__name__)

# Location key → Gold city name: imported from constants.LOCATION_CITY_NAME

# ── SQL templates ─────────────────────────────────────────────────────────────

_SQL_TOP_COMPANIES = """
SELECT
    dc.company_name,
    COUNT(DISTINCT f.job_link) AS job_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill    ds ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_company  dc ON f.company_id  = dc.company_id
{location_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
GROUP BY dc.company_name
ORDER BY job_count DESC
LIMIT 5
"""

_SQL_TOTAL_JOBS = """
SELECT COUNT(DISTINCT f.job_link) AS total
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
{location_join}
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
{location_where}
"""

_SQL_WORK_MODE = """
SELECT
    dwm.work_mode,
    COUNT(DISTINCT f.job_link) AS cnt
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill     ds  ON f.skill_id = ds.skill_id
JOIN iceberg.gold.dim_work_mode dwm ON f.mode_id  = dwm.mode_id
WHERE LOWER(ds.skill_name) = LOWER('{skill}')
GROUP BY dwm.work_mode
ORDER BY cnt DESC
"""

_SQL_RELATED_SKILLS = """
SELECT
    ds.skill_name,
    COUNT(DISTINCT f.job_link) AS co_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
WHERE f.job_link IN (
    SELECT DISTINCT f2.job_link
    FROM iceberg.gold.fact_job_posting f2
    JOIN iceberg.gold.dim_skill ds2 ON f2.skill_id = ds2.skill_id
    WHERE LOWER(ds2.skill_name) = LOWER('{skill}')
)
AND LOWER(ds.skill_name) != LOWER('{skill}')
GROUP BY ds.skill_name
ORDER BY co_count DESC
LIMIT 6
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
    @return      Tuple of (JOIN clause string, WHERE clause string); both empty if city is None.
    """
    if not city:
        return "", ""
    safe = _escape(city)
    join  = "JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id"
    where = f"AND LOWER(dl.city_name) LIKE LOWER('%{safe}%')"
    return join, where


def _extract_from_jobs(jobs: list[JobResult]) -> str | None:
    """
    @brief Pick the primary skill by frequency across returned job text_content fields.

    Parses the "Skills:" section of each JobResult's text_content, tallies
    occurrences, and returns the most common skill name.

    @param jobs  Reranked JobResult list from hybrid search.
    @return      Most frequent skill string, or None if no Skills section is found.
    """
    skill_counter: Counter = Counter()
    for job in jobs:
        match = re.search(r"Skills:\s*([^.]+)", job.text_content, re.IGNORECASE)
        if match:
            for s in match.group(1).split(","):
                s = s.strip()
                if s:
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

_SQL_LOAD_SKILLS = "SELECT skill_name FROM iceberg.gold.dim_skill"


class MarketContextService:
    """
    @class MarketContextService
    @brief Query the Gold star schema (Trino/Iceberg) and return real-time market intelligence.

    Runs up to four focused Trino queries (total jobs, top companies, work-mode
    distribution, related co-occurring skills) and packages the results into a
    MarketInsight object.  All queries are wrapped in a single try/except block —
    on any Trino failure the service returns None silently so the RAG pipeline
    can continue without market context.

    At construction time, the full skill list is loaded from dim_skill and cached
    so the query-fallback skill detector never needs a hard-coded list.
    """

    def __init__(self) -> None:
        # lowercase → original-case mapping built from Gold dim_skill.
        # Populated lazily on first get_insight() call (so logging is ready).
        self._skill_lookup: dict[str, str] = {}
        self._skills_loaded: bool = False

    def _load_skills(self) -> None:
        """
        @brief Load all skill_name values from iceberg.gold.dim_skill into memory.

        Called once at startup.  Fails silently — if Trino is unavailable the
        fallback strategy will simply return None instead of a skill name, which
        is acceptable (market context is optional and non-blocking).
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
            # Build lookup sorted by skill name length (longest first) so that
            # multi-word skills like "React Native" are checked before "React".
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
        filters: dict,          # {work_mode: str|None, location: str|None}
    ) -> MarketInsight | None:
        """
        @brief Main entry point — called from RAGPipeline after vector search completes.

        @param jobs     Reranked job results used to detect the primary skill.
        @param query    Original user query string (skill detection fallback).
        @param filters  Dict with keys ``work_mode`` and ``location`` (values may be None).
        @return         Populated MarketInsight, or None if Trino is unavailable or no skill found.
        """
        # Lazy-load skill list on first real request (logging is ready by then)
        if not self._skills_loaded:
            self._load_skills()
            self._skills_loaded = True

        primary_skill = self._extract_primary_skill(jobs, query)
        if not primary_skill:
            logger.info("Market context skipped: no primary skill detected.")
            return None

        location_key  = filters.get("location")
        city_name     = LOCATION_CITY_NAME.get(location_key) if location_key else None
        loc_join, loc_where = _build_location_clauses(city_name)

        safe_skill = _escape(primary_skill)

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=8,   # fail fast if Trino is slow
            )

            # Q1 — total jobs for this skill (+ optional location)
            total_rows = _run(conn, _SQL_TOTAL_JOBS.format(
                skill=safe_skill,
                location_join=loc_join,
                location_where=loc_where,
            ))
            total_jobs = int(total_rows[0]["total"]) if total_rows else 0

            if total_jobs == 0:
                logger.info(
                    "Market context: 0 jobs found for skill='%s' — skipping.",
                    primary_skill,
                )
                conn.close()
                return None

            # Q2 — top companies
            company_rows = _run(conn, _SQL_TOP_COMPANIES.format(
                skill=safe_skill,
                location_join=loc_join,
                location_where=loc_where,
            ))
            top_companies = [
                f"{r['company_name']} ({r['job_count']})"
                for r in company_rows
            ]

            # Q3 — work mode distribution
            mode_rows = _run(conn, _SQL_WORK_MODE.format(skill=safe_skill))
            work_mode_dist = {
                r["work_mode"]: int(r["cnt"])
                for r in mode_rows
                if r["work_mode"]
            }

            # Q4 — related / co-occurring skills
            skill_rows = _run(conn, _SQL_RELATED_SKILLS.format(skill=safe_skill))
            related_skills = [r["skill_name"] for r in skill_rows]

            conn.close()

            insight = MarketInsight(
                primary_skill   = primary_skill,
                total_jobs      = total_jobs,
                top_companies   = top_companies,
                work_mode_dist  = work_mode_dist,
                related_skills  = related_skills,
                location_filter = city_name,
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
    loc_str = f" tại {insight.location_filter}" if insight.location_filter else ""

    mode_parts = [
        f"{mode} {cnt}"
        for mode, cnt in sorted(
            insight.work_mode_dist.items(), key=lambda x: x[1], reverse=True
        )
    ]

    lines = [
        f"=== MARKET CONTEXT — {insight.primary_skill}{loc_str} (Gold layer, Iceberg) ===",
        f"Tổng vị trí trong database: {insight.total_jobs}",
        f"Top công ty tuyển dụng: {', '.join(insight.top_companies[:4])}",
    ]
    if mode_parts:
        lines.append(f"Work mode: {' | '.join(mode_parts)}")
    if insight.related_skills:
        lines.append(f"Kỹ năng hay đi kèm: {', '.join(insight.related_skills[:5])}")
    lines.append(
        "INSTRUCTION: After the job cards, add a short market summary paragraph "
        "('Thị trường [skill]...') using ONLY the market data above. 2-3 sentences max."
    )
    lines.append("=== END MARKET CONTEXT ===")

    return "\n".join(lines)
