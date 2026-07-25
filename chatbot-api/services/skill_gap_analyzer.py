"""
@file skill_gap_analyzer.py
@brief Skill Gap Analyzer — compare a CV's skills against Gold-layer market demand.

Given a CVProfile, this service queries the Gold star schema (Trino/Iceberg) to find
the top skills required for the candidate's target role category, then identifies:
  - present_skills    : CV skills already in the market's top 15
  - missing_skills    : market top skills the CV does NOT cover (with frequency %)
  - cv_coverage_score : % of top-15 market skills covered by the CV

Why does this need the Lakehouse?
  All three category-inference, ranking, and comparison steps depend on the
  Gold star schema (fact_job_posting ⋈ dim_skill ⋈ dim_job_category) — impossible
  with raw Silver data.

Queries executed (against iceberg.gold.*):
  Q0 — infer target category from CV skills (data-driven, no hard-coded patterns)
  Q1 — total distinct jobs for that category (baseline denominator)
  Q2 — top-20 skills ranked by job_count for that category

Failure contract:
  All Trino queries are wrapped in try/except.  Returns None silently if Trino
  is unavailable so the CV-match pipeline is never blocked.
"""

import logging
import re

import trino

from config import settings
from constants import SKILL_QUERY_ALIASES
from models.schemas import CVProfile, SkillGap, SkillGapAnalysis

logger = logging.getLogger(__name__)


def _normalize_skill(s: str) -> str:
    """Normalize a user-typed skill to its canonical dim_skill name."""
    low = s.strip().lower()
    return SKILL_QUERY_ALIASES.get(low, low)


# Direct role-keyword → category mapping (avoids LIKE mismatches on job titles
# like "Full Stack" vs "Fullstack"). Checked before the LIKE fallback.
_ROLE_CATEGORY_MAP: dict[str, str] = {
    "backend": "Backend Development", "back-end": "Backend Development",
    "back end": "Backend Development",
    "frontend": "Frontend Development", "front-end": "Frontend Development",
    "front end": "Frontend Development",
    "fullstack": "Fullstack Development", "full-stack": "Fullstack Development",
    "full stack": "Fullstack Development",
    "mobile": "Mobile Development", "android": "Mobile Development",
    "ios": "Mobile Development",
    "devops": "DevOps & Infrastructure", "sre": "DevOps & Infrastructure",
    "data engineer": "Data Engineering", "data pipeline": "Data Engineering",
    "data analyst": "Data Analytics", "analytics": "Data Analytics",
    "machine learning": "AI & Machine Learning", "ml engineer": "AI & Machine Learning",
    "data science": "AI & Machine Learning",
    "qa": "Testing & QA", "tester": "Testing & QA", "testing": "Testing & QA",
    "security": "Cyber Security", "cybersecurity": "Cyber Security",
    "embedded": "Embedded & IoT", "iot": "Embedded & IoT",
    "game": "Game Development",
    "erp": "ERP & CRM", "crm": "ERP & CRM",
    "product manager": "Product & Business Analysis",
    "business analyst": "Product & Business Analysis",
    "manager": "Management", "management": "Management",
}


def _infer_category_from_keywords(role: str) -> str | None:
    """Match role string against known keywords (longest first)."""
    role_lower = role.lower()
    for keyword in sorted(_ROLE_CATEGORY_MAP, key=len, reverse=True):
        if keyword in role_lower:
            return _ROLE_CATEGORY_MAP[keyword]
    return None


# ── SQL templates ─────────────────────────────────────────────────────────────

# Q-roles: match preferred_roles against actual job titles in fact_job_posting
# → find which Gold categories those roles belong to (fully data-driven, no hard-coded map)
_SQL_INFER_CATEGORIES_FROM_ROLES = """
SELECT
    djc.category_name,
    COUNT(DISTINCT f.job_link) AS cnt
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE {role_conditions}
GROUP BY djc.category_name
ORDER BY cnt DESC
LIMIT 3
"""

_SQL_HAS_IT_SKILLS = """
SELECT COUNT(DISTINCT ds.skill_id) AS matched
FROM iceberg.gold.dim_skill ds
WHERE LOWER(ds.skill_name) IN ({skills})
"""

# Q0: data-driven category inference — find which job category requires the most
# of the candidate's actual skills.  No hard-coded keyword lists needed.
_SQL_INFER_CATEGORY = """
SELECT
    djc.category_name,
    COUNT(DISTINCT f.job_link) AS cnt
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill        ds  ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE LOWER(ds.skill_name) IN ({skills})
GROUP BY djc.category_name
ORDER BY cnt DESC
LIMIT 1
"""

_SQL_TOTAL_JOBS = """
SELECT COUNT(DISTINCT f.job_link) AS total
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE LOWER(djc.category_name) = LOWER('{category}')
"""

_SQL_TOP_SKILLS = """
SELECT
    ds.skill_name,
    ds.skill_group,
    COUNT(DISTINCT f.job_link) AS job_count
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill        ds  ON f.skill_id    = ds.skill_id
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE LOWER(djc.category_name) = LOWER('{category}')
GROUP BY ds.skill_name, ds.skill_group
ORDER BY job_count DESC
LIMIT 20
"""


def _escape(s: str) -> str:
    """
    @brief Escape single quotes in a string for safe embedding in Trino SQL literals.

    @param s  Raw string value.
    @return   String with single quotes doubled (SQL standard escaping).
    """
    return s.replace("'", "''")


def _tokens(skill: str) -> frozenset[str]:
    """
    @brief Split a skill name into lowercase word tokens for fuzzy matching.

    @param skill  Skill name string (e.g. "Spring Boot", "Node.js").
    @return       Frozenset of lowercase tokens (splits on whitespace, dots, dashes, slashes, +, #).
    """
    return frozenset(t for t in re.split(r'[\s.\-/+#]+', skill.lower()) if t)


def _skill_in_cv(market_skill: str, cv_skills: list[str]) -> bool:
    """
    @brief Check if a market skill is covered by any skill in the CV.

    Uses word-token subset matching so partial names match correctly:
      'Spring'    ⊆ tokens('Spring Boot')     → True  ✓
      'API'       ⊆ tokens('RESTful API')      → True  ✓
      'Node'      ⊆ tokens('Node.js')          → True  ✓
      'React'     ⊆ tokens('React.js')         → True  ✓
      'Java'      ⊆ tokens('JavaScript')       → False ✓  (javascript is one token)
      'SQL'       ⊆ tokens('MySQL')            → False ✓  (mysql is one token)
      'C'         ⊆ tokens('C++')              → False ✓  (c++ is one token)
    """
    m_tokens = _tokens(market_skill)
    if not m_tokens:
        return False
    for cv_s in cv_skills:
        cv_tok = _tokens(cv_s)
        if m_tokens.issubset(cv_tok):
            return True
    return False


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


def _infer_category_from_db(
    conn: trino.dbapi.Connection,
    skills: list[str],
) -> str | None:
    """
    @brief Data-driven category inference: ask Gold layer which category
    requires the most of the candidate's actual skills.

    Builds a Trino IN-list from the CV's skill names and counts how many
    distinct job postings per category require those skills.  The category
    with the highest count wins — no hard-coded keyword lists.

    Returns None when the query yields no rows, meaning none of the CV's
    skills exist in dim_skill — the caller must treat this as a non-IT CV
    rather than silently falling back to a generic category.

    @param conn    Open Trino connection (reused from the caller's block).
    @param skills  CV skill list (up to 20 used; extras ignored for query size).
    @return        Gold dim_job_category.category_name string, or None if no
                   CV skill matches any IT skill in the Gold layer.
    """
    if not skills:
        return None

    # Build SQL-safe IN-list: normalize aliases (nodejs→node.js, golang→go, etc.)
    in_list = ", ".join(
        f"'{_escape(_normalize_skill(s))}'"
        for s in skills[:20]
        if s.strip()
    )
    if not in_list:
        return None

    rows = _run(conn, _SQL_INFER_CATEGORY.format(skills=in_list))
    if rows:
        category = rows[0]["category_name"]
        logger.info("Category inferred from Gold layer: '%s'", category)
        return category

    logger.warning(
        "Category inference: none of the CV's skills found in Gold dim_skill — "
        "this CV may not be IT-related."
    )
    return None


def _infer_categories_from_roles_db(
    conn: trino.dbapi.Connection,
    preferred_roles: list[str],
) -> list[str]:
    """
    @brief Data-driven role-to-category inference via job title matching in fact_job_posting.

    Matches each preferred_role against actual job titles stored in the Gold layer
    and returns the categories with the most matching jobs — no hard-coded keyword
    lists required.

    Example: preferred_roles=["Data Engineer", "ML Engineer"]
      → LOWER(f.job_title) LIKE '%data engineer%' OR LIKE '%ml engineer%'
      → "Data & AI" wins with 312 jobs → returned first

    @param conn             Open Trino connection (reused from the caller's block).
    @param preferred_roles  Role strings from CVProfile (up to 5 used).
    @return                 Ordered list of up to 3 Gold category_name strings.
                            Empty list if preferred_roles is empty or no titles match.
    """
    if not preferred_roles:
        return []

    # Strategy 1: direct keyword mapping (handles fullstack/full-stack/full stack)
    keyword_cats: list[str] = []
    for role in preferred_roles[:5]:
        cat = _infer_category_from_keywords(role)
        if cat and cat not in keyword_cats:
            keyword_cats.append(cat)
    if keyword_cats:
        logger.info("Role-keyword category inference → %s", keyword_cats)
        return keyword_cats[:3]

    # Strategy 2: LIKE fallback on job titles
    conditions = [
        f"LOWER(f.job_title) LIKE '%{_escape(r.strip().lower())}%'"
        for r in preferred_roles[:5]
        if r.strip()
    ]
    if not conditions:
        return []

    rows = _run(conn, _SQL_INFER_CATEGORIES_FROM_ROLES.format(
        role_conditions=" OR ".join(conditions)
    ))
    categories = [r["category_name"] for r in rows if r.get("category_name")]
    logger.info("Role-title category inference (LIKE) → %s", categories)
    return categories


# ── Main service ──────────────────────────────────────────────────────────────

class SkillGapAnalyzerService:
    """
    @class SkillGapAnalyzerService
    @brief Query the Gold star schema to identify skill gaps for a given CVProfile.

    Stateless — no models to load, no shared state.  Instantiated once per
    application startup (lazy, from the cv_match router).
    """

    def has_it_skills(self, skills: list[str]) -> bool | None:
        """
        @brief Check whether any of the given skills exist in Gold dim_skill.

        Used as a data-driven gate before running vector search — if the CV has
        zero skills that appear in the IT job corpus, it is almost certainly not
        an IT CV and should be rejected early.

        @param skills  Raw skill strings from CVProfile (before language merge).
        @return        True if ≥1 skill found, False if 0 found, None if Trino
                       is unavailable (caller should fall back to LLM-only check).
        """
        if not skills:
            return False

        in_list = ", ".join(
            f"'{_escape(_normalize_skill(s))}'"
            for s in skills[:30]
            if s.strip()
        )
        if not in_list:
            return False

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=8,
            )
            rows = _run(conn, _SQL_HAS_IT_SKILLS.format(skills=in_list))
            conn.close()
            matched = int(rows[0]["matched"]) if rows else 0
            logger.info(
                "IT skill gate | checked=%d skills → %d matched in dim_skill",
                len(skills), matched,
            )
            return matched > 0
        except Exception as exc:
            logger.warning("IT skill gate Trino unavailable: %s — skipping gate.", exc)
            return None  # caller treats None as "unknown", falls back to LLM check

    def _analyze_one(
        self,
        conn: trino.dbapi.Connection,
        category: str,
        cv_skills: list[str],
    ) -> SkillGapAnalysis | None:
        """
        @brief Run skill gap analysis for a single Gold category on an open Trino connection.

        @param conn       Active Trino connection (reused from the caller's block).
        @param category   Gold dim_job_category.category_name to analyze.
        @param cv_skills  List of skill strings from the candidate's CVProfile.
        @return           Populated SkillGapAnalysis, or None if the category has no data.
        """
        total_rows = _run(conn, _SQL_TOTAL_JOBS.format(category=_escape(category)))
        total_jobs = int(total_rows[0]["total"]) if total_rows else 0
        if total_jobs == 0:
            return None

        skill_rows = _run(conn, _SQL_TOP_SKILLS.format(category=_escape(category)))
        if not skill_rows:
            return None

        all_market: list[SkillGap] = []
        present: list[str] = []
        missing: list[SkillGap] = []

        for row in skill_rows:
            name  = row["skill_name"]
            count = int(row["job_count"])
            group = row.get("skill_group") or "Other"
            freq  = round((count / total_jobs) * 100, 1)
            gap   = SkillGap(skill_name=name, market_frequency=freq,
                             job_count=count, skill_group=group)
            all_market.append(gap)
            if _skill_in_cv(name, cv_skills):
                present.append(name)
            else:
                missing.append(gap)

        top15    = all_market[:15]
        covered  = sum(1 for g in top15 if _skill_in_cv(g.skill_name, cv_skills))
        coverage = round((covered / len(top15)) * 100, 1) if top15 else 0.0

        logger.info(
            "Skill gap ready | category=%s | total=%d | coverage=%.1f%% | missing=%d",
            category, total_jobs, coverage, len(missing),
        )
        return SkillGapAnalysis(
            role_category       = category,
            total_jobs_analyzed = total_jobs,
            cv_coverage_score   = coverage,
            cv_skill_count      = len(cv_skills),
            present_skills      = present,
            missing_skills      = missing[:10],
            all_market_skills   = top15,
        )

    def analyze_multi(self, profile: CVProfile) -> list[SkillGapAnalysis]:
        """
        @brief Run skill gap analysis for up to 3 categories derived from CV target roles.

        Priority:
          1. Categories mapped from profile.preferred_roles (e.g. "Data Engineer" → "Data & AI")
          2. Data-driven category from Gold layer (fills remaining slots up to max 3)

        @return  List of SkillGapAnalysis, one per unique category (max 3). Empty on error.
        """
        if not profile.skills:
            logger.info("Skill gap skipped: CV has no skills.")
            return []

        logger.info("Skill gap | candidate=%s | skills=%d", profile.name or "unknown", len(profile.skills))

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=15,
            )

            # Step 1: match preferred_roles against job titles in Gold layer
            categories = _infer_categories_from_roles_db(conn, profile.preferred_roles)

            # Step 2: fill remaining slots with skill-based inference
            if len(categories) < 2:
                db_cat = _infer_category_from_db(conn, profile.skills)
                if db_cat and db_cat not in categories:
                    categories.append(db_cat)

            # Cap at 3 unique categories
            categories = list(dict.fromkeys(categories))[:3]

            if not categories:
                logger.warning(
                    "Skill gap skipped: no IT skills from this CV found in Gold layer "
                    "and no preferred_roles matched any known IT category. "
                    "CV may not be IT-related."
                )
                conn.close()
                return []

            results: list[SkillGapAnalysis] = []
            for cat in categories:
                result = self._analyze_one(conn, cat, profile.skills)
                if result:
                    results.append(result)

            conn.close()
            return results

        except Exception as exc:
            logger.warning("Skill gap analysis unavailable (Trino error): %s", exc)
            return []

    def analyze(self, profile: CVProfile) -> SkillGapAnalysis | None:
        """
        @brief Backward-compatible single-result wrapper around analyze_multi().

        @param profile  CVProfile to analyze.
        @return         First SkillGapAnalysis result, or None if analysis returns no data.
        """
        results = self.analyze_multi(profile)
        return results[0] if results else None
