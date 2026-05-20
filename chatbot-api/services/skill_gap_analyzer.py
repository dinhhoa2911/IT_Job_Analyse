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
from models.schemas import CVProfile, SkillGap, SkillGapAnalysis

logger = logging.getLogger(__name__)

_FALLBACK_CATEGORY = "Software Engineering"

# Maps target role keywords → Gold dim_job_category.category_name
_ROLE_CATEGORY_MAP: list[tuple[str, list[str]]] = [
    ("Data & AI",           ["data engineer", "data scientist", "data analyst",
                              "machine learning", "ml engineer", "ai engineer", "data"]),
    ("DevOps & Infra",      ["devops", "sre", "cloud engineer", "platform engineer",
                              "infrastructure", "system engineer"]),
    ("Testing & QA",        ["qa", "qc", "tester", "automation test", "quality"]),
    ("Frontend & Mobile",   ["frontend", "front-end", "mobile", "android", "ios",
                              "react native", "flutter", "ui developer"]),
    ("Backend",             ["backend", "back-end", "java developer", "python developer",
                              "node", "php", "golang", ".net developer", "spring"]),
    ("Software Engineering",["fullstack", "full stack", "software engineer"]),
    ("Management",          ["engineering manager", "tech lead", "team lead", "head of"]),
    ("Product & BA",        ["product manager", "product owner", "business analyst", "scrum"]),
]


def _target_roles_to_categories(preferred_roles: list[str]) -> list[str]:
    """Map CV preferred_roles → ordered, deduplicated Gold category list (max 3)."""
    categories: list[str] = []
    seen: set[str] = set()
    for role in preferred_roles:
        role_lower = role.lower()
        for category, keywords in _ROLE_CATEGORY_MAP:
            if any(kw in role_lower for kw in keywords):
                if category not in seen:
                    categories.append(category)
                    seen.add(category)
                break
    return categories[:3]

# ── SQL templates ─────────────────────────────────────────────────────────────

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
    return s.replace("'", "''")


def _tokens(skill: str) -> frozenset[str]:
    """Split a skill name into lowercase word tokens.
    e.g. 'Spring Boot' → {'spring','boot'}, 'Node.js' → {'node','js'}, 'RESTful API' → {'restful','api'}
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
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _infer_category_from_db(
    conn: trino.dbapi.Connection,
    skills: list[str],
) -> str:
    """
    @brief Data-driven category inference: ask Gold layer which category
    requires the most of the candidate's actual skills.

    Builds a Trino IN-list from the CV's skill names and counts how many
    distinct job postings per category require those skills.  The category
    with the highest count wins — no hard-coded keyword lists.

    Falls back to _FALLBACK_CATEGORY when the query returns no rows
    (e.g. all CV skills are absent from dim_skill, or Trino has no data).

    @param conn    Open Trino connection (reused from the caller's block).
    @param skills  CV skill list (up to 20 used; extras ignored for query size).
    @return        Gold dim_job_category.category_name string.
    """
    if not skills:
        return _FALLBACK_CATEGORY

    # Build SQL-safe IN-list: LOWER each skill, escape quotes, wrap in single quotes
    in_list = ", ".join(
        f"'{_escape(s.strip().lower())}'"
        for s in skills[:20]   # cap at 20 to keep query short
        if s.strip()
    )
    if not in_list:
        return _FALLBACK_CATEGORY

    rows = _run(conn, _SQL_INFER_CATEGORY.format(skills=in_list))
    if rows:
        category = rows[0]["category_name"]
        logger.info("Category inferred from Gold layer: '%s'", category)
        return category

    logger.info("Category inference returned no rows — using fallback '%s'.", _FALLBACK_CATEGORY)
    return _FALLBACK_CATEGORY


# ── Main service ──────────────────────────────────────────────────────────────

class SkillGapAnalyzerService:
    """
    @class SkillGapAnalyzerService
    @brief Query the Gold star schema to identify skill gaps for a given CVProfile.

    Stateless — no models to load, no shared state.  Instantiated once per
    application startup (lazy, from the cv_match router).
    """

    def _analyze_one(
        self,
        conn: trino.dbapi.Connection,
        category: str,
        cv_skills: list[str],
    ) -> SkillGapAnalysis | None:
        """Run skill gap analysis for a single category against an open Trino connection."""
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

            # Step 1: categories from target roles (highest priority)
            categories = _target_roles_to_categories(profile.preferred_roles)

            # Step 2: fill with data-driven category if still empty or only 1
            if len(categories) < 2:
                db_cat = _infer_category_from_db(conn, profile.skills)
                if db_cat not in categories:
                    categories.append(db_cat)

            # Cap at 3 unique categories
            categories = list(dict.fromkeys(categories))[:3]

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
        """Backward-compat wrapper — returns only the first result."""
        results = self.analyze_multi(profile)
        return results[0] if results else None
