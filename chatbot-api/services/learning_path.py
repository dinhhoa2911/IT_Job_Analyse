"""
@file learning_path.py
@brief Data-driven skill learning path service — Gold layer via Trino.

Given a target role and the user's existing skills, this service queries the
Gold star schema to recommend the TOP-10 skills to learn next, ranked by:

  rank_score = 0.85 × market_freq + 0.15 × bridge_score

  market_freq   = % of target-category jobs requiring this skill
  bridge_score  = % of target-category jobs that require BOTH one of the
                  user's known skills AND this skill

Supports optional filters extracted from the target_role string:
  - level     : senior / junior / fresher / lead / etc.
  - work_mode : remote / hybrid
  - location  : HCM / Ha Noi / Da Nang / etc.

When the filtered pool is too small (< 10 jobs), falls back to the broader
category-only pool automatically.
"""

import logging
import re

import trino

from config import settings
from constants import LOCATION_ALIAS_TO_KEY, LOCATION_CITY_NAME, SKILL_QUERY_ALIASES
from models.schemas import LearningPathResult, LearningPathStep

logger = logging.getLogger(__name__)


def _normalize_skill(s: str) -> str:
    """Normalize a user-typed skill to its canonical dim_skill name."""
    low = s.strip().lower()
    return SKILL_QUERY_ALIASES.get(low, low)

# ── Filter extraction helpers ────────────────────────────────────────────────

_LEVEL_TOKENS = [
    "principal", "staff", "lead", "senior", "mid-level", "mid level",
    "middle", "junior", "fresher", "intern", "entry",
]

_WORK_MODE_KEYWORDS: dict[str, str] = {
    "remote": "Remote", "từ xa": "Remote", "wfh": "Remote",
    "hybrid": "Hybrid", "kết hợp": "Hybrid",
}

_LEVEL_ENTRY_SYNS = {"fresher", "intern", "junior", "entry"}


def _extract_level(role: str) -> str | None:
    q = role.lower()
    for token in _LEVEL_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", q):
            return token
    return None


def _extract_work_mode(role: str) -> str | None:
    q = role.lower()
    for kw, canonical in _WORK_MODE_KEYWORDS.items():
        if re.search(rf"\b{re.escape(kw)}\b", q):
            return canonical
    return None


def _extract_location(role: str) -> str | None:
    q = role.lower()
    for alias, key in sorted(LOCATION_ALIAS_TO_KEY.items(), key=lambda x: len(x[0]), reverse=True):
        if alias in q:
            return LOCATION_CITY_NAME.get(key)
    return None


def _build_extra_joins_where(
    level: str | None, work_mode: str | None, location: str | None,
) -> tuple[str, str]:
    joins, wheres = [], []
    if work_mode:
        joins.append("JOIN iceberg.gold.dim_work_mode dwm ON f.mode_id = dwm.mode_id")
        wheres.append(f"AND dwm.work_mode = '{_escape(work_mode)}'")
    if location:
        joins.append("JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id")
        wheres.append(f"AND dl.city_name = '{_escape(location)}'")
    if level:
        if level in _LEVEL_ENTRY_SYNS:
            wheres.append(
                "AND (LOWER(f.job_title) LIKE '%fresher%' "
                "OR LOWER(f.job_title) LIKE '%intern%' "
                "OR LOWER(f.job_title) LIKE '%junior%' "
                "OR LOWER(f.job_title) LIKE '%entry%')"
            )
        else:
            wheres.append(f"AND LOWER(f.job_title) LIKE '%{_escape(level)}%'")
    return "\n".join(joins), "\n".join(wheres)


# ── SQL templates ────────────────────────────────────────────────────────────

_SQL_TOTAL_JOBS = """
SELECT COUNT(DISTINCT f.job_link) AS total
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
{extra_joins}
WHERE LOWER(djc.category_name) = LOWER('{category}')
{extra_where}
"""

_SQL_LEARNING_PATH = """
WITH cat_jobs AS (
    SELECT DISTINCT f.job_link
    FROM iceberg.gold.fact_job_posting f
    JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
    {extra_joins}
    WHERE LOWER(djc.category_name) = LOWER('{category}')
    {extra_where}
),
total_cat AS (SELECT COUNT(*) AS n FROM cat_jobs),
bridge_jobs AS (
    SELECT DISTINCT f.job_link
    FROM iceberg.gold.fact_job_posting f
    JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
    WHERE f.job_link IN (SELECT job_link FROM cat_jobs)
    {known_skills_filter}
),
total_bridge AS (SELECT COUNT(*) AS n FROM bridge_jobs)
SELECT
    ds.skill_name,
    ds.skill_group,
    COUNT(DISTINCT f.job_link) AS market_count,
    COUNT(DISTINCT CASE WHEN f.job_link IN (SELECT job_link FROM bridge_jobs)
                        THEN f.job_link END) AS bridge_count,
    ROUND(COUNT(DISTINCT f.job_link) * 100.0 / NULLIF((SELECT n FROM total_cat), 0), 1)
        AS market_freq,
    ROUND(
        CASE WHEN (SELECT n FROM total_bridge) > 0
             THEN COUNT(DISTINCT CASE WHEN f.job_link IN (SELECT job_link FROM bridge_jobs)
                                      THEN f.job_link END) * 100.0
                  / (SELECT n FROM total_bridge)
             ELSE 0.0 END,
    1) AS bridge_score,
    (SELECT n FROM total_bridge) AS bridge_pool_size,
    (SELECT n FROM total_cat) AS total_pool_size
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
WHERE f.job_link IN (SELECT job_link FROM cat_jobs)
{exclude_known_filter}
AND LOWER(ds.skill_name) NOT IN (
    'data engineer','software engineer','backend developer','frontend developer',
    'fullstack developer','mobile developer','devops engineer','qa engineer',
    'data analyst','data scientist','machine learning engineer','ai engineer',
    'cloud engineer','embedded engineer','game developer','security engineer',
    'product manager','business analyst','database administrator',
    'system administrator','network engineer',
    'tester','qa/qc','manual test',
    'ai','cloud','database','it','oop','agile','scrum','software','technology',
    'english','japanese','korean','chinese',
    'team management','project management','leadership'
)
GROUP BY ds.skill_name, ds.skill_group
HAVING COUNT(DISTINCT f.job_link) >= {min_count}
ORDER BY COUNT(DISTINCT f.job_link) DESC
LIMIT 10
"""

_SQL_INFER_CATEGORY = """
SELECT
    djc.category_name,
    COUNT(DISTINCT f.job_link) AS cnt
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE {role_conditions}
GROUP BY djc.category_name
ORDER BY cnt DESC
LIMIT 1
"""


def _escape(s: str) -> str:
    return s.replace("'", "''")


_ROLE_CATEGORY_MAP: dict[str, str] = {
    "backend": "Backend Development",
    "back-end": "Backend Development",
    "back end": "Backend Development",
    "frontend": "Frontend Development",
    "front-end": "Frontend Development",
    "front end": "Frontend Development",
    "fullstack": "Fullstack Development",
    "full-stack": "Fullstack Development",
    "full stack": "Fullstack Development",
    "mobile": "Mobile Development",
    "android": "Mobile Development",
    "ios": "Mobile Development",
    "devops": "DevOps & Infrastructure",
    "sre": "DevOps & Infrastructure",
    "infrastructure": "DevOps & Infrastructure",
    "data engineer": "Data Engineering",
    "data pipeline": "Data Engineering",
    "data analyst": "Data Analytics",
    "analytics": "Data Analytics",
    "ai": "AI & Machine Learning",
    "machine learning": "AI & Machine Learning",
    "ml engineer": "AI & Machine Learning",
    "data science": "AI & Machine Learning",
    "qa": "Testing & QA",
    "tester": "Testing & QA",
    "testing": "Testing & QA",
    "security": "Cyber Security",
    "cybersecurity": "Cyber Security",
    "embedded": "Embedded & IoT",
    "iot": "Embedded & IoT",
    "game": "Game Development",
    "erp": "ERP & CRM",
    "crm": "ERP & CRM",
    "product manager": "Product & Business Analysis",
    "business analyst": "Product & Business Analysis",
    "manager": "Management",
    "management": "Management",
}


def _infer_category_from_keywords(role: str) -> str | None:
    role_lower = role.lower()
    for keyword in sorted(_ROLE_CATEGORY_MAP, key=len, reverse=True):
        if keyword in role_lower:
            return _ROLE_CATEGORY_MAP[keyword]
    return None


def _run(conn: trino.dbapi.Connection, sql: str) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


_MIN_FILTERED_POOL = 10


class LearningPathService:
    """
    @class LearningPathService
    @brief Recommend the next skills to learn based on target role and existing skill set.

    Supports optional level/work_mode/location filters extracted from the
    target_role string. Falls back to broader category-only when filtered
    pool is too small (< 10 jobs).
    """

    def analyze(
        self,
        target_role:  str,
        known_skills: list[str],
    ) -> LearningPathResult | None:
        if not target_role.strip():
            return None

        # Extract optional filters from target_role text
        level = _extract_level(target_role)
        work_mode = _extract_work_mode(target_role)
        location = _extract_location(target_role)

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=20,
            )

            # Q0 — infer category
            category = _infer_category_from_keywords(target_role)
            if category:
                logger.info("LearningPath | role='%s' → category='%s' (keyword match)", target_role, category)
            else:
                role_condition = f"LOWER(f.job_title) LIKE '%{_escape(target_role.strip().lower())}%'"
                rows = _run(conn, _SQL_INFER_CATEGORY.format(role_conditions=role_condition))
                if not rows:
                    logger.warning("LearningPath: no category found for role '%s'", target_role)
                    conn.close()
                    return None
                category = rows[0]["category_name"]
                logger.info("LearningPath | role='%s' → category='%s' (LIKE fallback)", target_role, category)

            # Build filter clauses
            extra_joins, extra_where = _build_extra_joins_where(level, work_mode, location)
            has_filters = bool(level or work_mode or location)
            filter_desc = ", ".join(
                f for f in [
                    f"level={level}" if level else "",
                    f"mode={work_mode}" if work_mode else "",
                    f"city={location}" if location else "",
                ] if f
            )

            # Q1 — total jobs (filtered)
            total_rows = _run(conn, _SQL_TOTAL_JOBS.format(
                category=_escape(category), extra_joins=extra_joins, extra_where=extra_where,
            ))
            total_jobs = int(total_rows[0]["total"]) if total_rows else 0

            # Fallback: if filtered pool too small, drop filters
            used_fallback = False
            if total_jobs < _MIN_FILTERED_POOL and has_filters:
                logger.info(
                    "LearningPath: filtered pool too small (%d < %d) for [%s]; "
                    "falling back to category-only.",
                    total_jobs, _MIN_FILTERED_POOL, filter_desc,
                )
                extra_joins, extra_where = "", ""
                total_rows = _run(conn, _SQL_TOTAL_JOBS.format(
                    category=_escape(category), extra_joins="", extra_where="",
                ))
                total_jobs = int(total_rows[0]["total"]) if total_rows else 0
                used_fallback = True

            if total_jobs == 0:
                conn.close()
                return None

            logger.info(
                "LearningPath | category='%s' | filters=[%s] | pool=%d%s",
                category, filter_desc if not used_fallback else "fallback:none",
                total_jobs, " (fallback)" if used_fallback else "",
            )

            # Build known-skills filters (normalize aliases: "nodejs"→"node.js", "golang"→"go", etc.)
            clean_known = list(dict.fromkeys(
                _normalize_skill(s) for s in known_skills if s.strip()
            ))
            if clean_known:
                in_list = ", ".join(f"'{_escape(s)}'" for s in clean_known)
                known_skills_filter = f"AND LOWER(ds.skill_name) IN ({in_list})"
                exclude_known_filter = f"AND LOWER(ds.skill_name) NOT IN ({in_list})"
            else:
                known_skills_filter = "AND 1=0"
                exclude_known_filter = ""

            # Adaptive min_count: lower threshold for small pools
            min_count = 2 if total_jobs < 50 else 5

            # Q2 — bridge + market scores
            skill_rows = _run(conn, _SQL_LEARNING_PATH.format(
                category=_escape(category),
                extra_joins=extra_joins,
                extra_where=extra_where,
                known_skills_filter=known_skills_filter,
                exclude_known_filter=exclude_known_filter,
                min_count=min_count,
            ))
            conn.close()

            # Detect career change: if bridge pool < 5% of total → user's skills
            # are foreign to the target domain (e.g. Testing→Cyber Security).
            # Bridge scores are noisy on tiny pools → use market_freq only.
            bridge_pool = int(skill_rows[0].get("bridge_pool_size") or 0) if skill_rows else 0
            is_career_change = bridge_pool < total_jobs * 0.05
            if is_career_change and clean_known:
                logger.info(
                    "LearningPath: career change detected (bridge=%d / total=%d = %.1f%%) "
                    "→ ranking by market_freq only.",
                    bridge_pool, total_jobs, 100 * bridge_pool / total_jobs if total_jobs else 0,
                )

            w_market = 1.0
            w_bridge = 0.0

            steps: list[LearningPathStep] = []
            for row in skill_rows:
                mf = float(row.get("market_freq") or 0)
                bs = float(row.get("bridge_score") or 0)
                steps.append(LearningPathStep(
                    skill_name=row["skill_name"],
                    skill_group=row.get("skill_group") or "Other",
                    market_freq=mf,
                    bridge_score=bs if not is_career_change else 0.0,
                    rank_score=round(w_market * mf + w_bridge * bs, 1),
                    market_count=int(row.get("market_count") or 0),
                ))

            # Re-sort by adjusted rank_score (SQL sorted by fixed 0.6/0.4 formula)
            steps.sort(key=lambda s: s.rank_score, reverse=True)
            steps = steps[:10]

            logger.info(
                "LearningPath ready | category=%s | filters=[%s] | known=%d | steps=%d | pool=%d | career_change=%s",
                category, filter_desc if not used_fallback else "none(fallback)",
                len(clean_known), len(steps), total_jobs, is_career_change,
            )
            return LearningPathResult(
                target_role=target_role,
                role_category=category,
                total_jobs=total_jobs,
                known_skills=[s.strip() for s in known_skills if s.strip()],
                steps=steps,
            )

        except Exception as exc:
            logger.warning("LearningPath Trino error: %s", exc)
            return None
