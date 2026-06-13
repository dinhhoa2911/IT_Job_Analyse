"""
@file learning_path.py
@brief Data-driven skill learning path service — Gold layer via Trino.

Given a target role and the user's existing skills, this service queries the
Gold star schema to recommend the TOP-10 skills to learn next, ranked by:

  rank_score = 0.6 × market_freq + 0.4 × bridge_score

  market_freq   = % of target-category jobs requiring this skill
                  (pure market demand signal)

  bridge_score  = % of target-category jobs that require BOTH one of the
                  user's known skills AND this skill
                  (measures how well the skill connects to what user knows)

When known_skills is empty, bridge_score = 0 and ranking falls back to
market_freq only (still data-driven, just without personalisation).

Queries executed (against iceberg.gold.*):
  Q0 — infer category from target_role via job title matching
  Q1 — total distinct jobs in that category (denominator)
  Q2 — bridge + market SQL (CTE-based, single round-trip)

Failure contract:
  All Trino queries are wrapped in try/except.  Returns None silently if
  Trino is unavailable so the pipeline is never blocked.
"""

import logging
import re

import trino

from config import settings
from models.schemas import LearningPathResult, LearningPathStep

logger = logging.getLogger(__name__)


# ── SQL templates ─────────────────────────────────────────────────────────────

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

_SQL_TOTAL_JOBS = """
SELECT COUNT(DISTINCT f.job_link) AS total
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE LOWER(djc.category_name) = LOWER('{category}')
"""

# Bridge + market score in one CTE round-trip.
# known_skills_filter: AND LOWER(ds.skill_name) IN (...) — empty string when no known skills
_SQL_LEARNING_PATH = """
WITH cat_jobs AS (
    SELECT DISTINCT f.job_link
    FROM iceberg.gold.fact_job_posting f
    JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
    WHERE LOWER(djc.category_name) = LOWER('{category}')
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
    1) AS bridge_score
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
WHERE f.job_link IN (SELECT job_link FROM cat_jobs)
{exclude_known_filter}
AND LOWER(ds.skill_name) NOT IN (
    'data engineer','software engineer','backend developer','frontend developer',
    'fullstack developer','mobile developer','devops engineer','qa engineer',
    'data analyst','data scientist','machine learning engineer','ai engineer',
    'cloud engineer','embedded engineer','game developer','security engineer',
    'product manager','business analyst',
    'ai','cloud','database','it','oop','agile','scrum',
    'english','japanese','korean','team management','project management'
)
GROUP BY ds.skill_name, ds.skill_group
HAVING COUNT(DISTINCT f.job_link) >= 5
ORDER BY (
    0.6 * COUNT(DISTINCT f.job_link) * 1.0 / NULLIF((SELECT n FROM total_cat), 0)
    + 0.4 * CASE WHEN (SELECT n FROM total_bridge) > 0
                 THEN COUNT(DISTINCT CASE WHEN f.job_link IN (SELECT job_link FROM bridge_jobs)
                                          THEN f.job_link END) * 1.0
                      / (SELECT n FROM total_bridge)
                 ELSE 0.0 END
) DESC
LIMIT 10
"""


def _escape(s: str) -> str:
    return s.replace("'", "''")


def _run(conn: trino.dbapi.Connection, sql: str) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


class LearningPathService:
    """
    @class LearningPathService
    @brief Recommend the next skills to learn based on target role and existing skill set.

    Stateless — instantiated once at application startup from RAGPipeline.
    """

    def analyze(
        self,
        target_role:  str,
        known_skills: list[str],
    ) -> LearningPathResult | None:
        """
        @brief Run the full learning path analysis for a target role.

        @param target_role   Job role string (e.g. "Data Engineer", "Backend Developer").
        @param known_skills  Skills the user already has (may be empty).
        @return              LearningPathResult with ordered steps, or None on Trino error.
        """
        if not target_role.strip():
            return None

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=20,
            )

            # Q0 — infer category from target role title
            role_condition = f"LOWER(f.job_title) LIKE '%{_escape(target_role.strip().lower())}%'"
            rows = _run(conn, _SQL_INFER_CATEGORY.format(role_conditions=role_condition))
            if not rows:
                logger.warning("LearningPath: no category found for role '%s'", target_role)
                conn.close()
                return None
            category = rows[0]["category_name"]
            logger.info("LearningPath | role='%s' → category='%s'", target_role, category)

            # Q1 — total jobs in category
            total_rows = _run(conn, _SQL_TOTAL_JOBS.format(category=_escape(category)))
            total_jobs = int(total_rows[0]["total"]) if total_rows else 0
            if total_jobs == 0:
                conn.close()
                return None

            # Build known-skills filters
            clean_known = [s.strip().lower() for s in known_skills if s.strip()]
            if clean_known:
                in_list = ", ".join(f"'{_escape(s)}'" for s in clean_known)
                known_skills_filter  = f"AND LOWER(ds.skill_name) IN ({in_list})"
                exclude_known_filter = f"AND LOWER(ds.skill_name) NOT IN ({in_list})"
            else:
                known_skills_filter  = "AND 1=0"   # empty bridge set
                exclude_known_filter = ""

            # Q2 — bridge + market scores
            skill_rows = _run(conn, _SQL_LEARNING_PATH.format(
                category             = _escape(category),
                known_skills_filter  = known_skills_filter,
                exclude_known_filter = exclude_known_filter,
            ))
            conn.close()

            steps: list[LearningPathStep] = []
            for row in skill_rows:
                mf  = float(row.get("market_freq")  or 0)
                bs  = float(row.get("bridge_score") or 0)
                steps.append(LearningPathStep(
                    skill_name   = row["skill_name"],
                    skill_group  = row.get("skill_group") or "Other",
                    market_freq  = mf,
                    bridge_score = bs,
                    rank_score   = round(0.6 * mf + 0.4 * bs, 1),
                    market_count = int(row.get("market_count") or 0),
                ))

            logger.info(
                "LearningPath ready | category=%s | known=%d | steps=%d",
                category, len(clean_known), len(steps),
            )
            return LearningPathResult(
                target_role   = target_role,
                role_category = category,
                total_jobs    = total_jobs,
                known_skills  = [s.strip() for s in known_skills if s.strip()],
                steps         = steps,
            )

        except Exception as exc:
            logger.warning("LearningPath Trino error: %s", exc)
            return None
