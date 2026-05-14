"""
@file sql_agent.py
@brief Analytics SQL agent: translates natural-language questions to Trino SQL and executes them.

The LLM generates a single valid Trino SQL query against the Gold/Silver star
schema using a detailed schema context prompt.  The result rows are then
optionally converted to a Chart.js-compatible spec for frontend visualisation.
"""

import logging
import textwrap
from datetime import date, datetime
from typing import Any

import trino
from openai import OpenAI

from config import settings

logger = logging.getLogger(__name__)

# Schema context injected into every SQL-generation prompt.
# Reflects the exact Gold + Silver tables produced by Build_Gold.py.
_SCHEMA_CONTEXT = textwrap.dedent("""
    You have access to an IT job market database (ITviec data) via Trino SQL.

    Catalog: iceberg

    === GOLD LAYER (aggregated, star schema) ===

    iceberg.gold.fact_job_posting
        fact_id      BIGINT  (PK)
        job_link     VARCHAR
        job_title    VARCHAR
        company_id   BIGINT  (FK → dim_company)
        location_id  BIGINT  (FK → dim_location)
        skill_id     BIGINT  (FK → dim_skill)
        mode_id      BIGINT  (FK → dim_work_mode)
        category_id  BIGINT  (FK → dim_job_category)
        date_id      INT     (FK → dim_date, format YYYYMMDD)
        one_posting  INT     (always 1 — use SUM for counts)

    iceberg.gold.dim_skill
        skill_id    BIGINT, skill_name VARCHAR
        skill_group VARCHAR  ('Backend'|'Frontend'|'Data & Cloud'|'Other')

    iceberg.gold.dim_location
        location_id BIGINT, city_name VARCHAR
        region      VARCHAR  ('North'|'Central'|'South'|'Other')

    iceberg.gold.dim_company
        company_id BIGINT, company_name VARCHAR

    iceberg.gold.dim_work_mode
        mode_id BIGINT, work_mode VARCHAR  ('office'|'remote'|'hybrid')

    iceberg.gold.dim_date
        date_id INT, full_date DATE
        day INT, month INT, year INT, quarter INT, day_of_week VARCHAR

    iceberg.gold.dim_job_category
        category_id BIGINT
        category_name VARCHAR
            ('Data & AI'|'Testing & QA'|'DevOps & Infra'|'Frontend & Mobile'
            |'Backend'|'Software Engineering'|'Management'|'Product & BA'|'Other')

    === SILVER LAYER (row-level job data) ===

    iceberg.silver.it_jobs_clean
        job_title VARCHAR, company_name VARCHAR, job_link VARCHAR
        work_mode VARCHAR, salary VARCHAR
        skills_required ARRAY<VARCHAR>
        location        ARRAY<VARCHAR>
        date_only DATE, ingest_time TIMESTAMP

    === SQL RULES ===
    - Always prefix tables with catalog.schema (e.g. iceberg.gold.fact_job_posting)
    - Use LIMIT ≤ 20 to keep results manageable
    - For ARRAY columns use CROSS JOIN UNNEST(col) AS t(val)
    - JOIN fact_job_posting to dimensions via their respective *_id foreign keys
    - Avoid SELECT *; pick only columns needed to answer the question
    - Order results meaningfully (e.g. COUNT DESC)
""").strip()

_SYSTEM_PROMPT = f"""You are a Trino SQL expert. Generate a single, valid Trino SQL query.

{_SCHEMA_CONTEXT}

Return ONLY the raw SQL — no explanation, no markdown fences, no comments."""

# Màu mặc định cho chart
_CHART_COLORS = [
    "rgba(99, 102, 241, 0.8)",
    "rgba(14, 165, 233, 0.8)",
    "rgba(16, 185, 129, 0.8)",
    "rgba(245, 158, 11, 0.8)",
    "rgba(239, 68, 68, 0.8)",
    "rgba(168, 85, 247, 0.8)",
    "rgba(236, 72, 153, 0.8)",
    "rgba(20, 184, 166, 0.8)",
]


def _is_numeric(value: Any) -> bool:
    """
    @brief Return True if value is a numeric type (int or float) but not a bool.

    @param value  Any Python value from a SQL result row.
    @return       True for int/float, False for bool, str, None, etc.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_date_like(value: Any) -> bool:
    """
    @brief Return True if value is a date or datetime instance.

    @param value  Any Python value from a SQL result row.
    @return       True for date/datetime objects, False otherwise.
    """
    return isinstance(value, (date, datetime))


def _build_chart_spec(rows: list[dict]) -> dict | None:
    """
    @brief Analyse SQL result rows and produce a Chart.js-compatible spec if chartable.

    Chart type selection heuristic:
      - date column + numeric column  → line chart (time-series trend).
      - string column + numeric column, ≤ 8 rows → pie chart (distribution).
      - string column + numeric column, > 8 rows → bar chart (ranking).
      - string column + multiple numeric columns  → multi-series bar chart.

    @param rows  SQL result rows; each dict maps column name → Python value.
    @return      Chart.js spec dict, or None if the data is not suitable for charting.
    """
    if not rows or len(rows) < 2:
        return None

    columns = list(rows[0].keys())
    if len(columns) < 2:
        return None

    # Phân loại từng cột
    str_cols = [c for c in columns if isinstance(rows[0][c], str)]
    num_cols = [c for c in columns if _is_numeric(rows[0][c])]
    date_cols = [c for c in columns if _is_date_like(rows[0][c])]

    # --- Line chart: date + 1 số ---
    if date_cols and num_cols:
        label_col = date_cols[0]
        value_col = num_cols[0]
        labels = [str(r[label_col]) for r in rows]
        data = [r[value_col] for r in rows]
        return {
            "type": "line",
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": value_col.replace("_", " ").title(),
                    "data": data,
                    "borderColor": _CHART_COLORS[0],
                    "backgroundColor": _CHART_COLORS[0].replace("0.8", "0.15"),
                    "tension": 0.3,
                    "fill": True,
                }],
            },
        }

    # --- Bar / Pie: 1 string + 1 số ---
    if str_cols and num_cols:
        label_col = str_cols[0]
        value_col = num_cols[0]
        labels = [str(r[label_col]) for r in rows]
        data = [r[value_col] for r in rows]

        # Pie nếu ≤ 8 categories
        if len(rows) <= 8:
            return {
                "type": "pie",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": value_col.replace("_", " ").title(),
                        "data": data,
                        "backgroundColor": _CHART_COLORS[: len(labels)],
                    }],
                },
            }

        # Bar nếu nhiều hơn
        colors = (_CHART_COLORS * ((len(labels) // len(_CHART_COLORS)) + 1))[: len(labels)]
        return {
            "type": "bar",
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": value_col.replace("_", " ").title(),
                    "data": data,
                    "backgroundColor": colors,
                }],
            },
        }

    # --- Multi-series bar: 1 string + nhiều số ---
    if str_cols and len(num_cols) >= 2:
        label_col = str_cols[0]
        labels = [str(r[label_col]) for r in rows]
        datasets = [
            {
                "label": col.replace("_", " ").title(),
                "data": [r[col] for r in rows],
                "backgroundColor": _CHART_COLORS[i % len(_CHART_COLORS)],
            }
            for i, col in enumerate(num_cols)
        ]
        return {
            "type": "bar",
            "data": {"labels": labels, "datasets": datasets},
        }

    return None


class SQLAgentService:
    """
    @class SQLAgentService
    @brief Translate natural-language analytics questions into Trino SQL and execute them.

    Uses the LLM (with a detailed schema context prompt) to generate a single
    valid Trino SQL query, executes it against the Gold/Silver Iceberg catalog,
    and optionally produces a Chart.js spec for frontend visualisation.
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)


    # Private helpers

    def _get_connection(self) -> trino.dbapi.Connection:
        """
        @brief Create and return a new Trino DBAPI connection using app settings.

        @return  Open trino.dbapi.Connection ready for cursor operations.
        """
        return trino.dbapi.connect(
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog=settings.trino_catalog,
        )

    def _generate_sql(self, question: str) -> str:
        """
        @brief Use the LLM to generate a valid Trino SQL query for the given question.

        Strips accidental markdown fences and trailing semicolons from the
        LLM output before returning the clean SQL string.

        @param question  Natural-language analytics question from the user.
        @return          Clean, executable Trino SQL string.
        """
        response = self._client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=512,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
        )
        sql = response.choices[0].message.content.strip()

        # Strip accidental markdown code fences
        if sql.startswith("```"):
            lines = sql.splitlines()
            sql = "\n".join(lines[1:-1]).strip()

        # Trino rejects trailing semicolons
        sql = sql.rstrip(";").strip()

        return sql

    def _execute(self, sql: str) -> list[dict]:
        """
        @brief Execute a SQL string on Trino and return all result rows as dicts.

        @param sql  Valid Trino SQL string (no trailing semicolons).
        @return     List of dicts mapping column names to row values.
        @throws Exception  Propagated from the Trino driver on query failure.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
    # Public API

    def query(self, question: str) -> tuple[str, list[dict], dict | None]:
        """
        @brief Translate a question to SQL, execute it, and return results with an optional chart.

        @param question  Natural-language analytics question from the user.
        @return          3-tuple of (sql_string, result_rows, chart_spec).
                         chart_spec is a Chart.js-compatible dict or None if not chartable.
        @throws Exception  Propagated from Trino on query execution failure.
        """
        sql = self._generate_sql(question)
        logger.info("Generated SQL:\n%s", sql)

        rows = self._execute(sql)
        logger.info("SQL returned %d row(s).", len(rows))

        chart = _build_chart_spec(rows)
        logger.info("Chart type: %s", chart["type"] if chart else "none")

        return sql, rows, chart
