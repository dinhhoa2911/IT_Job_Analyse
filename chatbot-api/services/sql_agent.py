"""
@file sql_agent.py
@brief Analytics SQL agent: translates natural-language questions to Trino SQL and executes them.

The LLM generates a single valid Trino SQL query against the Gold/Silver star
schema using a detailed schema context prompt.  The result rows are then
optionally converted to a Chart.js-compatible spec for frontend visualisation.
"""

import logging
import re
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
        date_posted DATE, ingest_time TIMESTAMP

    === SQL RULES ===
    - Always prefix tables with catalog.schema (e.g. iceberg.gold.fact_job_posting)
    - Use LIMIT ≤ 50 to keep results manageable (daily charts need up to 31 rows)
    - BANNED FUNCTIONS (Trino does not support these — never use them):
        array_contains()  → use CROSS JOIN UNNEST or JOIN dim_skill instead
        collect_list()    → use ARRAY_AGG()
        size()            → use CARDINALITY()
    - For ARRAY columns use CROSS JOIN UNNEST(col) AS t(val)
    - PREFER Gold layer JOINs over Silver ARRAY operations for skill-based queries:
        WRONG: WHERE array_contains(s.skills_required, 'Python')
        RIGHT: JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
               WHERE LOWER(ds.skill_name) = 'python'
    - JOIN fact_job_posting to dimensions via their respective *_id foreign keys
    - Avoid SELECT *; pick only columns needed to answer the question
    - Order results meaningfully (e.g. COUNT DESC)

    === CHART / VISUALIZATION RULES (apply when user asks for a chart or graph) ===
    - Always put the LABEL/CATEGORY column FIRST, the numeric METRIC column SECOND
    - Cast time-period grouping columns to VARCHAR so they become readable string labels:
        Month:   CAST(d.month AS VARCHAR) AS thang
        Quarter: CONCAT('Q', CAST(d.quarter AS VARCHAR)) AS quy
        Year:    CAST(d.year AS VARCHAR) AS nam
        Month+Year: CONCAT(CAST(d.year AS VARCHAR), '-', LPAD(CAST(d.month AS VARCHAR), 2, '0')) AS month_label
    - Choose time granularity based on the scope of the query:
        • Query scoped to ONE specific month  → GROUP BY day, only return days that have data:
            SELECT LPAD(CAST(d.day AS VARCHAR), 2, '0') AS ngay,
                   SUM(f.one_posting) AS job_count
            FROM iceberg.gold.fact_job_posting f
            JOIN iceberg.gold.dim_date d ON f.date_id = d.date_id
            WHERE d.month = <month> AND d.year = <year>
            GROUP BY 1 ORDER BY 1
        • Query scoped to ONE specific year   → GROUP BY month (label: LPAD(CAST(d.month AS VARCHAR),2,'0') AS thang)
        • Query spanning multiple years       → GROUP BY year  (label: CAST(d.year AS VARCHAR) AS nam)
        • Query spanning multiple months/quarters → GROUP BY month or quarter
      This ensures the chart always has multiple data points to render.
    - For time-trend charts ORDER BY the time column ASC (chronological order)
    - For rankings (top skills, top companies) ORDER BY metric DESC
    - Keep SELECT to exactly 2 columns for single-metric charts (label + value)
    - CRITICAL: Trino does NOT support GROUP BY <alias>. Always use ordinal position:
        CORRECT:  GROUP BY 1
        WRONG:    GROUP BY month_label   ← alias not allowed in GROUP BY
      ORDER BY may use either alias or ordinal (both work in Trino)
""").strip()

_SYSTEM_PROMPT = f"""You are a Trino SQL expert. Generate a single, valid Trino SQL query.

{_SCHEMA_CONTEXT}

Return ONLY the raw SQL — no explanation, no markdown fences, no comments."""

# ── Color palette ─────────────────────────────────────────────────────────────
# Single bar/line colors — clean, professional
_LINE_COLOR   = "rgba(14, 165, 233, 1)"      # sky-500
_LINE_FILL    = "rgba(14, 165, 233, 0.08)"   # sky very transparent

# Multi-series palette (elegant, not rainbow)
_SERIES_COLORS = [
    "rgba(99, 102, 241, 0.85)",   # indigo
    "rgba(14, 165, 233, 0.85)",   # sky
    "rgba(16, 185, 129, 0.85)",   # emerald
    "rgba(245, 158, 11, 0.85)",   # amber
    "rgba(239, 68, 68, 0.85)",    # red
    "rgba(168, 85, 247, 0.85)",   # violet
]

# Pie chart — curated elegant palette
_PIE_COLORS = [
    "rgba(99, 102, 241, 0.88)",
    "rgba(14, 165, 233, 0.88)",
    "rgba(16, 185, 129, 0.88)",
    "rgba(245, 158, 11, 0.88)",
    "rgba(239, 68, 68, 0.88)",
    "rgba(168, 85, 247, 0.88)",
    "rgba(20, 184, 166, 0.88)",
    "rgba(236, 72, 153, 0.88)",
]

# ── Label normalization ────────────────────────────────────────────────────────
_LABEL_PATTERNS = [
    (re.compile(r'job|posting|so_luong|viec|count|total', re.IGNORECASE), 'Job Count'),
    (re.compile(r'salary|luong|wage|avg_sal|compensation', re.IGNORECASE), 'Avg Salary'),
    (re.compile(r'company|cong_ty|employer|firm', re.IGNORECASE), 'Companies'),
    (re.compile(r'skill|ky_nang', re.IGNORECASE), 'Skills'),
]

def _normalize_label(col: str) -> str:
    """Map SQL column aliases to clean English labels."""
    for pattern, label in _LABEL_PATTERNS:
        if pattern.search(col):
            return label
    return col.replace("_", " ").title()

# ── Nhận dạng date string dạng "YYYY-MM-DD" hoặc "YYYY-MM" ───────────────────
_DATE_STRING_RE = re.compile(r'^\d{4}-\d{2}(-\d{2})?$')


def _is_numeric(value: Any) -> bool:
    """Return True for int/float but not bool or None."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_date_like(value: Any) -> bool:
    """
    Return True for date/datetime objects AND date-like strings (YYYY-MM-DD / YYYY-MM).
    Trino thường trả về date dạng Python date object hoặc string — xử lý cả hai.
    """
    if isinstance(value, (date, datetime)):
        return True
    if isinstance(value, str) and _DATE_STRING_RE.match(value.strip()):
        return True
    return False


def _parse_date_val(value: Any) -> date:
    """Parse date/datetime/string → date để sort chronologically."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value).strip()[:10], "%Y-%m-%d").date()
    except Exception:
        return date.min


def _first_non_none(rows: list[dict], col: str) -> Any:
    """Trả về giá trị đầu tiên không phải None trong cột, tránh lỗi khi row[0] là None."""
    for r in rows:
        v = r.get(col)
        if v is not None:
            return v
    return None


def _safe_num(value: Any) -> float | int:
    """Trả về giá trị numeric, hoặc 0 nếu None/invalid."""
    return value if _is_numeric(value) else 0


# ── Time-label detection ──────────────────────────────────────────────────────
_TIME_DAY_RE     = re.compile(r'^(0?[1-9]|[12]\d|3[01])$')
_TIME_YEAR_RE    = re.compile(r'^20\d{2}$')
_TIME_QUARTER_RE = re.compile(r'^Q[1-4]$')
_TIME_DATE_RE    = re.compile(r'^\d{4}-\d{2}')


def _is_time_label(labels: list[str]) -> bool:
    """Label đại diện thời gian (ngày/tháng/quý/năm) thay vì category."""
    if not labels:
        return False
    return all(
        _TIME_DAY_RE.match(str(l)) or _TIME_YEAR_RE.match(str(l)) or
        _TIME_QUARTER_RE.match(str(l)) or _TIME_DATE_RE.match(str(l).strip())
        for l in labels
    )


def _is_dense_time(labels: list[str]) -> bool:
    """Time data đủ dày để vẽ line chart (≥60% ngày có data, ≥6 tháng, hoặc năm/quý)."""
    if not labels:
        return False
    if all(_TIME_YEAR_RE.match(str(l)) or _TIME_QUARTER_RE.match(str(l)) for l in labels):
        return True
    if all(_TIME_DATE_RE.match(str(l).strip()) for l in labels):
        return len(labels) >= 6
    try:
        nums    = [int(l) for l in labels]
        max_val = max(nums)
        if max_val <= 12:
            return len(labels) >= 6
        if max_val <= 31:
            return len(labels) / max_val >= 0.6
    except (ValueError, TypeError):
        pass
    return False


# ── Scale options ─────────────────────────────────────────────────────────────
_SCALE_XY = {
    "x": {"grid": {"display": False}, "border": {"display": False}},
    "y": {"grid": {"color": "rgba(148,163,184,0.12)"}, "border": {"display": False}, "beginAtZero": True},
}
_SCALE_HORIZONTAL = {
    "x": {"grid": {"color": "rgba(148,163,184,0.12)"}, "border": {"display": False}, "beginAtZero": True},
    "y": {"grid": {"display": False}, "border": {"display": False}},
}


# ── Chart spec builders ───────────────────────────────────────────────────────

def _bar_spec(labels, data, label_text, horizontal=False):
    colors = (_PIE_COLORS * ((len(labels) // len(_PIE_COLORS)) + 1))[:len(labels)]
    opts   = {"scales": _SCALE_HORIZONTAL if horizontal else _SCALE_XY}
    if horizontal:
        opts["indexAxis"] = "y"
    return {
        "type": "bar",
        "data": {"labels": labels, "datasets": [{
            "label": label_text, "data": data,
            "backgroundColor": colors, "borderWidth": 0,
            "borderRadius": 5, "borderSkipped": False,
        }]},
        "options": opts,
    }


def _line_spec(labels, data, label_text, fill=False):
    return {
        "type": "line",
        "data": {"labels": labels, "datasets": [{
            "label": label_text, "data": data,
            "borderColor": _LINE_COLOR,
            "backgroundColor": _LINE_FILL if fill else "transparent",
            "fill": fill, "tension": 0.4,
            "pointRadius": 4, "pointHoverRadius": 7,
            "pointStyle": "circle",
            "pointBackgroundColor": _LINE_COLOR, "borderWidth": 2,
        }]},
        "options": {"scales": _SCALE_XY},
    }


def _pie_spec(ctype, labels, data, label_text):
    return {
        "type": ctype,
        "data": {"labels": labels, "datasets": [{
            "label": label_text, "data": data,
            "backgroundColor": _PIE_COLORS[:len(labels)], "borderWidth": 0,
        }]},
    }


def _make_chart(ctype: str, labels, data, label_text) -> dict:
    if ctype == "line":             return _line_spec(labels, data, label_text, fill=False)
    if ctype == "area":             return _line_spec(labels, data, label_text, fill=True)
    if ctype in ("pie","doughnut"): return _pie_spec(ctype, labels, data, label_text)
    if ctype == "horizontalBar":    return _bar_spec(labels, data, label_text, horizontal=True)
    return _bar_spec(labels, data, label_text)


def _build_chart_spec(rows: list[dict], preferred: str | None = None) -> dict | None:
    """
    Tạo Chart.js spec từ SQL rows với smart chart type selection.

    preferred: 'bar'|'line'|'area'|'pie'|'doughnut'|'horizontalBar' — override của user.
    Nếu None thì auto-detect:
      - Date col + numeric         → line
      - Time labels (ngày/tháng)   → line nếu dense (≥60%), bar nếu sparse
      - Category ≤ 6               → doughnut
      - Category 7-20              → bar
      - Category > 20              → horizontalBar
      - Multi-series               → grouped bar
    """
    if not rows or len(rows) < 2:
        return None
    columns = list(rows[0].keys())
    if len(columns) < 2:
        return None

    date_cols = [c for c in columns if _is_date_like(_first_non_none(rows, c))]
    num_cols  = [c for c in columns if _is_numeric(_first_non_none(rows, c))]
    str_cols  = [
        c for c in columns
        if isinstance(_first_non_none(rows, c), str)
        and not _is_date_like(_first_non_none(rows, c))
    ]

    # ── 1. Date object + numeric → time series ────────────────────────────────
    if date_cols and num_cols:
        label_col   = date_cols[0]
        value_col   = num_cols[0]
        sorted_rows = sorted(rows, key=lambda r: _parse_date_val(r[label_col]))
        labels = [str(r[label_col]).strip()[:10] for r in sorted_rows]
        data   = [_safe_num(r[value_col]) for r in sorted_rows]
        ctype  = preferred if preferred in ("bar","line","area") else "line"
        return _make_chart(ctype, labels, data, _normalize_label(value_col))

    # ── 2. Multi-series: string + 2+ numeric ─────────────────────────────────
    if str_cols and len(num_cols) >= 2:
        label_col = str_cols[0]
        labels    = [str(r[label_col]) for r in rows]
        datasets  = [
            {
                "label": _normalize_label(col),
                "data": [_safe_num(r[col]) for r in rows],
                "backgroundColor": _SERIES_COLORS[i % len(_SERIES_COLORS)],
                "borderRadius": 5, "borderSkipped": False,
            }
            for i, col in enumerate(num_cols)
        ]
        ctype = preferred if preferred in ("bar","line","horizontalBar") else "bar"
        opts  = {"scales": _SCALE_HORIZONTAL if ctype == "horizontalBar" else _SCALE_XY}
        if ctype == "horizontalBar":
            opts["indexAxis"] = "y"
            ctype = "bar"
        return {"type": ctype, "data": {"labels": labels, "datasets": datasets}, "options": opts}

    # ── 3. Single series: string + 1 numeric ─────────────────────────────────
    if str_cols and num_cols:
        label_col = str_cols[0]
        value_col = num_cols[0]
        labels    = [str(r[label_col]) for r in rows]
        data      = [_safe_num(r[value_col]) for r in rows]

        # Sort numeric string labels ("01","02","11") đúng thứ tự số
        try:
            pairs  = sorted(zip(labels, data), key=lambda x: float(x[0]))
            labels = [p[0] for p in pairs]
            data   = [p[1] for p in pairs]
        except (ValueError, TypeError):
            pass

        is_time = _is_time_label(labels)
        n       = len(labels)

        if preferred:
            ctype = preferred
        elif is_time:
            ctype = "line" if _is_dense_time(labels) else "bar"
        else:
            if n <= 6:    ctype = "doughnut"
            elif n <= 20: ctype = "bar"
            else:         ctype = "horizontalBar"

        return _make_chart(ctype, labels, data, _normalize_label(value_col))

    # ── Fallback: 2 numeric cols, first là label-like int ────────────────────
    if len(num_cols) == 2 and not str_cols and not date_cols:
        label_col  = num_cols[0]
        value_col  = num_cols[1]
        first_vals = [r[label_col] for r in rows if _is_numeric(r.get(label_col))]
        if first_vals and 1 <= min(first_vals) <= max(first_vals) <= 31:
            labels = [f"{int(r[label_col]):02d}" for r in rows]
            data   = [_safe_num(r[value_col]) for r in rows]
            ctype  = preferred if preferred in ("bar","line","area") else "bar"
            return _make_chart(ctype, labels, data, _normalize_label(value_col))

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

    def query(self, question: str, preferred_chart: str | None = None) -> tuple[str, list[dict], dict | None]:
        """
        @brief Translate a question to SQL, execute it, and return results with an optional chart.

        @param question        Natural-language analytics question.
        @param preferred_chart Chart type requested by user ('bar','line','area','pie','doughnut','horizontalBar').
        @return                3-tuple of (sql_string, result_rows, chart_spec).
        """
        sql = self._generate_sql(question)
        logger.info("Generated SQL:\n%s", sql)

        rows = self._execute(sql)
        logger.info("SQL returned %d row(s).", len(rows))

        chart = _build_chart_spec(rows, preferred=preferred_chart)
        logger.info("Chart type: %s", chart["type"] if chart else "none")

        return sql, rows, chart
