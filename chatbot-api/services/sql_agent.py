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
from decimal import Decimal
from typing import Any

import trino
from openai import OpenAI

from config import settings

logger = logging.getLogger(__name__)

# Schema context injected into every SQL-generation prompt.
# Reflects the exact Gold + Silver tables produced by Build_Gold.py.
_SCHEMA_CONTEXT = textwrap.dedent("""
    You have access to an IT job market database (ITviec data) via Trino SQL.
    Data source: ITviec Vietnam — 9,627 job postings crawled 2025-09-23 → 2026-04-11.

    ⚠ NO SALARY DATA — neither Silver nor Gold contains salary information.
      If asked about salary, return 0 rows or state this limitation in your query comment.
      NEVER fabricate salary numbers.

    Catalog: iceberg

    === GOLD LAYER (star schema — use for all analytics) ===

    iceberg.gold.fact_job_posting   ← 8,114 distinct jobs (fact rows are more due to explosion)
        fact_id      BIGINT  (PK)
        job_link     VARCHAR  ← unique job URL — use COUNT(DISTINCT job_link) for job counts
        job_title    VARCHAR  ← raw job title from posting
        company_id   BIGINT  (FK → dim_company)
        location_id  BIGINT  (FK → dim_location)
        skill_id     BIGINT  (FK → dim_skill)
        mode_id      BIGINT  (FK → dim_work_mode)
        category_id  BIGINT  (FK → dim_job_category)
        date_id      INT     (FK → dim_date)
        one_posting  INT     ← always 1, DO NOT USE for counting

    ⚠ CRITICAL — fact_job_posting is EXPLODED (one job → many rows, one per skill × location).
      ALWAYS:  COUNT(DISTINCT f.job_link)   ← actual job count
      NEVER:   COUNT(*) or COUNT(f.fact_id)  ← overcounts massively

    iceberg.gold.dim_skill          ← 504 skills
        skill_id    BIGINT
        skill_name  VARCHAR  ← stored in UPPERCASE: 'PYTHON', 'JAVA', 'REACT', 'SQL', etc.
                               Always compare with LOWER(): WHERE LOWER(ds.skill_name) = 'python'
        skill_group VARCHAR  ← WARNING: 91% of skills are 'Other'. Groups:
                               'Backend'(7)|'Frontend'(9)|'Cloud & DevOps'(6)|'Database'(5)|
                               'AI & ML'(5)|'Mobile'(5)|'Data Engineering'(3)|
                               'Software Engineering'(2)|'Testing'(2)|'Other'(460)
                               Do NOT use skill_group for meaningful filtering — use skill_name directly.

    ⚠ SKILL NAME ALIASES — resolve user input to exact DB skill_name before writing SQL:
        'golang'               → WHERE LOWER(ds.skill_name) = 'go'           (453 jobs)
        'k8s'                  → WHERE LOWER(ds.skill_name) = 'kubernetes'   (193 jobs)
        'react native'         → WHERE LOWER(ds.skill_name) = 'react_native' (205 jobs, UNDERSCORE)
        'nodejs'/'node js'     → WHERE LOWER(ds.skill_name) = 'node.js'      (616 jobs)
        'vuejs'/'vue.js'       → WHERE LOWER(ds.skill_name) = 'vue'          (165 jobs; DB has VUE not VUE.JS)
        'ci/cd'/'ci cd'        → WHERE LOWER(ds.skill_name) = 'cicd'         (467 jobs, no slash)
        'scikit-learn'/'sklearn'→ WHERE LOWER(ds.skill_name) = 'scikitlearn' (9 jobs, no hyphen)
        'microservices'        → WHERE LOWER(ds.skill_name) = 'microservice' (380 jobs, singular)
        'elastic search'       → WHERE LOWER(ds.skill_name) = 'elasticsearch'(26 jobs)
        'elk'                  → WHERE LOWER(ds.skill_name) = 'elk stack'    (5 jobs; separate from elasticsearch)
        'postgres'             → WHERE LOWER(ds.skill_name) = 'postgresql'   (427 jobs)
        'dotnet'/'dot net'     → WHERE LOWER(ds.skill_name) = '.net'         (742 jobs)
        'airflow'              → WHERE LOWER(ds.skill_name) = 'apache airflow'(11 jobs)
        'apache kafka'         → WHERE LOWER(ds.skill_name) = 'kafka'        (118 jobs)
        'nest.js'/'nestjs'     → WHERE LOWER(ds.skill_name) = 'nestjs'       (136 jobs, no dot)
        WRONG: WHERE LOWER(ds.skill_name) = 'golang'     ← skill doesn't exist in DB
        WRONG: WHERE LOWER(ds.skill_name) = 'vue.js'     ← doesn't exist; DB has 'vue'
        WRONG: WHERE LOWER(ds.skill_name) = 'ci/cd'      ← slash version doesn't exist; DB has 'cicd'
        WRONG: WHERE LOWER(ds.skill_name) = 'microservices' ← plural doesn't exist; DB has 'microservice'

    iceberg.gold.dim_location       ← 10 cities  [alias: dl]
        location_id BIGINT
        city_name   VARCHAR  — EXACT values (case-sensitive, no abbreviations):
                               'Ho Chi Minh'(5264) | 'Ha Noi'(3075) | 'Da Nang'(536)
                               'Others'(82) | 'Binh Duong'(9) | 'International'(4)
                               'Hung Yen'(4) | 'Hai Phong'(4) | 'Long An'(2) | 'Hue'(1)
                    ← ALIASES to resolve: HCM/TP.HCM/Saigon → 'Ho Chi Minh'
                                          Hanoi/HN/Hà Nội   → 'Ha Noi'
                                          Danang/Đà Nẵng    → 'Da Nang'
        region      VARCHAR  — 'South' | 'North' | 'Central' | 'Other'
                    ← region is in dim_location (alias dl), NOT in dim_date (alias d)!
                               South=HCM+Binh Duong | North=Hanoi+Hung Yen | Central=DaNang+Hue
        USAGE: JOIN iceberg.gold.dim_location dl ON f.location_id = dl.location_id
               → dl.city_name, dl.region  (NOT d.region — d is for dim_date!)

    iceberg.gold.dim_company        ← 1,172 companies
        company_id   BIGINT
        company_name VARCHAR  ← top hirers: MB BANK(219), BOSCH(96), LG CNS(91), CROSSIAN(87)

    iceberg.gold.dim_work_mode      ← EXACT string values (case-sensitive):
        mode_id   BIGINT
        work_mode VARCHAR  — 'At Office'(8,004 jobs) | 'Remote'(100) | 'Hybrid'(10)
                             Market is heavily office-based: At Office = ~98% of postings

    iceberg.gold.dim_date           ← 8 distinct months covered
        date_id      INT
        full_date    TIMESTAMP
        day INT, month INT, year INT, quarter INT, day_of_week VARCHAR
        ← Range: 2025-09-23 to 2026-04-11 (months: Sep2025 → Apr2026)

    iceberg.gold.dim_job_category   ← 17 categories (use EXACT names below)
        category_id   BIGINT
        category_name VARCHAR  — exact values and job counts:
            'Backend Development'(1483) | 'Other'(1092) | 'Frontend Development'(1041)
            'Testing & QA'(744) | 'Product & Business Analysis'(572)
            'DevOps & Infrastructure'(476) | 'Mobile Development'(458)
            'AI & Machine Learning'(433) | 'Software Engineering'(410)
            'Management'(361) | 'Data Engineering'(278) | 'ERP & CRM'(198)
            'Cyber Security'(180) | 'Fullstack Development'(113)
            'Embedded & IoT'(101) | 'Game Development'(91) | 'Data Analytics'(83)
        ← NEVER use: 'Backend', 'Data & AI', 'DevOps & Infra', 'Frontend & Mobile' (old names)

    === SILVER LAYER (raw job rows — use only when Gold doesn't have what's needed) ===

    iceberg.silver.it_jobs_clean    ← 9,627 rows (one row per job posting)
        job_title        VARCHAR
        company_name     VARCHAR
        location         ARRAY<VARCHAR>   ← e.g. ['Ho Chi Minh', 'Remote']
        skills_required  ARRAY<VARCHAR>   ← skill names in UPPERCASE, e.g. ['PYTHON', 'SQL']
        job_category     VARCHAR          ← granular ITviec category (not same as Gold dim_job_category)
                                            e.g. 'Backend Developer', 'AI / Machine Learning Engineer'
        work_mode        VARCHAR          ← 'At Office' | 'Remote' | 'Hybrid'
        date_posted      TIMESTAMP
        date_only        DATE
        job_link         VARCHAR          ← unique job URL
        crawl_date       TIMESTAMP
        ingest_time      TIMESTAMP
        source           VARCHAR          ← always 'ITviec'
        skill_groups     ARRAY<VARCHAR>
        ← NO salary column — salary data does not exist in this database

    === TOP N PER GROUP (Window Function Pattern) ===
    "top 3 kỹ năng mỗi ngành" / "top N skills per category" / "với mỗi X, top N Y":
    ALWAYS write ONE single query using ROW_NUMBER() window function. NEVER make separate
    queries per category — that violates the single-call rule and gives wrong results.

    Correct pattern for "top 3 skills per job category":
      WITH skill_counts AS (
          SELECT djc.category_name,
                 ds.skill_name,
                 COUNT(DISTINCT f.job_link) AS job_count
          FROM iceberg.gold.fact_job_posting f
          JOIN iceberg.gold.dim_skill ds        ON f.skill_id    = ds.skill_id
          JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
          GROUP BY 1, 2
      ),
      ranked AS (
          SELECT category_name, skill_name, job_count,
                 ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY job_count DESC) AS rn
          FROM skill_counts
      )
      SELECT category_name, skill_name, job_count
      FROM ranked
      WHERE rn <= 3
      ORDER BY category_name, rn

    ✗ WRONG: separate WHERE category='X' queries for each category
    ✓ RIGHT: one query with ROW_NUMBER() OVER (PARTITION BY ...) covering all categories

    === CO-OCCURRENCE / SKILL CORRELATION QUERIES ===
    "Kỹ năng nào hay xuất hiện cùng X?" / "skills that appear with X" / "co-occur with X":
    Always write ONE single query returning BOTH co_count AND pct in the same SELECT.
    The correct pattern (replace <skill> with the target skill name in lowercase):

      SELECT ds.skill_name,
             COUNT(DISTINCT f.job_link)                                              AS co_count,
             ROUND(COUNT(DISTINCT f.job_link) * 100.0 / (
                 SELECT COUNT(DISTINCT f2.job_link)
                 FROM iceberg.gold.fact_job_posting f2
                 JOIN iceberg.gold.dim_skill ds2 ON f2.skill_id = ds2.skill_id
                 WHERE LOWER(ds2.skill_name) = '<skill>'
             ), 1)                                                                   AS pct_of_<skill>_jobs
      FROM iceberg.gold.fact_job_posting f
      JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id
      WHERE f.job_link IN (
          SELECT DISTINCT f2.job_link
          FROM iceberg.gold.fact_job_posting f2
          JOIN iceberg.gold.dim_skill ds2 ON f2.skill_id = ds2.skill_id
          WHERE LOWER(ds2.skill_name) = '<skill>'
      )
      AND LOWER(ds.skill_name) != '<skill>'
      GROUP BY 1
      ORDER BY co_count DESC
      LIMIT 10

    ✗ WRONG: WHERE LOWER(ds.skill_name) = 'python' → counts Python itself, always 1 row
    ✗ WRONG: two separate queries for count and percentage
    ✓ RIGHT: one query, two output columns (co_count + pct), excludes the target skill itself

    === WORK MODE vs LOCATION — CRITICAL DISAMBIGUATION ===
    ⚠ "tỷ lệ làm việc tại văn phòng / remote / hybrid" → ALWAYS use dim_work_mode, NEVER dim_location.
    Vietnamese work mode terms map to ENGLISH DB values (case-sensitive):
      văn phòng / tại văn phòng / onsite / tại chỗ / "at office" → 'At Office'
      remote / từ xa / làm từ xa / work from home                → 'Remote'
      hybrid / kết hợp / linh hoạt                              → 'Hybrid'
    CORRECT query for work mode distribution:
      SELECT dw.work_mode, COUNT(DISTINCT f.job_link) AS job_count
      FROM iceberg.gold.fact_job_posting f
      JOIN iceberg.gold.dim_work_mode dw ON f.mode_id = dw.mode_id
      GROUP BY 1
      ORDER BY 2 DESC
    WRONG: querying dl.region or dl.city_name for work mode questions.
    WRONG: using Vietnamese string values like 'Văn phòng' in WHERE clauses — always use English.

    === SQL RULES ===
    ⚠ ABSOLUTE RULE — NO HALLUCINATED LOCATION FILTERS:
      NEVER add WHERE conditions on city_name or region unless the user EXPLICITLY mentions
      a specific city or region in their question.
      "top 5 công ty Python"          → NO location filter (query ALL cities)
      "top 10 kỹ năng backend"        → NO location filter
      "top công ty tuyển nhiều nhất"  → NO location filter
      "top 5 công ty Python ở HCM"    → WHERE dl.city_name = 'Ho Chi Minh'
      "top skills tại Hà Nội"         → WHERE dl.city_name = 'Ha Noi'
      Do NOT join dim_location unless the user explicitly requests a city/region filter.

    ⚠ ABSOLUTE RULE — NO HALLUCINATED TIME FILTERS:
      NEVER add WHERE conditions on dim_date (year, month, quarter, day) unless the user
      EXPLICITLY mentions a specific time period.
      "top 10 kỹ năng backend" → NO date filter at all (query all data in the database)
      "top skills tháng 3 năm 2026" → WHERE d.month=3 AND d.year=2026
      "phân bổ work mode hiện tại"  → NO date filter (query all data as-is)
      "kỹ năng hot nhất hiện nay"   → NO date filter
      "top công ty HCM"             → NO date filter
      The database covers 2025-09-23 → 2026-04-11. NEVER use year=2023 or any year
      not explicitly stated by the user. Do not join dim_date unless a time filter is needed.
      "hiện tại" / "hiện nay" / "currently" / "now" = query ALL data, NO date filter.

    - When the user asks for BOTH count AND percentage in the same question, always SELECT both
      as separate columns so the frontend can render two charts:
        SELECT ds.skill_name,
               COUNT(DISTINCT f.job_link)                                           AS co_count,
               ROUND(COUNT(DISTINCT f.job_link) * 100.0 / <total_subquery>, 1)     AS pct
        ...
      Do NOT collapse them into one column or compute % only in application logic.

    - Always prefix tables: iceberg.gold.fact_job_posting, iceberg.silver.it_jobs_clean, etc.
    - LIMIT rules:
        • Rankings (company/skill/category): use EXACTLY the number the user requests.
          "top 5" → LIMIT 5 | "top 15" → LIMIT 15 | "top 20" → LIMIT 20 | "top 50" → LIMIT 50
          No number specified → LIMIT 10 by default.
          No hard cap — respect whatever the user asks for.
        • Daily charts → LIMIT 31 | Monthly charts → LIMIT 12 | Other time-series → LIMIT 50
    - BANNED FUNCTIONS: array_contains() → CROSS JOIN UNNEST | collect_list() → ARRAY_AGG() | size() → CARDINALITY()
    - For ARRAY columns: CROSS JOIN UNNEST(col) AS t(val)
    - Prefer Gold JOINs over Silver ARRAY for skill queries:
        RIGHT: JOIN iceberg.gold.dim_skill ds ON f.skill_id = ds.skill_id WHERE LOWER(ds.skill_name) = 'python'
        WRONG: WHERE array_contains(skills_required, 'Python')
    - COUNTING: COUNT(DISTINCT f.job_link) — never COUNT(*) or COUNT(f.fact_id)
    - GROUP BY: use ordinal position (GROUP BY 1, 2, 3), NOT alias
    - CRITICAL: Every column in ORDER BY that is NOT an aggregate MUST also be in GROUP BY.
      WRONG: GROUP BY d.month ORDER BY d.year, d.month  ← d.year missing from GROUP BY → ERROR
      RIGHT: GROUP BY d.year, d.month ORDER BY d.year, d.month
      SAFEST: always use ordinals → GROUP BY 1, 2 ORDER BY 1, 2
    - CASE sensitivity: city_name and work_mode are case-sensitive ('Ho Chi Minh' not 'ho chi minh')

    === PERIOD COMPARISON / QoQ / YoY QUERIES ===
    "ngành nào tăng trưởng từ Q4 sang Q1" / "so sánh hai kỳ theo category" pattern:
    Use conditional COUNT(DISTINCT CASE WHEN ...) to pivot multiple periods into columns.
    ALWAYS break down BY CATEGORY (or whatever dimension the user asks about) — never
    return a single row of totals when the user wants a ranking or breakdown.

    Example: "ngành IT nào tăng trưởng mạnh nhất từ Q4/2025 sang Q1/2026?"
      WITH quarterly AS (
          SELECT djc.category_name,
                 COUNT(DISTINCT CASE WHEN d.year=2025 AND d.quarter=4 THEN f.job_link END) AS q4_2025,
                 COUNT(DISTINCT CASE WHEN d.year=2026 AND d.quarter=1 THEN f.job_link END) AS q1_2026
          FROM iceberg.gold.fact_job_posting f
          JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
          JOIN iceberg.gold.dim_date d             ON f.date_id    = d.date_id
          GROUP BY 1
      )
      SELECT category_name, q4_2025, q1_2026,
             ROUND((CAST(q1_2026 AS DOUBLE) - q4_2025) * 100.0 / NULLIF(q4_2025, 0), 1) AS growth_pct
      FROM quarterly
      WHERE q4_2025 > 0
      ORDER BY growth_pct DESC

    Key rules:
    ✗ WRONG: GROUP BY quarter → returns 2 summary rows (Q4 total, Q1 total) — not a ranking
    ✓ RIGHT: GROUP BY category_name → 17 rows, one per category, with growth rate per category
    - growth_pct can be NEGATIVE (decline) — handle in response: if all values negative,
      note that the market declined and rank by "least decline" not "most growth"
    - Use CAST(... AS DOUBLE) before percentage math to avoid integer division

    === MONTH-OVER-MONTH (MoM) GROWTH QUERIES ===
    "tốc độ tăng trưởng theo tháng" / "MoM growth" / "so với tháng trước" — use LAG():

      WITH monthly AS (
          SELECT d.year, d.month,
                 CONCAT(CAST(d.year AS VARCHAR), '-', LPAD(CAST(d.month AS VARCHAR), 2, '0')) AS month_label,
                 COUNT(DISTINCT f.job_link) AS job_count
          FROM iceberg.gold.fact_job_posting f
          JOIN iceberg.gold.dim_date d ON f.date_id = d.date_id
          GROUP BY 1, 2, 3
      )
      SELECT month_label,
             job_count,
             ROUND(
                 (job_count - LAG(job_count) OVER (ORDER BY year, month)) * 100.0
                 / NULLIF(LAG(job_count) OVER (ORDER BY year, month), 0),
             1) AS mom_growth_pct
      FROM monthly
      ORDER BY year, month

    ✗ WRONG: GROUP BY d.month ORDER BY d.year, d.month  ← EXPRESSION_NOT_AGGREGATE error
    ✓ RIGHT: include ALL ORDER BY columns in GROUP BY, or use ordinal positions

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
                   COUNT(DISTINCT f.job_link) AS job_count
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

def _build_system_prompt() -> str:
    today = date.today().strftime("%Y-%m-%d")
    critical = (
        "⚠⚠⚠ CRITICAL RULES — violations produce wrong results:\n"
        "\n"
        "RULE 1 — NO HALLUCINATED DATE/TIME FILTERS:\n"
        "  NEVER join dim_date or add WHERE on year/month/quarter/day\n"
        "  unless the user EXPLICITLY states a time period (e.g. 'tháng 3', 'năm 2026', 'Q1 2026').\n"
        "  Data range is 2025-09-23 → 2026-04-11. NEVER use year=2023, 2024, or any\n"
        "  year/month not stated by the user.\n"
        "  ✗ WRONG: JOIN dim_date d ON ... WHERE d.year=2023  ← user never said 2023\n"
        "  ✗ WRONG: WHERE d.month=10 AND d.year=2023          ← hallucinated\n"
        "  ✓ RIGHT: no dim_date join at all when no time filter needed\n"
        "\n"
        "RULE 2 — NO HALLUCINATED LOCATION FILTERS:\n"
        "  NEVER join dim_location or add WHERE on city_name/region\n"
        "  unless the user EXPLICITLY mentions a city or region.\n"
        "  ✗ WRONG: WHERE dl.city_name = 'Ho Chi Minh'  ← user never said HCM\n"
        "  ✓ RIGHT: no dim_location join when no city filter needed\n"
        "\n"
        "RULE 3 — USER INTENT ONLY:\n"
        "  Query EXACTLY what the user asked. Do not add 'helpful' extra filters.\n"
        "  'top 5 công ty Python' → no city, no date filter, just skill + company.\n"
        "  'phân bổ ngành IT' → no date, no city, just category distribution.\n"
    )
    return (
        f"You are a Trino SQL expert. Generate a single, valid Trino SQL query.\n"
        f"Today's date: {today}. The database covers 2025-09-23 → 2026-04-11.\n\n"
        f"{critical}\n"
        f"{_SCHEMA_CONTEXT}\n\n"
        "Return ONLY the raw SQL — no explanation, no markdown fences, no comments."
    )

# ── Color palette ─────────────────────────────────────────────────────────────
# Single bar/line colors — clean, professional
_LINE_COLOR   = "rgba(14, 165, 233, 1)"      # sky-500
_LINE_FILL    = "rgba(14, 165, 233, 0.08)"   # sky very transparent

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
# Order matters — pct must come before job/count to avoid "pct_of_python_jobs" → "Job Count"
_LABEL_PATTERNS = [
    (re.compile(r'pct|percent|ratio|proportion|\brate\b|tỷ.lệ|phần.trăm', re.IGNORECASE), 'Tỷ lệ (%)'),
    (re.compile(r'job|posting|so_luong|viec|count|total', re.IGNORECASE), 'Số lượng job'),
    (re.compile(r'salary|luong|wage|avg_sal|compensation', re.IGNORECASE), 'Avg Salary'),
    (re.compile(r'company|cong_ty|employer|firm', re.IGNORECASE), 'Công ty'),
    (re.compile(r'skill|ky_nang', re.IGNORECASE), 'Kỹ năng'),
]

_PCT_COL_RE = re.compile(r'pct|percent|ratio|proportion|\brate\b', re.IGNORECASE)

def _normalize_label(col: str) -> str:
    """Map SQL column aliases to clean display labels."""
    for pattern, label in _LABEL_PATTERNS:
        if pattern.search(col):
            return label
    return col.replace("_", " ").title()

def _is_pct_col(col: str) -> bool:
    """True khi column name gợi ý dữ liệu phần trăm (0-100)."""
    return bool(_PCT_COL_RE.search(col))

# ── Nhận dạng date string dạng "YYYY-MM-DD" hoặc "YYYY-MM" ───────────────────
_DATE_STRING_RE = re.compile(r'^\d{4}-\d{2}(-\d{2})?$')


def _is_numeric(value: Any) -> bool:
    """Return True for int/float/Decimal but not bool or None."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


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
    """Trả về giá trị numeric, hoặc 0 nếu None/invalid. Decimal → float."""
    if isinstance(value, Decimal):
        return float(value)
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

def _title_plugin(text: str) -> dict:
    return {
        "display": True,
        "text": text,
        "font": {"size": 14, "weight": "600", "family": "'Inter', sans-serif"},
        "color": "#111827",
        "align": "center",
        "padding": {"top": 6, "bottom": 14},
    }


def _bar_spec(labels, data, label_text, horizontal=False, title: str | None = None):
    colors = (_PIE_COLORS * ((len(labels) // len(_PIE_COLORS)) + 1))[:len(labels)]
    opts   = {"scales": _SCALE_HORIZONTAL if horizontal else _SCALE_XY}
    if horizontal:
        opts["indexAxis"] = "y"
    if title:
        opts["plugins"] = {"title": _title_plugin(title)}
    return {
        "type": "bar",
        "data": {"labels": labels, "datasets": [{
            "label": label_text, "data": data,
            "backgroundColor": colors, "borderWidth": 0,
            "borderRadius": 5, "borderSkipped": False,
        }]},
        "options": opts,
    }


def _line_spec(labels, data, label_text, fill=False, title: str | None = None):
    opts: dict = {"scales": _SCALE_XY}
    if title:
        opts["plugins"] = {"title": _title_plugin(title)}
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
        "options": opts,
    }


def _pie_spec(ctype, labels, data, label_text, title: str | None = None):
    n = len(labels)
    colors = (_PIE_COLORS * ((n // len(_PIE_COLORS)) + 1))[:n]
    opts: dict = {}
    if title:
        opts["plugins"] = {"title": _title_plugin(title)}
    spec: dict = {
        "type": ctype,
        "data": {"labels": labels, "datasets": [{
            "label": label_text, "data": data,
            "backgroundColor": colors, "borderWidth": 0,
        }]},
    }
    if opts:
        spec["options"] = opts
    return spec


def _make_chart(ctype: str, labels, data, label_text, title: str | None = None) -> dict:
    if ctype == "line":             return _line_spec(labels, data, label_text, fill=False, title=title)
    if ctype == "area":             return _line_spec(labels, data, label_text, fill=True,  title=title)
    if ctype in ("pie","doughnut"): return _pie_spec(ctype, labels, data, label_text, title=title)
    if ctype == "horizontalBar":    return _bar_spec(labels, data, label_text, horizontal=True, title=title)
    return _bar_spec(labels, data, label_text, title=title)


_COL_DISPLAY: dict[str, str] = {
    "category_name": "Ngành", "category": "Ngành",
    "skill_name": "Kỹ năng", "skill": "Kỹ năng",
    "company_name": "Công ty", "company": "Công ty",
    "city_name": "Thành phố", "location": "Địa điểm",
    "work_mode": "Hình thức làm việc",
    "region": "Khu vực",
}

def _col_display(col: str) -> str:
    return _COL_DISPLAY.get(col.lower(), col.replace("_", " ").title())


def _grouped_bar_chart(
    rows: list[dict],
    group_col: str,
    item_col: str,
    value_col: str,
) -> dict:
    """
    Pivot (group, item, value) rows → grouped horizontal bar chart.
    Each rank (Top 1, Top 2, Top 3…) becomes a dataset.
    Stores item names in dataset.skillNames for tooltip display.
    """
    groups: list[str] = list(dict.fromkeys(str(r[group_col]) for r in rows))
    group_items: dict[str, list[tuple[str, float]]] = {g: [] for g in groups}
    for r in rows:
        group_items[str(r[group_col])].append(
            (str(r.get(item_col, "")), _safe_num(r.get(value_col)))
        )

    max_rank = max((len(v) for v in group_items.values()), default=0)

    datasets = []
    for rank in range(max_rank):
        vals: list[float] = []
        names: list[str] = []
        for g in groups:
            items = group_items[g]
            if rank < len(items):
                name, val = items[rank]
                vals.append(val)
                names.append(name)
            else:
                vals.append(0)
                names.append("")
        color = _PIE_COLORS[rank % len(_PIE_COLORS)]
        datasets.append({
            "label": f"Top {rank + 1}",
            "data": vals,
            "skillNames": names,        # picked up by ChartRenderer for tooltip
            "backgroundColor": color,
            "borderWidth": 0,
            "borderRadius": 3,
            "borderSkipped": False,
        })

    n = len(groups)
    max_len = max((len(g) for g in groups), default=0)
    horiz = n > 5 or max_len > 14

    item_d  = _col_display(item_col).lower()
    group_d = _col_display(group_col).lower()
    title_text = f"Top {max_rank} {item_d} — theo từng {group_d}"

    opts: dict = {"scales": _SCALE_HORIZONTAL if horiz else _SCALE_XY}
    if horiz:
        opts["indexAxis"] = "y"
    opts["plugins"] = {"title": _title_plugin(title_text)}

    return {
        "type": "bar",
        "data": {"labels": groups, "datasets": datasets},
        "options": opts,
    }



def _build_chart_spec(rows: list[dict], preferred: str | None = None) -> list[dict]:
    """
    Build Chart.js specs from SQL result rows. Returns [] / [1] / [N] charts.

    Shape detection:
      (date/time, value…)           → line chart(s), one per metric
      (group, item, value)          → grouped bar (pivot by group, one dataset per rank)
      (label, count, pct…)          → one chart per metric; pct → doughnut if ≤8 items
      (label, value)                → bar / doughnut / line depending on n and label type
    """
    if not rows or len(rows) < 2:
        return []
    columns = list(rows[0].keys())
    if len(columns) < 2:
        return []

    date_cols = [c for c in columns if _is_date_like(_first_non_none(rows, c))]
    num_cols  = [c for c in columns if _is_numeric(_first_non_none(rows, c))]
    str_cols  = [
        c for c in columns
        if isinstance(_first_non_none(rows, c), str)
        and not _is_date_like(_first_non_none(rows, c))
    ]

    # ── 1. Time-series: date/time labels + 1..N metrics ──────────────────────
    if date_cols and num_cols:
        label_col   = date_cols[0]
        sorted_rows = sorted(rows, key=lambda r: _parse_date_val(r[label_col]))
        labels = [str(r[label_col]).strip()[:10] for r in sorted_rows]
        ctype  = preferred if preferred in ("bar", "line", "area") else "line"

        charts = []
        for col in num_cols:
            data = [_safe_num(r[col]) for r in sorted_rows]
            lbl  = _normalize_label(col)
            charts.append(_make_chart(ctype, labels, data, lbl, title=lbl))
        return charts

    # ── 2. Grouped top-N: 2 str cols + 1 numeric with repeating first col ────
    # Shape: (group, item, value) e.g. (category, skill, job_count)
    if len(str_cols) >= 2 and len(num_cols) == 1:
        group_col = str_cols[0]
        item_col  = str_cols[1]
        value_col = num_cols[0]
        group_labels = [str(r[group_col]) for r in rows]
        if len(group_labels) != len(set(group_labels)):  # repeating → grouped data
            return [_grouped_bar_chart(rows, group_col, item_col, value_col)]

    # ── 3. Multi-metric: 1 str label + 2..N numeric → one chart per metric ───
    if str_cols and len(num_cols) >= 2:
        label_col = str_cols[0]
        labels    = [str(r[label_col]) for r in rows]
        n         = len(labels)
        max_len   = max(len(str(l)) for l in labels)
        horiz     = n > 8 or max_len > 20

        charts = []
        for col in num_cols:
            data   = [_safe_num(r[col]) for r in rows]
            lbl    = _normalize_label(col)
            is_pct = _is_pct_col(col)

            all_pos = all(v >= 0 for v in data)
            if preferred in ("pie", "doughnut") and all_pos:
                charts.append(_pie_spec(preferred, labels, data, lbl, title=lbl))
            elif is_pct and n <= 8 and all_pos:
                # % proportional data, small set, no negatives → doughnut
                charts.append(_pie_spec("doughnut", labels, data, lbl, title=lbl))
            else:
                # Negative values (e.g. growth_pct decline) or large n → bar
                charts.append(_bar_spec(labels, data, lbl, horizontal=horiz, title=lbl))
        return charts

    # ── 4. Single metric: 1 str label + 1 numeric ────────────────────────────
    if str_cols and num_cols:
        label_col = str_cols[0]
        value_col = num_cols[0]
        labels    = [str(r[label_col]) for r in rows]
        data      = [_safe_num(r[value_col]) for r in rows]
        lbl       = _normalize_label(value_col)

        # Still repeating labels but no secondary str col to pivot → skip chart
        if len(labels) != len(set(labels)):
            return []

        # Sort numeric-string labels ("01","02"…) chronologically
        try:
            pairs  = sorted(zip(labels, data), key=lambda x: float(x[0]))
            labels = [p[0] for p in pairs]
            data   = [p[1] for p in pairs]
        except (ValueError, TypeError):
            pass

        is_time = _is_time_label(labels)
        n       = len(labels)
        avg_len = sum(len(str(l)) for l in labels) / max(n, 1)

        if preferred:
            ctype = preferred
        elif is_time:
            ctype = "line" if _is_dense_time(labels) else "bar"
        else:
            if n <= 5 and avg_len <= 12:
                ctype = "doughnut"
            elif n > 5 or avg_len > 12:
                ctype = "horizontalBar"
            else:
                ctype = "bar"

        return [_make_chart(ctype, labels, data, lbl, title=lbl)]

    # ── 5. Fallback: 2 numeric cols, first is a day/month ordinal ────────────
    if len(num_cols) == 2 and not str_cols and not date_cols:
        label_col  = num_cols[0]
        value_col  = num_cols[1]
        first_vals = [r[label_col] for r in rows if _is_numeric(r.get(label_col))]
        if first_vals and 1 <= min(first_vals) <= max(first_vals) <= 31:
            labels = [f"{int(r[label_col]):02d}" for r in rows]
            data   = [_safe_num(r[value_col]) for r in rows]
            lbl    = _normalize_label(value_col)
            ctype  = preferred if preferred in ("bar", "line", "area") else "bar"
            return [_make_chart(ctype, labels, data, lbl, title=lbl)]

    return []


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
                {"role": "system", "content": _build_system_prompt()},
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

    def query(self, question: str, preferred_chart: str | None = None) -> tuple[str, list[dict], list[dict]]:
        """
        @brief Translate a question to SQL, execute it, and return results with chart specs.

        @param question        Natural-language analytics question.
        @param preferred_chart Chart type requested by user ('bar','line','area','pie','doughnut','horizontalBar').
        @return                3-tuple of (sql_string, result_rows, chart_specs).
                               chart_specs is a list: [] (none), [1 chart], or [2 charts].
        """
        sql = self._generate_sql(question)
        logger.info("Generated SQL:\n%s", sql)

        rows = self._execute(sql)
        logger.info("SQL returned %d row(s).", len(rows))

        charts = _build_chart_spec(rows, preferred=preferred_chart)
        logger.info("Charts: %d spec(s), types=%s", len(charts), [c["type"] for c in charts])

        return sql, rows, charts
