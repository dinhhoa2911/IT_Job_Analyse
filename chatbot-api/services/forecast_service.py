"""
@file forecast_service.py
@brief Prophet Forecast Service — query pre-computed forecast results from Gold layer.

The Prophet models (Prophet.py / Prophetweek.py) are run as a Spark job that reads
the Gold star schema, trains one Prophet model per job category, and writes results
to two Iceberg tables:
  - iceberg.gold.ml_forecast_jobs         (monthly granularity, 3 months ahead)
  - iceberg.gold.ml_model_evaluation      (MAPE accuracy per category)

This service queries those tables via Trino and builds a Chart.js spec with:
  - Solid line   : historical actual job counts (from fact_job_posting)
  - Dashed line  : Prophet forecast (future months)
  - Shaded band  : 95% confidence interval (yhat_lower → yhat_upper)

Why does this need the Lakehouse?
  Both the training data (Gold star schema) and the forecast output (ml_forecast_jobs)
  are stored in Iceberg tables accessed via Trino.  This combines ML model results with
  the data pipeline in a single queryable layer — impossible without the lakehouse.

Failure contract:
  All Trino queries are wrapped in try/except.  Returns (None, [], None) silently if
  Trino is unavailable or ml_forecast_jobs does not exist.
"""

import logging
from typing import Optional

import trino

from config import settings
from models.schemas import ForecastInsight

logger = logging.getLogger(__name__)

# ── Category inference from free-text query ───────────────────────────────────
# Maps natural-language query fragments → Gold dim_job_category.category_name.
# More specific entries first (Data before Software Engineering).

_QUERY_CATEGORY_PATTERNS: list[tuple[str, list[str]]] = [
    ("Data & AI",           ["data engineer", "data scientist", "data analyst", "machine learning",
                              "ml engineer", "ai engineer", "analytics", "big data",
                              "data science", "nlp", "llm", " data "]),
    ("Testing & QA",        ["tester", "qa ", "qc ", "quality assurance", "automation test",
                              "kiểm thử"]),
    ("DevOps & Infra",      ["devops", "sre", "platform engineer", "infrastructure",
                              "cloud engineer", "system engineer", "kubernetes", "k8s",
                              "hạ tầng"]),
    ("Frontend & Mobile",   ["frontend", "front-end", "mobile", "android", "ios",
                              "react native", "flutter", "ui developer", "react developer",
                              "vue developer", "angular"]),
    ("Backend",             ["backend", "back-end", "java ", "python ", ".net", "php",
                              "golang", "go developer", "nodejs", "node.js", "spring",
                              "ruby", "lập trình viên"]),
    ("Software Engineering", ["fullstack", "full stack", "software engineer",
                               "developer", "lập trình"]),
    ("Management",          ["engineering manager", "tech lead", "team lead", "head of",
                              "director", "cto"]),
    ("Product & BA",        ["product manager", "product owner", "business analyst",
                              "scrum master", "ba "]),
]

_FALLBACK_CATEGORY = "Software Engineering"

# Category names that can appear literally in a query
_CATEGORY_LITERAL = {
    "backend":              "Backend",
    "frontend":             "Frontend & Mobile",
    "mobile":               "Frontend & Mobile",
    "data":                 "Data & AI",
    "devops":               "DevOps & Infra",
    "testing":              "Testing & QA",
    "qa":                   "Testing & QA",
    "management":           "Management",
    "software engineering": "Software Engineering",
}


def _infer_category(query: str) -> str:
    """
    @brief Infer the Gold job category from a natural-language forecast query.

    Checks literal category names first, then pattern matching.

    @param query  Raw user query string.
    @return       Gold dim_job_category.category_name string.
    """
    q = query.lower()

    # Literal match (highest priority)
    for literal, cat in _CATEGORY_LITERAL.items():
        if literal in q:
            return cat

    # Pattern match
    for category, keywords in _QUERY_CATEGORY_PATTERNS:
        if any(kw in q for kw in keywords):
            return category

    return _FALLBACK_CATEGORY


# ── SQL templates ─────────────────────────────────────────────────────────────

_SQL_ACTUALS = """
SELECT
    CONCAT(CAST(d.year AS VARCHAR), '-',
           LPAD(CAST(d.month AS VARCHAR), 2, '0')) AS period,
    COUNT(DISTINCT f.job_link) AS actual_jobs
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_date         d   ON f.date_id    = d.date_id
JOIN iceberg.gold.dim_job_category djc ON f.category_id = djc.category_id
WHERE LOWER(djc.category_name) = LOWER('{category}')
GROUP BY d.year, d.month
ORDER BY d.year, d.month
LIMIT 24
"""

_SQL_FORECAST = """
SELECT
    date_format(ds, '%Y-%m') AS period,
    CAST(GREATEST(ROUND(yhat), 0)       AS INTEGER) AS predicted_jobs,
    CAST(GREATEST(ROUND(yhat_lower), 0) AS INTEGER) AS lower_bound,
    CAST(GREATEST(ROUND(yhat_upper), 0) AS INTEGER) AS upper_bound
FROM iceberg.gold.ml_forecast_jobs
WHERE LOWER(job_category) = LOWER('{category}')
ORDER BY ds
LIMIT 30
"""

_SQL_MAPE = """
SELECT
    ROUND(CAST("MAPE(%)" AS DOUBLE), 2) AS mape,
    "DataPoints" AS data_points
FROM iceberg.gold.ml_model_evaluation
WHERE LOWER(job_category) = LOWER('{category}')
LIMIT 1
"""


def _escape(s: str) -> str:
    return s.replace("'", "''")


def _run(conn: trino.dbapi.Connection, sql: str) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── Chart builder ─────────────────────────────────────────────────────────────

def _build_chart(
    category: str,
    actual_rows: list[dict],
    forecast_rows: list[dict],
) -> dict:
    """
    @brief Build a Chart.js line-chart spec combining actual and forecast data.

    Outputs four datasets:
      0 — "Thực tế"           : solid blue line, historical only
      1 — "Dự báo (Prophet)"  : dashed indigo line, bridges last actual → future
      2 — "_upper"            : upper confidence band (hidden from legend)
      3 — "_lower"            : lower confidence band (hidden from legend)

    The confidence band uses Chart.js fill '+1' (dataset 2 fills towards dataset 3).
    Datasets labelled with '_' prefix are filtered out in ChartRenderer's legend.

    @param category       Gold category name for the chart title.
    @param actual_rows    Monthly actuals from fact_job_posting.
    @param forecast_rows  Monthly forecast from ml_forecast_jobs.
    @return               Chart.js configuration dict.
    """
    actual_map: dict[str, int] = {r["period"]: int(r["actual_jobs"]) for r in actual_rows}
    forecast_map: dict[str, dict] = {r["period"]: r for r in forecast_rows}

    all_periods = sorted(set(list(actual_map.keys()) + list(forecast_map.keys())))
    last_actual = max(actual_map.keys()) if actual_map else None

    actual_data: list = []
    forecast_data: list = []
    upper_data: list = []
    lower_data: list = []

    for period in all_periods:
        if last_actual and period <= last_actual:
            actual_data.append(actual_map.get(period))
            forecast_data.append(None)
            upper_data.append(None)
            lower_data.append(None)
        else:
            actual_data.append(None)
            fr = forecast_map.get(period, {})
            forecast_data.append(fr.get("predicted_jobs"))
            upper_data.append(fr.get("upper_bound"))
            lower_data.append(fr.get("lower_bound"))

    # Connect forecast line visually to the last actual data point
    if last_actual and last_actual in actual_map:
        bridge_idx = all_periods.index(last_actual)
        forecast_data[bridge_idx] = actual_map[last_actual]

    future_count = sum(1 for p in all_periods if last_actual and p > last_actual)

    # Historical-only chart (no future predictions available)
    if future_count == 0:
        return {
            "type": "line",
            "data": {
                "labels": all_periods,
                "datasets": [
                    {
                        "label": f"Xu hướng thực tế ({category})",
                        "data": actual_data,
                        "borderColor": "#3b82f6",
                        "backgroundColor": "rgba(59,130,246,0.1)",
                        "borderWidth": 2.5,
                        "pointRadius": 3,
                        "pointBackgroundColor": "#3b82f6",
                        "tension": 0.3,
                        "fill": True,
                        "spanGaps": False,
                    }
                ],
            },
            "options": {
                "scales": {
                    "x": {"title": {"display": True, "text": "Tháng"}},
                    "y": {
                        "title": {"display": True, "text": "Số lượng job postings"},
                        "beginAtZero": True,
                    },
                },
            },
        }

    return {
        "type": "line",
        "data": {
            "labels": all_periods,
            "datasets": [
                {
                    "label": f"Thực tế ({category})",
                    "data": actual_data,
                    "borderColor": "#3b82f6",
                    "backgroundColor": "rgba(59,130,246,0.06)",
                    "borderWidth": 2.5,
                    "pointRadius": 4,
                    "pointBackgroundColor": "#3b82f6",
                    "tension": 0.3,
                    "fill": False,
                    "spanGaps": False,
                },
                {
                    "label": f"Dự báo Prophet ({future_count} tháng tới)",
                    "data": forecast_data,
                    "borderColor": "#6366f1",
                    "backgroundColor": "rgba(99,102,241,0.06)",
                    "borderWidth": 2.5,
                    "borderDash": [7, 4],
                    "pointRadius": 5,
                    "pointStyle": "triangle",
                    "pointBackgroundColor": "#6366f1",
                    "tension": 0.3,
                    "fill": False,
                    "spanGaps": True,
                },
                {
                    # Hidden from legend via '_' prefix (filtered in ChartRenderer)
                    "label": "_upper_95",
                    "data": upper_data,
                    "borderColor": "transparent",
                    "backgroundColor": "rgba(99,102,241,0.13)",
                    "borderWidth": 0,
                    "pointRadius": 0,
                    "fill": "+1",   # fills towards dataset index+1 (lower bound)
                    "spanGaps": False,
                },
                {
                    # Hidden from legend via '_' prefix
                    "label": "_lower_95",
                    "data": lower_data,
                    "borderColor": "transparent",
                    "backgroundColor": "transparent",
                    "borderWidth": 0,
                    "pointRadius": 0,
                    "fill": False,
                    "spanGaps": False,
                },
            ],
        },
        "options": {
            "scales": {
                "x": {"title": {"display": True, "text": "Tháng"}},
                "y": {
                    "title": {"display": True, "text": "Số lượng job postings"},
                    "beginAtZero": True,
                },
            },
        },
    }


def _build_data_rows(actual_rows: list[dict], forecast_rows: list[dict]) -> list[dict]:
    """
    @brief Merge actual and forecast data into a flat list for the LLM prompt.

    @param actual_rows    Monthly actuals.
    @param forecast_rows  Monthly forecast.
    @return               Merged rows with period, actual_jobs (or None), predicted_jobs,
                          and lower/upper bounds for future periods.
    """
    actual_map = {r["period"]: r["actual_jobs"] for r in actual_rows}
    all_periods = sorted(set([r["period"] for r in actual_rows + forecast_rows]))
    last_actual = max(actual_map.keys()) if actual_map else None

    rows = []
    for p in all_periods:
        fr = next((r for r in forecast_rows if r["period"] == p), {})
        is_future = last_actual and p > last_actual
        rows.append({
            "period":        p,
            "actual_jobs":   actual_map.get(p, "-") if not is_future else "N/A (forecast)",
            "predicted":     fr.get("predicted_jobs", "-"),
            "lower_95":      fr.get("lower_bound", "-") if is_future else "-",
            "upper_95":      fr.get("upper_bound", "-") if is_future else "-",
            "type":          "FORECAST" if is_future else "historical",
        })
    return rows


# ── Main service ──────────────────────────────────────────────────────────────

class ForecastService:
    """
    @class ForecastService
    @brief Query pre-computed Prophet forecast results from the Gold Iceberg layer via Trino.

    Stateless — no models loaded at construction time.  Instantiated once in RAGPipeline.
    Falls back gracefully when ml_forecast_jobs does not exist (Prophet not yet run).
    """

    def get_forecast(
        self,
        query: str,
    ) -> tuple[Optional[dict], list[dict], Optional[ForecastInsight]]:
        """
        @brief Fetch historical actuals + Prophet forecast for the inferred job category.

        @param query  Raw user query (used to infer the target job category).
        @return       Tuple of:
                        chart       — Chart.js spec dict, or None on failure.
                        data_rows   — Merged historical + forecast rows for the LLM prompt.
                        insight     — ForecastInsight metadata, or None on failure.
        """
        category = _infer_category(query)
        logger.info("Forecast | inferred_category=%s | query='%s...'", category, query[:60])

        try:
            conn = trino.dbapi.connect(
                host=settings.trino_host,
                port=settings.trino_port,
                user=settings.trino_user,
                catalog=settings.trino_catalog,
                request_timeout=10,
            )

            # Q1 — historical actuals
            actual_rows = _run(conn, _SQL_ACTUALS.format(category=_escape(category)))

            # Q2 — forecast (may fail if Prophet not yet run → fallback to empty)
            forecast_rows: list[dict] = []
            try:
                forecast_rows = _run(conn, _SQL_FORECAST.format(category=_escape(category)))
            except Exception as fe:
                logger.warning("ml_forecast_jobs unavailable for '%s': %s", category, fe)
                # Try fallback category
                if category != _FALLBACK_CATEGORY:
                    category = _FALLBACK_CATEGORY
                    actual_rows = _run(conn, _SQL_ACTUALS.format(category=_escape(category)))
                    try:
                        forecast_rows = _run(conn, _SQL_FORECAST.format(category=_escape(category)))
                    except Exception:
                        forecast_rows = []

            # Q3 — MAPE (optional — fail silently)
            mape: Optional[float] = None
            try:
                eval_rows = _run(conn, _SQL_MAPE.format(category=_escape(category)))
                if eval_rows and eval_rows[0].get("mape") is not None:
                    mape = float(eval_rows[0]["mape"])
            except Exception:
                pass

            conn.close()

            if not actual_rows and not forecast_rows:
                logger.info("Forecast: no data found for category '%s'.", category)
                return None, [], None

            # Count future periods
            last_actual = max(r["period"] for r in actual_rows) if actual_rows else None
            periods_ahead = sum(
                1 for r in forecast_rows
                if last_actual and r["period"] > last_actual
            )

            chart     = _build_chart(category, actual_rows, forecast_rows)
            data_rows = _build_data_rows(actual_rows, forecast_rows)

            if periods_ahead == 0:
                # All predicted dates are in the past — model is stale.
                # Return nothing so the UI doesn't show a broken/empty forecast.
                # Re-run Prophet.py to generate fresh 3-month predictions.
                logger.info(
                    "Forecast stale | category=%s | all forecast dates in past "
                    "→ returning None. Re-run Prophet.py to refresh.",
                    category,
                )
                return None, [], None

            insight = ForecastInsight(
                category      = category,
                mape          = mape,
                periods_ahead = periods_ahead,
                model         = "Prophet",
            )
            logger.info(
                "Forecast ready | category=%s | actual=%d | forecast=%d | mape=%s",
                category, len(actual_rows), periods_ahead,
                f"{mape:.1f}%" if mape else "N/A",
            )
            return chart, data_rows, insight

        except Exception as exc:
            logger.warning("Forecast unavailable (Trino error): %s", exc)
            return None, [], None
