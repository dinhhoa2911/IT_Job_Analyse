"""
@file agent.py
@brief Agentic RAG — LLM self-selects tools and executes them in parallel.

Architecture (replaces the hard-coded classify → route pipeline):

  User query
      │
      ▼
  ┌─────────────────────────────────────────────────────┐
  │  PLAN (1 LLM call, max_tokens=256)                  │
  │  LLM sees tool list + query → returns tool_calls[]  │
  └─────────────────────────────────────────────────────┘
      │
      ▼  (tool_calls may be 1, 2, 3, or 0)
  ┌─────────────────────────────────────────────────────┐
  │  EXECUTE (ThreadPoolExecutor — tools run in parallel)│
  │  search_jobs → HybridSearch + MarketContext          │
  │  run_analytics → SQLAgent (Trino)                   │

  │  career_advice → no retrieval, LLM knowledge only   │
  └─────────────────────────────────────────────────────┘
      │
      ▼
  ┌─────────────────────────────────────────────────────┐
  │  SYNTHESIZE (1 LLM call, max_tokens=1200)           │
  │  Combines all tool results into one response        │
  └─────────────────────────────────────────────────────┘

Single-tool queries (most queries) = 2 LLM calls total.
Multi-tool queries                 = 2 LLM calls + parallel IO.
The plan call is cheap (tiny output); synthesis is the only long generation.
"""

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

from openai import OpenAI

from config import settings
from constants import LOCATION_ALIASES, WORK_MODE_KEYWORDS
from models.schemas import (
    ChatResponse, JobResult, MarketInsight, QueryType,
)
from services.market_context import MarketContextService, format_market_block
from services.query_processor import QueryProcessor
from services.sql_agent import SQLAgentService
from services.vector_search import HybridSearchService

logger = logging.getLogger(__name__)

# ── Tool specifications (OpenAI function-calling format) ──────────────────────

TOOL_SPECS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "search_jobs",
            "description": (
                "Search IT job listings in Vietnam by skill, role, seniority, location, work-mode. "
                "Use when user wants to FIND or SEE actual job postings. "
                "Handles: 'tìm job Python senior', 'React remote HCM', 'việc làm Data Engineer Hà Nội', "
                "'jobs at Shopee', 'backend fresher HCMC'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language search query (skill / role / location / seniority)",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_analytics",
            "description": (
                "Query the Gold data lakehouse (Trino/Iceberg star schema) for CURRENT statistics. "
                "Use for: salary ranges, skill rankings, company counts, work-mode ratios, "
                "top companies, job counts by location/category. "
                "Keywords that trigger this: 'lương', 'bao nhiêu', 'top', 'ranking', 'thống kê', "
                "'phân bố', 'tỷ lệ', 'số lượng', 'nhiều nhất', 'salary', 'how many', 'statistics'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Analytics question in natural language",
                    }
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "career_advice",
            "description": (
                "Provide IT career guidance, learning roadmaps, and role comparisons. "
                "Use when user asks: what to learn, career paths, role differences, salary expectations, "
                "skills to acquire. This tool does NOT query the database — pure LLM knowledge. "
                "Examples: 'nên học gì để làm backend', 'DevOps vs SRE', 'roadmap Data Science'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Career advice question",
                    }
                },
                "required": ["question"],
            },
        },
    },
]

# ── Planning system prompt ─────────────────────────────────────────────────────

_PLAN_SYSTEM = """\
You are a dispatcher for an IT job-market AI assistant serving Vietnam's tech industry.

Given a user query, call the appropriate tool(s).

RULES:
1. Call search_jobs   → user wants to FIND specific job listings OR asks about salary for a
   SPECIFIC ROLE+LOCATION (e.g. "Python senior HCM lương thế nào?" needs BOTH search_jobs
   AND run_analytics — the job listings show real salaries and context).
2. Call run_analytics → user wants aggregate STATISTICS, RANKINGS, or MARKET DATA.
3. Call career_advice  → user wants LEARNING ROADMAP or CAREER GUIDANCE (no DB needed).
4. Call MULTIPLE tools when a query spans multiple intents — never drop an intent.
   Examples of multi-tool queries:
   • "Python senior HCM lương thế nào và xu hướng?" → search_jobs + run_analytics
   • "tìm job React, thị trường React như thế nào?"  → search_jobs + run_analytics
   • "Data Engineer tương lai ra sao và nên học gì?" → run_analytics + career_advice
5. Call NO tool and reply directly for: greetings, off-topic, or completely unclear queries.

IMPORTANT: If a query contains a role/skill name AND a location (like "Python HCM", "Java Hà Nội"),
ALWAYS call search_jobs — even if the query also asks for salary or trend information.\
"""

# ── Synthesis system prompt ───────────────────────────────────────────────────

_SYNTH_SYSTEM = """\
You are an IT recruitment AI assistant for Vietnam's tech job market (ITviec data).
You have retrieved data from multiple sources. Synthesize it into ONE cohesive response.

LANGUAGE RULE: Respond in {lang}. No exceptions.

OUTPUT STRUCTURE (follow in order, only include sections that have data):

1. JOBS (if search_jobs was called):
   Lead with 1 sentence summary (count + stack). Only mention location/work-mode if the
   === SEARCH_JOBS header shows an active filter for it.
   Format each job as:
   ### [Job Title] · [Company]
   Địa điểm: [Location] · [Work Mode]
   Kỹ năng: [skills]   ← OMIT if no skills
   Link: [Xem chi tiết](url)

2. ANALYTICS (if run_analytics was called):
   **[One direct sentence with the key number/finding]**
   [2–3 sentences with supporting data — cite specific numbers.]

3. CAREER ADVICE (if career_advice was called):
   3–5 concrete sentences. Name specific technologies.

FINAL LINE: ONE actionable tip that ties all sections together.

RULES:
- CRITICAL: NEVER fabricate job listings. Only format job cards from the === SEARCH_JOBS === section.
  If that section says "No matching jobs found" or is absent → do NOT write any job cards at all.
- CRITICAL: NEVER invent company names, salaries, or job titles not present in the data.
- Never fabricate numbers not present in the provided data.
- Do not start with "Based on the data", "Here are", "Sure", or any filler.
- Do not repeat the user's question back.
- Skip "Kỹ năng:" entirely if the value would be "-" or "N/A".\
"""

# ── Tool result container ─────────────────────────────────────────────────────

@dataclass
class ToolResult:
    name:             str
    success:          bool = True
    error:            Optional[str]             = None
    # search_jobs
    jobs:             list[JobResult]           = field(default_factory=list)
    market_insight:   Optional[MarketInsight]   = None
    filters:          dict                      = field(default_factory=dict)
    # run_analytics
    sql_query:        Optional[str]             = None
    sql_result:       Optional[list[dict]]      = None
    chart:            Optional[dict]            = None
    # career_advice
    advice_question:  Optional[str]             = None


# ── Shared helpers (mirrors rag_pipeline.py utilities) ───────────────────────

_VI_CHARS = set(
    "àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ"
)

# LOCATION_ALIASES and WORK_MODE_KEYWORDS imported from constants.py


def _extract_filters(query: str) -> dict:
    q = query.lower()
    wm = next((k for k, kws in WORK_MODE_KEYWORDS.items() if any(kw in q for kw in kws)), None)
    loc = next((k for k, al in LOCATION_ALIASES.items() if any(a in q for a in al)), None)
    return {"work_mode": wm, "location": loc}


def _post_filter(jobs: list[JobResult], filters: dict) -> list[JobResult]:
    if not filters["work_mode"] and not filters["location"]:
        return jobs
    result = []
    for job in jobs:
        content = job.text_content.lower()
        fields = _parse_fields(job.text_content)
        mode_t = fields.get("Work Mode", "").lower()
        loc_t  = fields.get("Location",  "").lower()
        if filters["work_mode"]:
            kws = WORK_MODE_KEYWORDS[filters["work_mode"]]
            if not any(kw in mode_t or kw in content for kw in kws):
                continue
        if filters["location"]:
            als = LOCATION_ALIASES[filters["location"]]
            if not any(a in loc_t for a in als):
                continue
        result.append(job)
    return result


def _parse_fields(text: str) -> dict:
    out = {}
    for part in text.split(". "):
        if ": " in part:
            k, _, v = part.partition(": ")
            out[k.strip()] = v.strip()
    return out


def _table(rows: list[dict], limit: int = 30) -> str:
    if not rows:
        return "(no data)"
    sample = rows[:limit]
    hdrs = list(sample[0].keys())
    lines = [
        " | ".join(hdrs),
        " | ".join("---" for _ in hdrs),
        *(" | ".join(str(r.get(h, "")) for h in hdrs) for r in sample),
    ]
    if len(rows) > limit:
        lines.append(f"*(showing {limit} of {len(rows)} rows)*")
    return "\n".join(lines)


# ── Main service ──────────────────────────────────────────────────────────────

class AgentService:
    """
    @class AgentService
    @brief Agentic RAG — plan tool calls, execute in parallel, synthesize response.

    Replaces the hard-coded classify → route pipeline with a two-step LLM loop:
      1. Plan  : LLM selects tools + args  (fast, ~256 tokens output)
      2. Synthesize : LLM writes final response using all tool results

    Tools execute concurrently in a ThreadPoolExecutor so multi-tool queries
    do not pay a sequential latency penalty.
    """

    def __init__(
        self,
        query_processor: QueryProcessor,
        vector_search:   HybridSearchService,
        sql_agent:       SQLAgentService,
        market_ctx:      MarketContextService,
    ) -> None:
        self._llm    = OpenAI(api_key=settings.openai_api_key)
        self._qp     = query_processor
        self._search = vector_search
        self._sql    = sql_agent
        self._market = market_ctx

    # ── Planning ──────────────────────────────────────────────────────────────

    def _plan(self, query: str, history: list[dict]) -> list:
        """
        Call the LLM with tool specs and return a list of tool_call objects.
        Returns [] when the LLM decides no tool is needed (out-of-scope / direct answer).
        """
        messages: list[dict] = [{"role": "system", "content": _PLAN_SYSTEM}]
        if history:
            messages.extend(history[-6:])   # last 3 turns for follow-up context
        messages.append({"role": "user", "content": query})

        resp = self._llm.chat.completions.create(
            model=settings.openai_model,
            messages=messages,
            tools=TOOL_SPECS,
            tool_choice="auto",
            max_tokens=256,
        )
        choice = resp.choices[0]
        if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
            calls = choice.message.tool_calls
            names = [c.function.name for c in calls]
            logger.info("Agent plan → tools=%s", names)
            return calls
        # LLM chose to answer directly (no tools)
        direct = (choice.message.content or "").strip()
        logger.info("Agent plan → no tools, direct answer: '%s...'", direct[:60])
        return []

    # ── Tool execution ────────────────────────────────────────────────────────

    def _exec_search_jobs(self, query: str) -> ToolResult:
        try:
            expanded = self._qp.process(query)
            jobs     = self._search.search_multi(expanded)
            filters  = _extract_filters(query)
            jobs     = _post_filter(jobs, filters)
            insight  = self._market.get_insight(jobs, query, filters)
            logger.info("search_jobs → %d jobs", len(jobs))
            return ToolResult(name="search_jobs", jobs=jobs, market_insight=insight, filters=filters)
        except Exception as e:
            logger.error("search_jobs failed: %s", e, exc_info=True)
            return ToolResult(name="search_jobs", success=False, error=str(e))

    def _exec_analytics(self, question: str) -> ToolResult:
        try:
            pref = None
            for pat, ct in [
                (re.compile(r'line chart|biểu đồ đường', re.I), "line"),
                (re.compile(r'pie chart|biểu đồ tròn', re.I),   "pie"),
                (re.compile(r'bar chart|biểu đồ cột', re.I),    "bar"),
            ]:
                if pat.search(question):
                    pref = ct
                    break
            sql_q, rows, chart = self._sql.query(question, pref)
            logger.info("run_analytics → %d rows", len(rows) if rows else 0)
            return ToolResult(name="run_analytics", sql_query=sql_q, sql_result=rows, chart=chart)
        except Exception as e:
            logger.error("run_analytics failed: %s", e, exc_info=True)
            return ToolResult(name="run_analytics", success=False, error=str(e))

    def _exec_career_advice(self, question: str) -> ToolResult:
        return ToolResult(name="career_advice", advice_question=question)

    def _execute_parallel(self, tool_calls: list) -> list[ToolResult]:
        """Run all tool calls concurrently and collect results."""
        dispatch = {
            "search_jobs":   lambda a: self._exec_search_jobs(a["query"]),
            "run_analytics": lambda a: self._exec_analytics(a["question"]),
            "career_advice": lambda a: self._exec_career_advice(a["question"]),
        }

        def run_one(tc):
            name = tc.function.name
            args = json.loads(tc.function.arguments)
            fn   = dispatch.get(name)
            if fn is None:
                return ToolResult(name=name, success=False, error=f"Unknown tool: {name}")
            return fn(args)

        results: list[ToolResult] = []
        if len(tool_calls) == 1:
            results = [run_one(tool_calls[0])]
        else:
            with ThreadPoolExecutor(max_workers=min(len(tool_calls), 4)) as ex:
                futures = {ex.submit(run_one, tc): tc for tc in tool_calls}
                for fut in as_completed(futures):
                    results.append(fut.result())

        return results

    # ── Synthesis ─────────────────────────────────────────────────────────────

    def _build_synthesis_prompt(
        self,
        query: str,
        results: list[ToolResult],
        lang: str,
    ) -> str:
        parts: list[str] = [f"[LANG={lang}] User query: {query}\n"]

        for r in results:
            if not r.success:
                parts.append(f"[{r.name.upper()}] Tool failed: {r.error}\n")
                continue

            if r.name == "search_jobs":
                if r.jobs:
                    active = {k: v for k, v in r.filters.items() if v}
                    filter_str = (", ".join(f"{k}={v}" for k, v in active.items())) if active else "no filters"
                    parts.append(f"=== SEARCH_JOBS ({len(r.jobs)} results, {filter_str}) ===")
                    for i, job in enumerate(r.jobs, 1):
                        f = _parse_fields(job.text_content)
                        parts.append(
                            f"JOB {i}: {f.get('Job Title', job.job_title)} "
                            f"at {f.get('Company', '')}\n"
                            f"  Location: {f.get('Location','')} | Mode: {f.get('Work Mode','')}\n"
                            f"  Skills: {f.get('Skills','')}\n"
                            f"  URL: {job.job_link}"
                        )
                    if r.market_insight:
                        parts.append(format_market_block(r.market_insight))
                else:
                    parts.append("[SEARCH_JOBS] No matching jobs found.")

            elif r.name == "run_analytics":
                if r.sql_result:
                    parts.append(
                        f"=== ANALYTICS ({len(r.sql_result)} rows) ===\n"
                        + _table(r.sql_result)
                    )
                    if r.chart:
                        parts.append("[A chart is being rendered in the UI for this data.]")
                else:
                    parts.append("[ANALYTICS] Query returned 0 rows.")

            elif r.name == "career_advice":
                parts.append(
                    f"=== CAREER ADVICE ===\n"
                    f"Answer this career question using your knowledge: {r.advice_question}"
                )

        parts.append("\nSynthesize all sections above into one cohesive response.")
        return "\n".join(parts)

    def _synthesize(
        self,
        query: str,
        results: list[ToolResult],
        lang: str,
        history: list[dict],
    ) -> ChatResponse:
        prompt = self._build_synthesis_prompt(query, results, lang)
        system = _SYNTH_SYSTEM.format(lang=lang)

        msgs: list[dict] = [{"role": "system", "content": system}]
        if history:
            msgs.extend(history[-6:])
        msgs.append({"role": "user", "content": prompt})

        resp = self._llm.chat.completions.create(
            model=settings.openai_model,
            messages=msgs,
            max_tokens=1200,
        )
        answer = resp.choices[0].message.content

        # Collect response fields from tool results
        jobs: list[JobResult] | None  = None
        market_insight: MarketInsight | None = None
        sql_query:  str | None        = None
        sql_result: list[dict] | None = None
        charts: list[dict]            = []

        for r in results:
            if r.name == "search_jobs" and r.success:
                if r.jobs is not None:
                    jobs = r.jobs
                if r.market_insight is not None:
                    market_insight = r.market_insight
            elif r.name == "run_analytics" and r.success:
                sql_query  = r.sql_query
                sql_result = r.sql_result
                if r.chart:
                    charts.append(r.chart)

        # primary chart for backward-compat (existing chart field in ChatResponse)
        primary_chart = charts[0] if charts else None

        return ChatResponse(
            answer         = answer,
            query_type     = QueryType.agent,
            jobs           = jobs,
            market_insight = market_insight,
            sql_query      = sql_query,
            sql_result     = sql_result,
            chart          = primary_chart,
            charts         = charts,
        )

    # ── Public entry point ────────────────────────────────────────────────────

    def run(
        self,
        resolved_query: str,
        history: list[dict],
        lang: str,
    ) -> ChatResponse | None:
        """
        @brief Run the full agent loop: plan → execute → synthesize.

        @param resolved_query  Standalone query (already rewritten by _resolve_query).
        @param history         Recent conversation turns for context.
        @param lang            "Vietnamese" | "English".
        @return                ChatResponse, or None if agent should fall back to
                               classic pipeline (e.g. planning LLM error).
        """
        try:
            tool_calls = self._plan(resolved_query, history)
        except Exception as e:
            logger.error("Agent planning failed: %s — triggering fallback", e)
            return None     # caller will use classic pipeline

        if not tool_calls:
            # LLM decided no tools → out-of-scope or direct answer
            return None     # let classic pipeline handle out-of-scope canned response

        results = self._execute_parallel(tool_calls)
        return self._synthesize(resolved_query, results, lang, history)
