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
    ChatResponse, JobResult, LearningPathResult, MarketInsight, QueryType,
)
from services import conversation_store
from services.learning_path import LearningPathService
from services.market_context import MarketContextService, format_market_block
from services.query_processor import QueryProcessor
from services.sql_agent import SQLAgentService
from services.vector_search import HybridSearchService

logger = logging.getLogger(__name__)

# Cross-encoder rerank score gate: if the BEST candidate in the result set scores
# below this threshold the query has no relevance to the IT job corpus at all
# (e.g. "lao công", "thời tiết", "nấu ăn").  Typical relevant queries score ≥ 0;
# completely off-domain queries score ≪ -2.  Set conservatively to avoid false
# positives on niche-but-valid IT queries (e.g. "Fortran developer").
_MIN_RERANK_SCORE = -2.0

# ── Skill name formatter ───────────────────────────────────────────────────────
# DB stores skill names in ALL-CAPS. Map to proper display format.
_SKILL_DISPLAY: dict[str, str] = {
    # Acronyms → keep uppercase
    "AI": "AI", "ML": "ML", "SQL": "SQL", "AWS": "AWS", "GCP": "GCP",
    "API": "API", "HTML": "HTML", "CSS": "CSS", "PHP": "PHP",
    "QA": "QA", "QC": "QC", "QA QC": "QA/QC", "OOP": "OOP",
    "UI": "UI", "UX": "UX", "SDK": "SDK", "ETL": "ETL", "NLP": "NLP",
    "LLM": "LLM", "RPA": "RPA", "ERP": "ERP", "CRM": "CRM", "SAP": "SAP",
    "SEO": "SEO", "SRE": "SRE", "BI": "BI", "IT": "IT",
    # Special casing
    ".NET": ".NET", "NODE.JS": "Node.js", "VUE.JS": "Vue.js",
    "REACT.JS": "React.js", "NEXT.JS": "Next.js", "NUXT.JS": "Nuxt.js",
    "CICD": "CI/CD", "CI/CD": "CI/CD", "DEVOPS": "DevOps",
    "POSTGRESQL": "PostgreSQL", "MONGODB": "MongoDB", "MYSQL": "MySQL",
    "JAVASCRIPT": "JavaScript", "TYPESCRIPT": "TypeScript",
    "REACT NATIVE": "React Native", "SPRING BOOT": "Spring Boot",
    "GITHUB": "GitHub", "GITLAB": "GitLab",
    "ELASTICSEARCH": "Elasticsearch", "KUBERNETES": "Kubernetes",
    "TENSORFLOW": "TensorFlow", "PYTORCH": "PyTorch",
    "FASTAPI": "FastAPI", "CHATGPT": "ChatGPT", "OPENAI": "OpenAI",
    # Standard title-case (common enough to map explicitly)
    "PYTHON": "Python", "JAVA": "Java", "REACT": "React",
    "ANGULAR": "Angular", "VUE": "Vue", "DOCKER": "Docker",
    "LINUX": "Linux", "REDIS": "Redis", "KAFKA": "Kafka",
    "SPARK": "Spark", "HADOOP": "Hadoop", "AIRFLOW": "Airflow",
    "FLUTTER": "Flutter", "SWIFT": "Swift", "KOTLIN": "Kotlin",
    "SCALA": "Scala", "RUST": "Rust", "RUBY": "Ruby", "GO": "Go",
    "DJANGO": "Django", "FLASK": "Flask", "SPRING": "Spring",
    "LARAVEL": "Laravel", "ORACLE": "Oracle", "AZURE": "Azure",
    "CLOUD": "Cloud", "DATABASE": "Database", "MICROSERVICE": "Microservices",
    "ENGLISH": "English", "JAPANESE": "Japanese", "KOREAN": "Korean",
    "AGILE": "Agile", "SCRUM": "Scrum", "JIRA": "Jira",
    "BLOCKCHAIN": "Blockchain", "TAILWIND": "Tailwind",
    "AUTOMATION TEST": "Automation Test", "TEAM MANAGEMENT": "Team Management",
    "PROJECT MANAGEMENT": "Project Management",
    "BUSINESS ANALYSIS": "Business Analysis",
    "DATA ENGINEER": "Data Engineer", "DATA ANALYST": "Data Analyst",
    "DATA SCIENTIST": "Data Scientist",
}


def _fmt_skill(raw: str) -> str:
    """Format an ALL-CAPS DB skill name into proper display form."""
    key = raw.strip().upper()
    return _SKILL_DISPLAY.get(key, raw.strip().title())

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
                "Use for: skill rankings, company counts, work-mode ratios, "
                "top companies, job counts by location/category/time. "
                "Keywords that trigger this: 'bao nhiêu', 'top', 'ranking', 'thống kê', "
                "'phân bố', 'tỷ lệ', 'số lượng', 'nhiều nhất', 'how many', 'statistics'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": (
                            "Analytics question in natural language. "
                            "IMPORTANT: preserve ALL numbers and quantities from the user's message "
                            "exactly as stated. E.g. 'top 15 công ty' → question must contain '15', "
                            "'top 20 kỹ năng' → question must contain '20'. Never drop numeric values."
                        ),
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
                "skills to acquire — WITHOUT stating specific skills they already have. "
                "This tool does NOT query the database — pure LLM knowledge. "
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
    {
        "type": "function",
        "function": {
            "name": "learning_path",
            "description": (
                "Generate a DATA-DRIVEN skill learning roadmap from real Vietnam job market data. "
                "Use ONLY when the user explicitly states skills they ALREADY KNOW and asks what "
                "to learn next to reach a specific TARGET ROLE. "
                "Triggers: 'tao biết Python + SQL muốn vào Data Engineer', "
                "'có React + JS cần học gì để Senior Frontend?', "
                "'Java + Spring Boot để làm Backend cần thêm gì?', "
                "'I know X and Y, what do I need to become Z?'. "
                "DO NOT use for generic 'nên học gì' without stated known skills — use career_advice."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "target_role": {
                        "type": "string",
                        "description": "The job role the user wants to reach (e.g. 'Data Engineer', 'Backend Developer')",
                    },
                    "known_skills": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Skills the user already has, extracted from their message",
                    },
                },
                "required": ["target_role"],
            },
        },
    },
]

# ── Planning system prompt ─────────────────────────────────────────────────────

_PLAN_SYSTEM = """\
You are a dispatcher for an IT job-market AI assistant serving Vietnam's tech industry.

Given a user query, call the appropriate tool(s).

DATABASE FACTS (Gold layer — real ITviec data):
  • 8,114 distinct jobs | 504 skills (stored UPPERCASE) | 1,172 companies | 10 cities | 17 categories
  • Date range: 2025-09-23 → 2026-04-11 (8 months)
  • Work mode: 'At Office'(~98%) | 'Remote'(~1.2%) | 'Hybrid'(~0.1%) — office-dominant market.
  • Top cities: Ho Chi Minh(5264 job-rows) | Ha Noi(3075) | Da Nang(536) — call run_analytics for exact %.
  • ⚠ NO SALARY DATA — the database has zero salary information. Never call run_analytics for salary.

SKILL NAME ALIASES — resolve ANY variant to canonical before routing:
  golang/Golang/golng(typo) → GO (453 jobs)
  k8s/k8/kube               → KUBERNETES (193 jobs)
  react native/react-native/reactnative → REACT_NATIVE (205 jobs, UNDERSCORE in DB)
  nodejs/node js/NodeJS     → NODE.JS (616 jobs)
  vuejs/vue.js/VueJS        → VUE (165 jobs — NOT VUE.JS, DB has VUE)
  ci/cd / ci cd / cicd      → CICD (467 jobs, no slash in DB)
  scikit-learn/sklearn       → SCIKITLEARN (9 jobs)
  microservices              → MICROSERVICE (380 jobs, singular)
  elastic search             → ELASTICSEARCH (26 jobs)
  elk/elk stack              → ELK STACK (5 jobs, separate from ELASTICSEARCH)
  postgres                   → POSTGRESQL | dotnet/dot net → .NET
  airflow                    → APACHE AIRFLOW | apache kafka → KAFKA
  powerbi                    → POWER BI | tailwindcss → TAILWIND

CATEGORIES (17 total, exact names for SQL):
  Backend Development(1483) | Frontend Development(1041) | Testing & QA(744)
  Product & Business Analysis(572) | DevOps & Infrastructure(476) | Mobile Development(458)
  AI & Machine Learning(433) | Software Engineering(410) | Management(361)
  Data Engineering(278) | ERP & CRM(198) | Cyber Security(180)
  Fullstack Development(113) | Embedded & IoT(101) | Game Development(91)
  Data Analytics(83) | Other(1092)

ROUTING RULES (follow strictly):
1. Call search_jobs → user wants to FIND or SEE specific job postings.
   For "lương thế nào" (salary) questions: call search_jobs to show listings, then inform user
   that salary data is not available in the system (do NOT call run_analytics for salary).
   "SHOW MORE" follow-ups → ALWAYS call search_jobs. Recognise any phrase that asks for
   additional job results: "đưa tao thêm", "cho xem thêm", "còn việc nào nữa không",
   "thêm kết quả", "more jobs", "show more", "next page", "xem tiếp", "tiếp tục",
   "thêm nữa đi", "đưa thêm", "cho tao thêm".
   For these, use the SAME query (skill/role/location/level) as the PREVIOUS search turn
   from the conversation history. Example: if history shows "senior Python HCM" and user
   says "đưa tao thêm", call search_jobs(query="senior Python HCM tại văn phòng").

2. Call run_analytics → MANDATORY for ALL market statistics — NEVER answer these from LLM knowledge:
   • "top công ty" / "which companies hire most"
   • "kỹ năng hot nhất" / "top skills" / "in-demand skills"
   • "bao nhiêu job" / "how many jobs" / "job count by X"
   • "work mode" / "remote" / "hybrid" / "văn phòng" / "tỷ lệ làm việc" / "remote ratio"
   • "xu hướng tuyển dụng" / "hiring trend" / "by month/quarter"
   • "thị trường [skill/role] như thế nào" (except salary — no salary data)
   • "theo khu vực" / "North/South/HCM/Hanoi/thành phố" / "phân bố địa điểm"
   • "ngành nào nhiều job nhất" / "category breakdown"
   ⚠ Even if the system prompt contains approximate values, you MUST call run_analytics to get exact data.

3. Call learning_path → user explicitly states skills they ALREADY HAVE and wants a data-driven
   roadmap to a TARGET ROLE. Extract target_role + known_skills[] from the message.
   Triggers: "tao biết Python + SQL muốn vào Data Engineer", "có React muốn làm Frontend senior",
             "Java + Spring để làm Backend cần thêm gì?", "I know X, want to become Y".

4. Call career_advice → ONLY when real data cannot help: role comparisons (Backend vs Frontend),
   interview tips, soft skills, general learning advice WITHOUT specific known skills.
   NOT for salary (no data) or market stats (use run_analytics).

5. Call MULTIPLE tools when a query spans CLEARLY DIFFERENT intents — never drop an intent:
   "tìm job React + thị trường React?" → search_jobs + run_analytics
   "Data Engineer cần kỹ năng gì và thị trường ra sao?" → run_analytics + career_advice

6. Call NO tool only for: greetings, completely off-topic, nonsense messages.

QUERY ISOLATION — CRITICAL:
- For search_jobs, include ONLY filters (location/level/work_mode) that the user EXPLICITLY stated
  in their CURRENT message. NEVER inherit location, level, or work_mode from previous turns.
- Context inheritance (location/level/mode from history) applies ONLY to "show more" follow-ups
  (rule 1). For ANY new query — even if the previous query was in HCM — start fresh with only
  what the user said NOW.
- Example: history="fresher IT HCM", current="Tìm job Cloud Engineer AWS Azure" → query MUST be
  "Cloud Engineer AWS Azure" (no HCM, no fresher, because user did not mention them).

CRITICAL CONSTRAINTS:
- Call each tool AT MOST ONCE per request. NEVER call run_analytics more than once.
- run_analytics answers ONE question with ONE SQL query — no matter how many groups/categories:
  "top 3 kỹ năng mỗi ngành IT" → ONE call, ONE SQL with ROW_NUMBER() OVER (PARTITION BY category)
  "kỹ năng nào xuất hiện cùng Python VÀ chiếm bao nhiêu %?" → ONE call, ONE SQL with both columns
  "top 5 công ty mỗi thành phố" → ONE call, ONE SQL with ROW_NUMBER() OVER (PARTITION BY city)
  Making N separate run_analytics calls (one per category/city/skill) is ALWAYS WRONG.
  Do NOT split by group/category into multiple calls — use window functions in a single query.
- Do NOT add unrequested time-trend analysis as "extra context".

IMPORTANT: If a query contains a role/skill AND a location, ALWAYS call search_jobs.\
"""

# ── Synthesis system prompt ───────────────────────────────────────────────────

_SYNTH_SYSTEM = """\
You are an IT recruitment AI assistant for Vietnam's tech job market (ITviec data).
You have retrieved data from multiple sources. Synthesize it into ONE cohesive response.

LANGUAGE RULE: Respond in {lang}. No exceptions.

OUTPUT STRUCTURE (follow in order, only include sections that have data):

1. JOBS (if search_jobs was called):
   If the user query is a "show more" / continuation request ("đưa thêm", "xem tiếp",
   "more jobs", etc.) lead with: "Đây là thêm [N] kết quả tiếp theo [location/mode nếu có]."
   Otherwise lead with: "Đây là [N] kết quả phù hợp nhất [location/mode nếu có filter]."
   where N = the number in "(N results, ...)" from the SEARCH_JOBS header.
   If MARKET CONTEXT is present, append: ", trong tổng số [X] vị trí [skill] trên thị trường [city nếu có]"
   where X = "Tổng vị trí thực tế" from MARKET CONTEXT.
   If MARKET CONTEXT is absent, do NOT append any total count. End the lead sentence after the location/mode.
   NEVER use the "8,114 distinct jobs" database fact as a filtered market count — 8,114 is the TOTAL database
   size across all skills/locations/levels, NOT a count for any specific search.
   NEVER say "Có N việc làm" or "Chỉ có N vị trí" — N is the TOP results shown, not the market total.
   Only mention location/work-mode if the === SEARCH_JOBS header shows an active filter for it.
   CRITICAL: List EVERY single job shown in the === SEARCH_JOBS section — the exact same
   count as "(N results, ...)" in its header. NEVER omit, skip, or summarize-away any of them
   (this must be deterministic — the same input list always produces the same N job blocks).
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

4. LEARNING PATH (if learning_path was called):
   Open with: "Dựa trên [total_jobs] job [role_category] trên thị trường:"
   Present as a numbered roadmap (1–10). Format each skill exactly as:
   **[N]. [Skill Name]** — xuất hiện trong [market_freq]% job [role]
   Do NOT add group labels like ([Other]) or ([Backend]) — use the skill name only.
   After the list, 2–3 sentences: WHY the top skills rank high, how they connect to known skills.
   NEVER fabricate percentages — only use numbers from the LEARNING PATH data section.

FINAL LINE: ONE actionable tip that ties all sections together.

DATABASE FACTS YOU MUST NEVER CONTRADICT:
  Source: ITviec Vietnam | Period: 2025-09-23 → 2026-04-11 | 8,114 distinct jobs
  ⚠ NO SALARY DATA — the database contains zero salary information.
  Work mode and city distribution: use ONLY numbers from the ANALYTICS section — never invent or approximate.
  Skills stored UPPERCASE: PYTHON, JAVA, REACT, SQL, JAVASCRIPT, etc.

GROUND TRUTH RULES:
- Every number, percentage, company name, or skill stat you write MUST come from the data
  sections provided below (SEARCH_JOBS, ANALYTICS, LEARNING PATH, MARKET CONTEXT).
- If it's not in those sections → DO NOT write it. Say "không có trong dữ liệu" instead.
- SALARY: This database has NO salary data. When user asks about salary, say:
  "Hệ thống chưa có dữ liệu lương. Vui lòng xem trực tiếp các job listing để biết mức lương."
  NEVER invent salary ranges from LLM training knowledge.
- NEVER use LLM training knowledge for: top companies, skill demand %, remote ratios,
  hiring trends, job counts. These MUST come from the ANALYTICS or MARKET CONTEXT sections.
- GROWTH/DECLINE INTERPRETATION:
  * If user asks "tăng trưởng mạnh nhất / grew the most" but ALL growth_pct < 0:
    → The answer is the row with the HIGHEST (least negative) growth_pct = "giảm ít nhất".
    → Say: "Toàn bộ các ngành đều giảm trong kỳ này. Ngành [X] có mức giảm ít nhất ([Y]%),
      tức là hiệu suất tốt nhất tương đối trong bối cảnh thị trường đi xuống."
    → NEVER highlight the most negative row (giảm nhiều nhất) as the answer.
  * If user asks "giảm mạnh nhất / declined the most":
    → The answer is the row with the LOWEST (most negative) growth_pct.
  * Match the user intent (tăng → ít giảm nhất; giảm → nhiều giảm nhất).
- PARTIAL DATA WARNING: The database starts at 2025-09-23 (only ~8 days in September).
  If analytics shows an extreme spike in October (e.g. 900%+ MoM growth), always note:
  "Lưu ý: tháng 9/2025 chỉ có dữ liệu từ ngày 23, nên con số tăng trưởng tháng 10 không phản ánh thực tế."
  Do NOT present the Sep→Oct spike as a real market trend.

RULES:
- CRITICAL: NEVER fabricate job listings. Format cards ONLY from === SEARCH_JOBS ===.
  If absent or "No matching jobs found" → write NO job cards.
- CRITICAL: NEVER invent company names, salary ranges, percentages not in the data.
- CRITICAL: NEVER add a year (2023, 2024, 2025, 2026) to headings or text unless the user
  explicitly asked for a specific year. "top 10 kỹ năng backend" → heading is just
  "Top 10 kỹ năng backend" NOT "Top 10 kỹ năng backend 2023".
- Do not start with "Based on the data", "Here are", "Sure", or any filler.
- Do not repeat the user's question back.
- Skip "Kỹ năng:" if value would be "-" or "N/A".
- When ANALYTICS has real numbers, lead with a bolded key figure from the data.\
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
    charts:           list[dict]                = field(default_factory=list)
    # career_advice
    advice_question:  Optional[str]             = None
    # learning_path
    learning_path_result: Optional[LearningPathResult] = None


# ── Shared helpers (mirrors rag_pipeline.py utilities) ───────────────────────

# LOCATION_ALIASES and WORK_MODE_KEYWORDS imported from constants.py

# Seniority-level tokens for post-filter detection.
# Must use word-boundary regex so "senior" doesn't match "seniority",
# "lead" doesn't match "leads", etc.
_LEVEL_TOKENS_SEARCH: list[str] = [
    "fresher", "intern", "junior", "entry",
    "middle", "mid-level", "mid level",
    "senior", "lead", "principal", "staff",
]


def _extract_filters(query: str) -> dict:
    q = query.lower()
    wm  = next((k for k, kws in WORK_MODE_KEYWORDS.items() if any(kw in q for kw in kws)), None)
    loc = next((k for k, al in LOCATION_ALIASES.items() if any(a in q for a in al)), None)
    lvl = next(
        (t for t in _LEVEL_TOKENS_SEARCH if re.search(rf"\b{re.escape(t)}\b", q)),
        None,
    )
    return {"work_mode": wm, "location": loc, "level": lvl}


def _post_filter(jobs: list[JobResult], filters: dict) -> list[JobResult]:
    """Soft-preference filter: jobs matching all active filters come first;
    non-matching jobs fill remaining slots so the caller always has enough
    candidates to truncate to `desired`.

    Filters checked (all optional):
      • work_mode — job's Work Mode field must contain the mode keywords
      • location  — job's Location field must contain a city alias
      • level     — job's title must contain the seniority token (word-boundary)

    Jobs matching ALL active filters → matched bucket (shown first).
    Jobs failing any filter          → fallback bucket (fill remaining slots).
    """
    has_wm  = bool(filters.get("work_mode"))
    has_loc = bool(filters.get("location"))
    has_lvl = bool(filters.get("level"))
    if not has_wm and not has_loc and not has_lvl:
        return jobs
    matched, fallback = [], []
    for job in jobs:
        content = job.text_content.lower()
        fields  = _parse_fields(job.text_content)
        title_t = job.job_title.lower()
        mode_t  = fields.get("Work Mode", "").lower()
        loc_t   = fields.get("Location",  "").lower()
        passes  = True
        if has_wm:
            kws = WORK_MODE_KEYWORDS[filters["work_mode"]]
            if not any(kw in mode_t or kw in content for kw in kws):
                passes = False
        if passes and has_loc:
            als = LOCATION_ALIASES[filters["location"]]
            if not any(a in loc_t for a in als):
                passes = False
        if passes and has_lvl:
            lvl = filters["level"]
            # Entry-level tokens (fresher/junior/intern/entry) are synonyms.
            # "fresher" query should also match titles containing "junior" or "intern".
            _ENTRY_SYNS = {"fresher", "intern", "junior", "entry"}
            if lvl in _ENTRY_SYNS:
                if not any(re.search(rf"\b{re.escape(t)}\b", title_t) for t in _ENTRY_SYNS):
                    passes = False
            elif not re.search(rf"\b{re.escape(lvl)}\b", title_t):
                passes = False
        (matched if passes else fallback).append(job)
    logger.debug(
        "_post_filter: %d matched / %d fallback (wm=%s loc=%s lvl=%s)",
        len(matched), len(fallback),
        filters.get("work_mode"), filters.get("location"), filters.get("level"),
    )
    return matched + fallback  # matching first; caller truncates to desired


def _job_key(job) -> str:
    """Stable identity key: 'normalised_title||normalised_company'.

    Used for both within-turn dedup and cross-turn exclusion so that the same
    job posted on multiple dates (different job_links, same title+company) is
    correctly identified as a duplicate.
    """
    title   = re.sub(r'\s+', ' ', job.job_title.strip().lower())
    company = _parse_fields(job.text_content).get("Company", "").strip().lower()
    return f"{title}||{company}"


def _parse_fields(text: str) -> dict:
    out = {}
    for part in text.split(". "):
        if ": " in part:
            k, _, v = part.partition(": ")
            out[k.strip()] = v.strip()
    return out


# ── "Show more" detection ─────────────────────────────────────────────────────
# Matches any phrase that asks for additional job results from a prior search.
# Used as a fallback when the planning LLM doesn't route these to search_jobs.
_SHOW_MORE_RE = re.compile(
    r"(?:"
    r"đưa.*thêm|cho.*thêm|xem thêm|thêm.*job|thêm.*việc|thêm kết quả|"
    r"còn.*nữa|nữa đi|thêm nữa|tiếp tục xem|xem tiếp|"
    r"more\s*jobs|more\s*results|show.{0,6}more|next\s*page|load\s*more"
    r")",
    re.I | re.UNICODE,
)

# Signals that a user message is likely a job search (used to find the prior query).
_SEARCH_SIGNAL_RE = re.compile(
    r"(?:job|việc|tìm|senior|junior|middle|fresher|intern|developer|engineer|"
    r"python|java|react|angular|vue|golang|node|backend|frontend|fullstack|devops|mobile|"
    r"tuyển|lập trình|data|ai\b|ml\b|kotlin|swift|flutter|docker|kubernetes|k8s)",
    re.I,
)


def _last_search_query_from_history(history: list[dict]) -> str | None:
    """Scan backwards through history and return the most recent user message
    that looks like a job search query."""
    for msg in reversed(history):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if _SEARCH_SIGNAL_RE.search(content):
                return content
    return None


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
        query_processor:    QueryProcessor,
        vector_search:      HybridSearchService,
        sql_agent:          SQLAgentService,
        market_ctx:         MarketContextService,
        learning_path_svc:  LearningPathService,
    ) -> None:
        self._llm           = OpenAI(api_key=settings.openai_api_key)
        self._qp            = query_processor
        self._search        = vector_search
        self._sql           = sql_agent
        self._market        = market_ctx
        self._learning_path = learning_path_svc

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

    def _exec_search_jobs(self, query: str, exclude_keys: set[str] | None = None, original_query: str = "") -> ToolResult:
        try:
            intent_query = (
                original_query
                if original_query and not _SHOW_MORE_RE.search(original_query)
                else query
            )
            expanded = self._qp.process(query)
            desired  = settings.top_k_results
            filters  = _extract_filters(intent_query)
            query_skills = self._market.extract_query_skills(intent_query)

            # Over-fetch: headroom for (a) location/mode/level post-filter,
            # (b) cross-turn exclude, (c) within-turn title+company dedup.
            # The more filters are active simultaneously the lower the base-rate
            # of matching documents in the corpus, so we scale pool_k up:
            #   • no filters           → small pool (desired × 2)
            #   • 1 filter active      → retrieval_k (20)
            #   • 2+ filters active    → retrieval_k × 2, capped at 40
            # e.g. "senior Python HCM at office" hits ~3 % of the corpus, so
            # pool_k=40 gives a reasonable chance of finding 5 matching jobs.
            n_active = sum([
                bool(filters.get("work_mode")),
                bool(filters.get("location")),
                bool(filters.get("level")),
            ])
            if n_active >= 2:
                pool_k = min(settings.retrieval_k * 2, 40)
            elif n_active == 1:
                pool_k = settings.retrieval_k
            else:
                pool_k = min(desired + len(exclude_keys or ()) + desired, settings.retrieval_k)
            jobs   = self._search.search_multi(expanded, top_k=pool_k, pool_k=pool_k)

            insight = self._market.get_insight(
                jobs,
                intent_query,
                filters,
                original_query=original_query or intent_query,
            )

            has_structured_filter = bool(
                insight and (
                    # co_skills alone is NOT a structured filter — only explicit user
                    # constraints (location, mode, level, date, category, company) warrant
                    # Gold exact search. Multi-skill AND queries without other filters use
                    # Milvus (better recall; avoids 0-result Gold SQL paths from category
                    # mismatch or INNER JOIN dropping partially-indexed jobs).
                    insight.location_filter
                    or insight.region_filter
                    or insight.work_mode_filter
                    or insight.level_filter
                    or insight.date_filter
                    or insight.category_filter
                    or insight.company_filter
                )
            )
            # Use exact Gold search only when a structured filter is active.
            # When only co-skills are present (no location/mode/etc.), Milvus gives
            # better recall than Gold SQL INNER JOINs (which can under-return due to
            # strict category joins or partially-indexed dimension rows).
            should_use_exact_gold = bool(query_skills and has_structured_filter)
            used_exact_gold = False
            if should_use_exact_gold:
                exact_jobs = self._market.search_matching_jobs(
                    intent_query,
                    filters,
                    limit=desired + len(exclude_keys or ()),
                    original_query=original_query or intent_query,
                    fallback_jobs=jobs,
                )
                if exact_jobs is not None:
                    jobs = exact_jobs
                    used_exact_gold = True
                    logger.info(
                        "search_jobs: using exact Gold job cards (%d rows) for structured query.",
                        len(jobs),
                    )

            # Relevance gate: off-domain queries (e.g. "lao công") score ≪ -2.
            # Bypass when explicit structural filters (location/level/mode) are present:
            # the user IS looking for IT jobs, just constrained. Low cross-encoder scores
            # on filtered queries are expected (e.g. "no experience required" sub-query
            # doesn't match "3+ years experience" job descriptions, but it's still IT).
            has_explicit_filters = bool(
                filters.get("location") or filters.get("work_mode") or filters.get("level")
            )
            if not used_exact_gold and not has_explicit_filters and jobs and max(j.score for j in jobs) < _MIN_RERANK_SCORE:
                logger.info(
                    "search_jobs: best score %.3f < threshold %.1f — off-domain, returning empty",
                    max(j.score for j in jobs), _MIN_RERANK_SCORE,
                )
                return ToolResult(name="search_jobs", jobs=[], market_insight=None, filters=filters)

            # Cross-turn exclude: remove jobs already shown in prior turns
            # (keyed by title+company so different-date scrapes of the same job are caught).
            if exclude_keys:
                jobs = [j for j in jobs if _job_key(j) not in exclude_keys]

            if not used_exact_gold:
                jobs = _post_filter(jobs, filters)

            # Within-turn dedup by (title, company): keeps distinct opportunities even
            # when multiple companies post the same role title.
            seen: set[str] = set()
            deduped: list[JobResult] = []
            for job in jobs:
                k = _job_key(job)
                if k not in seen:
                    seen.add(k)
                    deduped.append(job)
            jobs = deduped[:desired]

            logger.info("search_jobs → %d jobs (excluded=%d)", len(jobs), len(exclude_keys or ()))
            return ToolResult(name="search_jobs", jobs=jobs, market_insight=insight, filters=filters)
        except Exception as e:
            logger.error("search_jobs failed: %s", e, exc_info=True)
            return ToolResult(name="search_jobs", success=False, error=str(e))

    def _exec_analytics(self, question: str, original_query: str = "") -> ToolResult:
        try:
            pref = None
            for pat, ct in [
                (re.compile(r'line chart|biểu đồ đường|dạng đường', re.I),              "line"),
                (re.compile(r'pie chart|biểu đồ tròn|biểu đồ bánh|dạng tròn', re.I),   "pie"),
                (re.compile(r'doughnut|donut|biểu đồ nhẫn', re.I),                      "doughnut"),
                (re.compile(r'bar chart|biểu đồ cột|dạng cột', re.I),                   "bar"),
            ]:
                # Check both the planning LLM's question AND the original user query
                # because the planning LLM may strip chart-type keywords when rewriting.
                if pat.search(question) or (original_query and pat.search(original_query)):
                    pref = ct
                    break

            # If the planning LLM dropped a numeric quantity (top N) from the question,
            # re-inject it from the original user query so SQL LIMIT is correct.
            num_match = re.search(r'\b(top\s+)?(\d+)\b', question, re.I)
            if not num_match and original_query:
                orig_match = re.search(r'\b(top\s+)?(\d+)\b', original_query, re.I)
                if orig_match:
                    n = orig_match.group(0).strip()
                    question = f"{question} (lấy {n} kết quả)"
                    logger.debug("Injected missing count '%s' into analytics question", n)

            sql_q, rows, charts = self._sql.query(question, pref)
            logger.info("run_analytics → %d rows, %d chart(s)", len(rows) if rows else 0, len(charts))
            return ToolResult(name="run_analytics", sql_query=sql_q, sql_result=rows, charts=charts)
        except Exception as e:
            logger.error("run_analytics failed: %s", e, exc_info=True)
            return ToolResult(name="run_analytics", success=False, error=str(e))

    def _exec_career_advice(self, question: str) -> ToolResult:
        return ToolResult(name="career_advice", advice_question=question)

    def _exec_learning_path(self, target_role: str, known_skills: list[str]) -> ToolResult:
        try:
            result = self._learning_path.analyze(target_role, known_skills or [])
            logger.info(
                "learning_path → category=%s steps=%d",
                result.role_category if result else "N/A",
                len(result.steps) if result else 0,
            )
            return ToolResult(name="learning_path", learning_path_result=result)
        except Exception as e:
            logger.error("learning_path failed: %s", e, exc_info=True)
            return ToolResult(name="learning_path", success=False, error=str(e))

    def _execute_parallel(
        self,
        tool_calls: list,
        original_query: str = "",
        exclude_keys: set[str] | None = None,
    ) -> list[ToolResult]:
        """Run all tool calls concurrently and collect results."""
        dispatch = {
            "search_jobs":   lambda a: self._exec_search_jobs(a["query"], exclude_keys, original_query=original_query),
            "run_analytics": lambda a: self._exec_analytics(a["question"], original_query),
            "career_advice": lambda a: self._exec_career_advice(a["question"]),
            "learning_path": lambda a: self._exec_learning_path(
                a["target_role"], a.get("known_skills", [])
            ),
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
                    if r.charts:
                        parts.append(f"[{len(r.charts)} chart(s) are being rendered in the UI for this data.]")
                else:
                    parts.append("[ANALYTICS] Query returned 0 rows.")

            elif r.name == "career_advice":
                parts.append(
                    f"=== CAREER ADVICE ===\n"
                    f"Answer this career question using your knowledge: {r.advice_question}"
                )

            elif r.name == "learning_path":
                lpr = r.learning_path_result
                if lpr and lpr.steps:
                    known_str = ", ".join(lpr.known_skills) if lpr.known_skills else "không có"
                    rows = "\n".join(
                        f"{i}. {_fmt_skill(s.skill_name)} — xuất hiện trong {s.market_freq}% job {lpr.role_category}"
                        + (f" | bridge {s.bridge_score}%" if s.bridge_score > 0 else "")
                        for i, s in enumerate(lpr.steps, 1)
                    )
                    parts.append(
                        f"=== LEARNING PATH ===\n"
                        f"Target role: {lpr.target_role} → market category: {lpr.role_category}\n"
                        f"Total jobs analyzed: {lpr.total_jobs}\n"
                        f"User's known skills: {known_str}\n"
                        f"Top skills to learn next (from real job market data):\n{rows}\n"
                        f"INSTRUCTION: Present as a numbered roadmap. Use exact skill names and percentages above. "
                        f"Do not add skills not in this list."
                    )
                else:
                    parts.append("[LEARNING PATH] No data found for this role.")

        parts.append("\nSynthesize all sections above into one cohesive response.")
        return "\n".join(parts)

    def _synthesize(
        self,
        query: str,
        results: list[ToolResult],
        lang: str,
        history: list[dict],
    ) -> ChatResponse:
        # ── OR query: merge multiple search_jobs insights ─────────────────────
        # When the agent splits "A hoặc B" into two search_jobs calls, each
        # get_insight sees only 1 skill → or_skill_totals is empty in both.
        # Fix: pick primary (most jobs), inject all skills into or_skill_totals,
        # and clear non-primary market_insight so the synthesis prompt shows
        # exactly one market block (with the full OR comparison).
        _or_primary: "ToolResult | None" = None
        search_with_insight = [
            r for r in results
            if r.name == "search_jobs" and r.success and r.market_insight is not None
        ]
        if len(search_with_insight) > 1:
            _or_primary = max(search_with_insight, key=lambda r: r.market_insight.total_jobs)
            merged_totals: dict[str, int] = {}
            for r in search_with_insight:
                if r.market_insight.or_skill_totals:
                    for skill, total in r.market_insight.or_skill_totals.items():
                        merged_totals.setdefault(skill, total)
                else:
                    merged_totals.setdefault(
                        r.market_insight.primary_skill,
                        r.market_insight.total_jobs,
                    )
            _or_primary.market_insight.or_skill_totals = merged_totals
            for r in search_with_insight:
                if r is not _or_primary:
                    r.market_insight = None
            logger.info(
                "OR merge: primary=%s (%d jobs), or_skill_totals=%s",
                _or_primary.market_insight.primary_skill,
                _or_primary.market_insight.total_jobs,
                merged_totals,
            )

        prompt = self._build_synthesis_prompt(query, results, lang)
        system = _SYNTH_SYSTEM.format(lang=lang)

        msgs: list[dict] = [{"role": "system", "content": system}]
        if history:
            msgs.extend(history[-6:])
        msgs.append({"role": "user", "content": prompt})

        resp = self._llm.chat.completions.create(
            model=settings.openai_model,
            messages=msgs,
            max_tokens=2000,
            temperature=0.3,   # low temp: job listings/numbers must be reproduced verbatim, not "creatively" varied/dropped
        )
        answer = resp.choices[0].message.content

        # Collect response fields from tool results
        jobs: list[JobResult] | None          = None
        market_insight: MarketInsight | None  = None
        sql_query:  str | None                = None
        sql_result: list[dict] | None         = None
        charts: list[dict]                    = []
        learning_path_result: LearningPathResult | None = None

        analytics_count = 0
        for r in results:
            if r.name == "search_jobs" and r.success:
                if r.jobs is not None:
                    jobs = r.jobs
                if r.market_insight is not None:
                    market_insight = r.market_insight
            elif r.name == "run_analytics" and r.success:
                analytics_count += 1
                # Keep only the FIRST analytics result's query/data for display
                # (prevents duplicate charts when agent incorrectly plans 2 analytics calls)
                if analytics_count == 1:
                    sql_query  = r.sql_query
                    sql_result = r.sql_result
                    charts.extend(r.charts)   # may be 1 or 2 charts
                elif analytics_count > 1:
                    logger.warning("Agent made %d run_analytics calls — ignoring extra", analytics_count)
            elif r.name == "learning_path" and r.success:
                learning_path_result = r.learning_path_result

        # OR query: ensure primary result's jobs win (not arbitrary last-loop order)
        if _or_primary is not None and _or_primary.jobs:
            jobs = _or_primary.jobs

        # primary chart for backward-compat (existing chart field in ChatResponse)
        primary_chart = charts[0] if charts else None

        return ChatResponse(
            answer                = answer,
            query_type            = QueryType.agent,
            jobs                  = jobs,
            market_insight        = market_insight,
            sql_query             = sql_query,
            sql_result            = sql_result,
            chart                 = primary_chart,
            charts                = charts,
            learning_path         = learning_path_result,
        )

    # ── Public entry point ────────────────────────────────────────────────────

    def run(
        self,
        resolved_query: str,
        history: list[dict],
        lang: str,
        conversation_id: str = "",
    ) -> ChatResponse | None:
        """
        @brief Run the full agent loop: plan → execute → synthesize.

        @param resolved_query   Standalone query (already rewritten by _resolve_query).
        @param history          Recent conversation turns for context.
        @param lang             "Vietnamese" | "English".
        @param conversation_id  Client-generated conversation identifier. When present,
                                search_jobs excludes job_links already shown earlier in
                                this conversation, so "show me more/different jobs"
                                follow-ups don't just repeat the same top-ranked results.
        @return                 ChatResponse, or None if agent should fall back to
                                classic pipeline (e.g. planning LLM error).
        """
        try:
            tool_calls = self._plan(resolved_query, history)
        except Exception as e:
            logger.error("Agent planning failed: %s — triggering fallback", e)
            return None     # caller will use classic pipeline

        exclude_keys = conversation_store.get_shown_job_keys(conversation_id) if conversation_id else None

        if not tool_calls:
            # Before giving up, check for "show more" follow-ups that the planning LLM
            # may have missed ("đưa tao thêm nữa", "cho xem thêm", "more jobs", …).
            if _SHOW_MORE_RE.search(resolved_query):
                last_q = _last_search_query_from_history(history)
                if last_q:
                    logger.info(
                        "show-more fallback: re-running search for '%s'", last_q[:80]
                    )
                    results = [self._exec_search_jobs(last_q, exclude_keys, original_query=resolved_query)]
                    if conversation_id:
                        for r in results:
                            if r.name == "search_jobs" and r.success and r.jobs:
                                conversation_store.add_shown_job_keys(
                                    conversation_id, [_job_key(j) for j in r.jobs]
                                )
                    return self._synthesize(resolved_query, results, lang, history)
            # LLM decided no tools and no show-more pattern → out-of-scope / direct answer
            return None     # let classic pipeline handle out-of-scope canned response

        results = self._execute_parallel(tool_calls, original_query=resolved_query, exclude_keys=exclude_keys)

        if conversation_id:
            for r in results:
                if r.name == "search_jobs" and r.success and r.jobs:
                    conversation_store.add_shown_job_keys(conversation_id, [_job_key(j) for j in r.jobs])

        return self._synthesize(resolved_query, results, lang, history)
