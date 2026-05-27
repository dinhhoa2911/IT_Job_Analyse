"""
@file rag_pipeline.py
@brief Orchestrates the full RAG pipeline: classify → retrieve → generate.

Each incoming chat message is routed through four branches depending on the
QueryClassifier's decision:
  - search_job    → HybridSearchService (Milvus) → LLM with job-card prompt.
  - analytics     → SQLAgentService (Trino)      → LLM with chain-of-thought prompt.
  - career_advice → LLM only (no retrieval).
  - out_of_scope  → fixed canned response, no LLM call.

Helper utilities handle language detection, query-level post-filtering, prompt
construction, and the optional market-context block from the Gold layer.
"""

import json
import logging
import re

from openai import OpenAI

from config import settings
from constants import LOCATION_ALIASES, WORK_MODE_KEYWORDS
from models.schemas import ChatRequest, ChatResponse, JobResult, QueryType
from services.agent import AgentService
from services.learning_path import LearningPathService
from services.market_context import MarketContextService, format_market_block
from services.query_classifier import QueryClassifier, is_greeting, is_short_off_topic
from services.query_processor import QueryProcessor
from services.sql_agent import SQLAgentService
from services.vector_search import HybridSearchService

logger = logging.getLogger(__name__)

# ── Language detection ────────────────────────────────────────────────────────

_VI_CHARS = set(
    "àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ"
)

def _detect_lang(text: str) -> str:
    return "Vietnamese" if any(c in _VI_CHARS for c in text.lower()) else "English"


# Chart type keywords the user may include in the query
_CHART_PREF_RULES = [
    (re.compile(r'line chart|biểu đồ đường|đường kẻ|dạng đường', re.IGNORECASE), "line"),
    (re.compile(r'area chart|biểu đồ vùng|dạng vùng', re.IGNORECASE),            "area"),
    (re.compile(r'pie chart|biểu đồ tròn|biểu đồ bánh|dạng tròn', re.IGNORECASE),"pie"),
    (re.compile(r'doughnut|donut|biểu đồ nhẫn|dạng nhẫn', re.IGNORECASE),        "doughnut"),
    (re.compile(r'horizontal|biểu đồ ngang|dạng ngang|nằm ngang', re.IGNORECASE),"horizontalBar"),
    (re.compile(r'bar chart|biểu đồ cột|dạng cột', re.IGNORECASE),               "bar"),
]


def _extract_chart_preference(question: str) -> str | None:
    """
    @brief Detect an explicit chart-type preference in the user's query string.

    @param question  Raw or resolved user query.
    @return          Chart type string (e.g. "line", "bar", "pie") or None if not specified.
    """
    for pattern, ctype in _CHART_PREF_RULES:
        if pattern.search(question):
            return ctype
    return None


# ── System prompts ────────────────────────────────────────────────────────────

# search_job: domain expert + strict output structure
_SEARCH_SYSTEM_PROMPT = """\
You are an IT recruitment specialist with deep knowledge of Vietnam's tech job market, \
specialising in data from ITviec — the leading Vietnamese IT job platform.

LANGUAGE RULE: Respond in {lang}. No exceptions.

EXPERTISE:
- You know the salary bands, required skills, and career paths for every IT role in Vietnam.
- You can read between the lines of a job posting and give honest advice.

OUTPUT FORMAT — follow this exactly for each job:
### [Job Title] · [Company]
Địa điểm: [City] · [Work Mode]
Kỹ năng: [skills, comma-separated]  ← OMIT this line entirely if no skills data
Link: [Xem chi tiết](url)

RULES:
- Lead with 1 sentence summarising the result set (count, dominant location/stack).
- Present each job as a card using the format above.
- NEVER show "Kỹ năng: -" or "Kỹ năng: N/A" — if skills are missing, skip the line.
- End with 1 concrete tip relevant to this specific search (not generic advice).
- Use only data provided. Never fabricate skills, salary figures, or company details.
- Do NOT start with "Dưới đây là", "Here are", "Sure", or any filler opener.\
"""

# analytics: data analyst persona + mandatory chain-of-thought
_ANALYTICS_SYSTEM_PROMPT = """\
You are a senior IT labour-market analyst. You interpret job-market data from ITviec \
and communicate findings clearly to job seekers and recruiters in Vietnam.

LANGUAGE RULE: Respond in {lang}. No exceptions.

CHAIN-OF-THOUGHT — work through these steps internally before writing your answer:
  1. Identify the exact metric the user wants (count, ratio, trend, ranking, …).
  2. Scan the data for the top signal (highest value, biggest gap, unexpected result).
  3. Decide whether a comparison or trend adds meaningful context.
  4. Formulate one concrete, actionable takeaway for the user.

OUTPUT FORMAT:
**[One-sentence direct answer to the question with the key number/finding]**

[2-4 sentences of supporting analysis — cite specific numbers, name specific skills or \
companies when relevant. Point out anything surprising in the data.]

> [One actionable insight or recommendation based on the finding]

RULES:
- Show specific figures (numbers, percentages, rankings). Vague statements add no value.
- If the data is sparse or ambiguous, say so briefly and suggest a refined question.
- Do NOT start with "Based on the data", "The results show", or any filler opener.\
"""

# career_advice: senior dev mentor — opinionated, no hedging
_CAREER_ADVICE_SYSTEM_PROMPT = """\
You are a senior software engineer with 10+ years in Vietnam's IT industry. \
You give honest, direct career advice — the kind you'd give a junior colleague, \
not a generic article.

LANGUAGE RULE: Respond in {lang}. No exceptions.

SCOPE: Only answer questions about IT skills, career roadmaps, job roles, salary \
expectations, or hiring trends in Vietnam.

RULES:
- Be specific. Name actual technologies, companies, salary ranges when you know them.
- Be direct. Skip "It depends" hedges unless the nuance genuinely matters.
- Be concise. A good answer is often 3-6 sentences. Use a short bullet list only when \
  listing truly parallel items (e.g. a tech stack).
- Do NOT end with "Hope this helps", "Good luck", or any filler closer.\
"""

# greeting — detailed feature intro shown when user says hi
_GREETING_VI = (
    "Xin chào! Tôi là **IT Job Analyst** — trợ lý AI phân tích thị trường tuyển dụng CNTT Việt Nam.\n\n"
    "Tôi có thể giúp bạn:\n\n"
    "**Tìm kiếm việc làm IT**\n"
    "Tìm job theo kỹ năng, vị trí, địa điểm, mức lương\n"
    "_Ví dụ: \"React developer HCM lương 20-30 triệu\", \"Senior Python remote\"_\n\n"
    "**Phân tích thị trường tuyển dụng**\n"
    "Top công ty tuyển dụng, phân bổ work mode (remote/hybrid/onsite), kỹ năng hot nhất\n"
    "_Ví dụ: \"Thị trường Java hiện tại thế nào?\", \"Công ty nào tuyển nhiều DevOps nhất?\"_\n\n"
    "**Phân tích khoảng cách kỹ năng (Skill Gap)**\n"
    "Upload CV để biết bạn đang thiếu kỹ năng gì so với thị trường, tần suất xuất hiện trong job posting\n"
    "_Nhấn nút đính kèm để upload CV (PDF/DOCX)_\n\n"
    "**Tư vấn nghề nghiệp**\n"
    "Lộ trình học, so sánh stack công nghệ, định hướng phát triển sự nghiệp IT\n"
    "_Ví dụ: \"Backend hay Frontend phù hợp cho fresher?\", \"Lộ trình để thành Data Engineer\"_\n\n"
    "---\n"
    "Hãy đặt câu hỏi hoặc upload CV để bắt đầu nhé!"
)
_GREETING_EN = (
    "Hello! I'm **IT Job Analyst** — an AI assistant for Vietnam's tech job market.\n\n"
    "Here's what I can do for you:\n\n"
    "**Job Search**\n"
    "Find IT jobs by skill, role, location, and salary range\n"
    "_e.g. \"React developer HCM 20-30M salary\", \"Senior Python remote\"_\n\n"
    "**Market Analysis**\n"
    "Top hiring companies, work-mode breakdown, hottest skills in demand\n"
    "_e.g. \"How is the Java market right now?\", \"Which companies hire the most DevOps?\"_\n\n"
    "**Skill Gap Analysis**\n"
    "Upload your CV to see which skills you're missing vs. the market\n"
    "_Click the attachment button to upload a PDF or DOCX_\n\n"
    "**Career Advice**\n"
    "Learning roadmaps, tech stack comparisons, career path guidance\n"
    "_e.g. \"Backend vs Frontend for freshers?\", \"How to become a Data Engineer?\"_\n\n"
    "---\n"
    "Ask me anything or upload your CV to get started!"
)

# out_of_scope — no LLM call, returned as-is
_OUT_OF_SCOPE_VI = (
    "Câu hỏi này nằm ngoài phạm vi hỗ trợ của tôi.\n\n"
    "Tôi chuyên về thị trường tuyển dụng CNTT:\n"
    "• Tìm kiếm việc làm IT theo kỹ năng/địa điểm/lương\n"
    "• Phân tích thị trường: top công ty, kỹ năng hot, work mode\n"
    "• Dự báo xu hướng tuyển dụng (Prophet AI)\n"
    "• Phân tích khoảng cách kỹ năng từ CV\n"
    "• Tư vấn nghề nghiệp và lộ trình học\n\n"
    "Hỏi tôi về việc làm, kỹ năng, lương, hoặc xu hướng tuyển dụng IT nhé!"
)
_OUT_OF_SCOPE_EN = (
    "That's outside my area of expertise.\n\n"
    "I specialise in Vietnam's IT job market:\n"
    "• Job search by skill / location / salary\n"
    "• Market analysis: top companies, hot skills, work modes\n"
    "• Hiring trend forecasting (Prophet AI)\n"
    "• Skill gap analysis from your CV\n"
    "• Career advice and learning roadmaps\n\n"
    "Ask me about IT jobs, skills, salaries, or hiring trends!"
)

_LEARNING_PATH_SYSTEM_PROMPT = """\
You are an IT career coach with access to real Vietnam job market data from ITviec.

LANGUAGE RULE: Respond in {lang}. No exceptions.

You have received data on the most in-demand skills for a specific role, ranked by:
- market_freq: % of job postings in the category requiring this skill
- bridge_score: % of jobs requiring BOTH the user's known skills AND this new skill (0 if no known skills)

OUTPUT FORMAT:
**Lộ trình kỹ năng dựa trên dữ liệu thực tế — [total_jobs] job [role_category]**

Numbered list (1–10). For each skill:
**[N]. [Skill Name]** ([Skill Group]) — xuất hiện trong [market_freq]% job [role]

After the list, 2–3 sentences: WHY the top skills rank high, and if bridge_score > 0,
explain how they connect to the user's existing stack.

RULES:
- Only use percentages from the data — never invent numbers.
- Do NOT start with "Based on", "Here are", "Dưới đây là", or any filler.\
"""


# ── Prompt builders ───────────────────────────────────────────────────────────

# LOCATION_ALIASES and WORK_MODE_KEYWORDS imported from constants.py


def _extract_filters(query: str) -> dict:
    """
    @brief Parse work-mode and location keywords from the raw query string.

    @param query  User's natural-language query.
    @return       Dict with keys ``work_mode`` (str | None) and ``location`` (str | None).
    """
    q = query.lower()
    work_mode = next(
        (k for k, kws in WORK_MODE_KEYWORDS.items() if any(kw in q for kw in kws)),
        None,
    )
    location = next(
        (k for k, aliases in LOCATION_ALIASES.items() if any(a in q for a in aliases)),
        None,
    )
    return {"work_mode": work_mode, "location": location}


def _post_filter(jobs: list[JobResult], filters: dict) -> list[JobResult]:
    """
    @brief Hard-filter retrieved jobs by work_mode and/or location keywords.

    @param jobs     Reranked job list from the hybrid search pipeline.
    @param filters  Dict produced by _extract_filters(); may contain None values.
    @return         Subset of jobs that satisfy every non-None filter.
    """
    if not filters["work_mode"] and not filters["location"]:
        return jobs

    result = []
    for job in jobs:
        content = job.text_content.lower()
        fields = _parse_job_fields(job.text_content)
        mode_text = fields.get("Work Mode", "").lower()
        loc_text  = fields.get("Location", "").lower()

        if filters["work_mode"]:
            kws = WORK_MODE_KEYWORDS[filters["work_mode"]]
            if not any(kw in mode_text or kw in content for kw in kws):
                continue
        if filters["location"]:
            aliases = LOCATION_ALIASES[filters["location"]]
            if not any(a in loc_text for a in aliases):
                continue
        result.append(job)
    return result


def _parse_job_fields(text_content: str) -> dict:
    """
    @brief Split a job's ``text_content`` string into a field → value dict.

    text_content is stored as "Key1: value1. Key2: value2. …" by the indexing
    pipeline; this function reverses that encoding for prompt building.

    @param text_content  The raw concatenated text from a job document.
    @return              Dict mapping field names (e.g. "Job Title") to their values.
    """
    fields = {}
    for part in text_content.split(". "):
        if ": " in part:
            key, _, value = part.partition(": ")
            fields[key.strip()] = value.strip()
    return fields


def _build_search_prompt(
    question:     str,
    jobs:         list[JobResult],
    lang:         str,
    market_block: str | None = None,
) -> str:
    """
    @brief Assemble the user-turn prompt for the search_job LLM call.

    @param question      The resolved (possibly rewritten) user query.
    @param jobs          Filtered and ranked JobResult list from hybrid search.
    @param lang          "Vietnamese" or "English" — injected into the prompt.
    @param market_block  Optional pre-formatted Gold-layer market context string.
    @return              Complete user-turn prompt string ready for the LLM.
    """
    if not jobs:
        return (
            f"[LANG={lang}] User query: {question}\n\n"
            "[NO RESULTS] The database returned 0 matching jobs. "
            "Tell the user briefly and suggest 2 alternative search angles."
        )

    job_blocks = []
    for i, job in enumerate(jobs, 1):
        f = _parse_job_fields(job.text_content)
        title    = f.get("Job Title", job.job_title)
        company  = f.get("Company", "")
        location = f.get("Location", "")
        mode     = f.get("Work Mode", "")
        skills   = f.get("Skills", "")

        block = f"JOB {i}:\n  Title: {title}\n  Company: {company}"
        if location: block += f"\n  Location: {location}"
        if mode:     block += f"\n  Work Mode: {mode}"
        if skills:   block += f"\n  Skills: {skills}"
        block += f"\n  URL: {job.job_link}"
        job_blocks.append(block)

    prompt = (
        f"[LANG={lang}] User query: {question}\n\n"
        f"=== {len(jobs)} JOBS FROM DATABASE ===\n\n"
        + "\n\n".join(job_blocks)
        + "\n\n=== END OF DATA ===\n"
        "Format each job as a card per the system prompt. "
        "Open with a 1-sentence summary. Close with 1 relevant tip."
    )

    if market_block:
        prompt += f"\n\n{market_block}"

    return prompt


def _format_rows_as_table(rows: list[dict], limit: int = 50) -> str:
    """
    @brief Render a list of SQL result rows as a plain markdown table string.

    @param rows   SQL result rows; each dict maps column name → value.
    @param limit  Maximum number of rows to render (default 20).
    @return       Markdown-style table string, with a note if rows were truncated.
    """
    if not rows:
        return ""
    sample = rows[:limit]
    headers = list(sample[0].keys())
    lines = [
        " | ".join(headers),
        " | ".join("---" for _ in headers),
        *(" | ".join(str(row.get(h, "")) for h in headers) for row in sample),
    ]
    if len(rows) > limit:
        lines.append(f"*(showing {limit} of {len(rows)} rows)*")
    return "\n".join(lines)


def _build_analytics_prompt(question: str, rows: list[dict], lang: str) -> str:
    """
    @brief Assemble the user-turn prompt for the analytics LLM call.

    @param question  The user's analytics question.
    @param rows      SQL result rows from SQLAgentService.
    @param lang      "Vietnamese" or "English".
    @return          Complete user-turn prompt string including the data table.
    """
    if not rows:
        return (
            f"[LANG={lang}] User query: {question}\n\n"
            "[NO DATA] The query returned 0 rows. "
            "Inform the user and suggest a related question they could ask."
        )

    table = _format_rows_as_table(rows)
    return (
        f"[LANG={lang}] User query: {question}\n\n"
        f"=== DATA ({len(rows)} rows) ===\n{table}\n=== END OF DATA ===\n\n"
        "Apply the chain-of-thought steps from your instructions, "
        "then write your response in the required output format."
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

class RAGPipeline:
    """
    @class RAGPipeline
    @brief Top-level orchestrator: classify → retrieve → generate.

    Architecture:
      - search_job    → HybridSearchService (Milvus ANN + BM25 + RRF + rerank)
                        → MarketContextService (Trino Gold layer, non-blocking)
                        → LLM with _SEARCH_SYSTEM_PROMPT
      - analytics     → SQLAgentService (LLM-generated Trino SQL)
                        → LLM with _ANALYTICS_SYSTEM_PROMPT (chain-of-thought)
      - career_advice → LLM only with _CAREER_ADVICE_SYSTEM_PROMPT (no retrieval)
      - out_of_scope  → canned bilingual message, zero LLM calls

    All heavy service objects are initialised once in __init__ and reused
    across requests.  The pipeline also resolves vague follow-up messages
    into standalone queries using recent conversation history.
    """

    def __init__(self) -> None:
        self._llm             = OpenAI(api_key=settings.openai_api_key)
        self._classifier      = QueryClassifier()
        self._query_processor = QueryProcessor()
        self._vector_search   = HybridSearchService()
        self._sql_agent       = SQLAgentService()
        self._market_ctx      = MarketContextService()
        self._learning_path   = LearningPathService()
        # Agentic RAG — shares all services already instantiated above
        self._agent = AgentService(
            query_processor   = self._query_processor,
            vector_search     = self._vector_search,
            sql_agent         = self._sql_agent,
            market_ctx        = self._market_ctx,
            learning_path_svc = self._learning_path,
        )

    def _generate(
        self,
        prompt: str,
        system_template: str,
        lang: str,
        history: list[dict] | None = None,
    ) -> str:
        """
        @brief Call the LLM and return the generated text response.

        Builds the message list as [system, *history, user] so multi-turn
        context is injected before the current user prompt.

        @param prompt            User-turn content (assembled by a _build_*_prompt helper).
        @param system_template   System prompt template; must contain a ``{lang}`` placeholder.
        @param lang              "Vietnamese" or "English", substituted into the template.
        @param history           Optional prior conversation turns (role/content dicts).
        @return                  The LLM's text completion.
        """
        system = system_template.format(lang=lang)
        messages: list[dict] = [{"role": "system", "content": system}]
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": prompt})

        response = self._llm.chat.completions.create(
            model=settings.openai_model,
            max_tokens=1024,
            messages=messages,
        )
        return response.choices[0].message.content

    def _extract_learning_path_intent(self, query: str) -> tuple[str, list[str]]:
        """Extract target_role and known_skills from a learning path query via LLM."""
        prompt = (
            f'Extract from this message:\n'
            f'Message: "{query}"\n'
            'Return JSON with exactly: {"target_role": "...", "known_skills": ["...", ...]}\n'
            'known_skills = skills the user already has (empty array if none mentioned).\n'
            'Return ONLY valid JSON, no explanation.'
        )
        try:
            resp = self._llm.chat.completions.create(
                model=settings.openai_model,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            data = json.loads(resp.choices[0].message.content)
            return data.get("target_role", ""), data.get("known_skills", [])
        except Exception:
            return query, []

    def _resolve_query(self, message: str, history: list[dict]) -> str:
        """
        @brief Rewrite a vague follow-up into a self-contained query using conversation context.

        Uses up to the last 4 turns of history so the LLM can resolve pronouns
        and shorthand references (e.g. "lọc thêm remote", "còn job nào khác?").
        Falls back to the original message on any LLM error.

        @param message  The raw user message, possibly a vague follow-up.
        @param history  Recent conversation turns as role/content dicts.
        @return         Standalone query string suitable for classification and retrieval.
        """
        if not history:
            return message

        # Use full history (store caps at _MAX_TURNS=10 turns / 20 messages)
        context_lines = "\n".join(
            f"{m['role'].upper()}: {m['content'][:300]}" for m in history
        )
        prompt = (
            f"You help an IT job market chatbot understand follow-up messages.\n\n"
            f"Conversation history:\n{context_lines}\n\n"
            f'New message: "{message}"\n\n'
            "Rewrite the new message as a COMPLETE standalone query that preserves the full intent.\n"
            "Rules:\n"
            "- Keep the SAME request type as the prior context "
            "(chart→chart, job search→job search, analytics→analytics)\n"
            "- Include all details: topic, time period, filters, chart/visualization if applicable\n"
            "- If prior context asked for a chart and the follow-up only changes the time period, "
            "keep 'biểu đồ' or 'chart' in the rewrite\n"
            "Examples:\n"
            '• Prior: "biểu đồ job tháng 4 2026" → New: "còn tháng 3?" '
            '→ Output: "biểu đồ số lượng job tháng 3 năm 2026"\n'
            '• Prior: "thống kê job tháng 3 2026" → New: "tháng 2 thì sao" '
            '→ Output: "thống kê biểu đồ số lượng job tháng 2 năm 2026"\n'
            '• Prior: "tìm job Python remote HCM" → New: "còn Java?" '
            '→ Output: "tìm job Java remote HCM"\n'
            '• Prior: "top 10 công ty tuyển nhiều nhất" → New: "tháng 3 thì sao?" '
            '→ Output: "top 10 công ty tuyển nhiều nhất tháng 3 năm 2026"\n'
            "Return ONLY the rewritten query — no explanation, no quotes."
        )
        try:
            resp = self._llm.chat.completions.create(
                model=settings.openai_model,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return message

    def run(self, request: ChatRequest, history: list[dict] | None = None) -> ChatResponse:
        """
        @brief Execute the full RAG pipeline for a single chat request.

        Resolves the query, classifies it, runs the appropriate retrieval branch,
        and generates the LLM response.

        @param request  Validated ChatRequest from the HTTP layer.
        @param history  Prior conversation turns for multi-turn resolution and LLM context.
        @return         ChatResponse with answer, query_type, and branch-specific fields.
        """
        history = history or []
        lang = _detect_lang(request.message)

        # Check greeting/off-topic BEFORE resolve so greetings are not rewritten as job queries
        raw = request.message.strip()
        if is_greeting(raw):
            lang = _detect_lang(raw)
            canned = _GREETING_VI if lang == "Vietnamese" else _GREETING_EN
            return ChatResponse(answer=canned, query_type=QueryType.out_of_scope)
        if is_short_off_topic(raw):
            lang = _detect_lang(raw)
            canned = _OUT_OF_SCOPE_VI if lang == "Vietnamese" else _OUT_OF_SCOPE_EN
            return ChatResponse(answer=canned, query_type=QueryType.out_of_scope)

        resolved = self._resolve_query(request.message, history) if history else request.message

        # ── Agentic path (primary) ────────────────────────────────────────
        # AgentService plans tool calls, executes in parallel, synthesizes.
        # Returns None only when planning fails → fall back to classic pipeline.
        agent_response = self._agent.run(resolved, history, lang)
        if agent_response is not None:
            return agent_response

        # ── Classic pipeline (fallback) ───────────────────────────────────
        # Reached only when: (a) agent planning LLM call fails, or
        # (b) agent returned None because LLM chose no tools (out-of-scope).
        logger.info("Agent returned None — falling back to classic pipeline.")
        query_type = self._classifier.classify(resolved)

        if query_type == QueryType.out_of_scope:
            answer = _OUT_OF_SCOPE_VI if lang == "Vietnamese" else _OUT_OF_SCOPE_EN
            return ChatResponse(answer=answer, query_type=query_type)

        if query_type == QueryType.search_job:
            try:
                expanded = self._query_processor.process(resolved)
                jobs     = self._vector_search.search_multi(expanded)
            except Exception as exc:
                logger.error("Vector search failed: %s", exc, exc_info=True)
                jobs = []
            filters        = _extract_filters(resolved)
            jobs           = _post_filter(jobs, filters)
            market_insight = self._market_ctx.get_insight(jobs, resolved, filters)
            market_block   = format_market_block(market_insight) if market_insight else None
            prompt         = _build_search_prompt(resolved, jobs, lang, market_block)
            answer         = self._generate(prompt, _SEARCH_SYSTEM_PROMPT, lang, history)
            return ChatResponse(answer=answer, query_type=query_type,
                                jobs=jobs, market_insight=market_insight)

        if query_type == QueryType.analytics:
            sql_query = sql_result = chart = None
            try:
                preferred_chart = (_extract_chart_preference(request.message)
                                   or _extract_chart_preference(resolved))
                sql_query, sql_result, chart = self._sql_agent.query(resolved, preferred_chart)
                prompt = _build_analytics_prompt(resolved, sql_result, lang)
            except Exception as exc:
                logger.error("SQL agent failed: %s", exc, exc_info=True)
                prompt = (f"[LANG={lang}] User query: {resolved}\n\n"
                          f"[ERROR] {exc}\nInform the user politely.")
            answer = self._generate(prompt, _ANALYTICS_SYSTEM_PROMPT, lang, history)
            return ChatResponse(answer=answer, query_type=query_type,
                                sql_query=sql_query, sql_result=sql_result, chart=chart)

        if query_type == QueryType.learning_path:
            target_role, known_skills = self._extract_learning_path_intent(resolved)
            lp_result = None
            if target_role:
                lp_result = self._learning_path.analyze(target_role, known_skills)
            if lp_result and lp_result.steps:
                known_str = ", ".join(lp_result.known_skills) if lp_result.known_skills else "không có"
                rows = "\n".join(
                    f"{i}. {s.skill_name} [{s.skill_group}] "
                    f"market={s.market_freq}% bridge={s.bridge_score}%"
                    for i, s in enumerate(lp_result.steps, 1)
                )
                lp_prompt = (
                    f"[LANG={lang}] User query: {resolved}\n\n"
                    f"=== LEARNING PATH DATA ===\n"
                    f"Target role: {lp_result.target_role} → category: {lp_result.role_category}\n"
                    f"Total jobs analyzed: {lp_result.total_jobs}\n"
                    f"User's known skills: {known_str}\n"
                    f"Top skills to learn:\n{rows}\n"
                    f"=== END DATA ===\n\n"
                    "Present as a numbered learning roadmap. Explain why the top skills matter."
                )
                answer = self._generate(lp_prompt, _LEARNING_PATH_SYSTEM_PROMPT, lang, history)
                return ChatResponse(answer=answer, query_type=query_type, learning_path=lp_result)
            # Trino unavailable or no data — degrade gracefully to career_advice
            prompt = f"[LANG={lang}] {resolved}"
            answer = self._generate(prompt, _CAREER_ADVICE_SYSTEM_PROMPT, lang, history)
            return ChatResponse(answer=answer, query_type=QueryType.career_advice)

        # career_advice
        prompt = f"[LANG={lang}] {request.message}"
        answer = self._generate(prompt, _CAREER_ADVICE_SYSTEM_PROMPT, lang, history)
        return ChatResponse(answer=answer, query_type=query_type)
