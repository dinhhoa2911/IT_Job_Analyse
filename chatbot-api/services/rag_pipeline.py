"""RAG pipeline: classify → retrieve → generate."""

import logging

from openai import OpenAI

from config import settings
from models.schemas import ChatRequest, ChatResponse, JobResult, QueryType
from services.query_classifier import QueryClassifier
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

# out_of_scope — no LLM call, returned as-is
_OUT_OF_SCOPE_VI = (
    "Tôi chỉ hỗ trợ các câu hỏi về:\n"
    "• Tìm kiếm việc làm IT\n"
    "• Phân tích thị trường tuyển dụng CNTT\n"
    "• Tư vấn nghề nghiệp trong ngành IT\n\n"
    "Hỏi tôi về việc làm, kỹ năng, lương, hoặc xu hướng tuyển dụng IT nhé!"
)
_OUT_OF_SCOPE_EN = (
    "I can only help with:\n"
    "• IT job search\n"
    "• Tech job market analysis\n"
    "• IT career advice\n\n"
    "Ask me about jobs, skills, salaries, or hiring trends in Vietnam's tech industry."
)


# ── Prompt builders ───────────────────────────────────────────────────────────

_WORK_MODE_KEYWORDS: dict[str, list[str]] = {
    "remote":  ["remote", "từ xa", "làm tại nhà", "work from home", "wfh"],
    "hybrid":  ["hybrid", "kết hợp"],
    "onsite":  ["onsite", "tại văn phòng", "on-site", "office"],
}

_LOCATION_ALIASES: dict[str, list[str]] = {
    "hcm":     ["hồ chí minh", "ho chi minh", "hcm", "sài gòn", "saigon", "tp.hcm"],
    "hanoi":   ["hà nội", "ha noi", "hanoi"],
    "danang":  ["đà nẵng", "da nang"],
}


def _extract_filters(query: str) -> dict:
    """Return {work_mode: str | None, location: str | None} from query text."""
    q = query.lower()
    work_mode = next(
        (k for k, kws in _WORK_MODE_KEYWORDS.items() if any(kw in q for kw in kws)),
        None,
    )
    location = next(
        (k for k, aliases in _LOCATION_ALIASES.items() if any(a in q for a in aliases)),
        None,
    )
    return {"work_mode": work_mode, "location": location}


def _post_filter(jobs: list[JobResult], filters: dict) -> list[JobResult]:
    """Hard-filter retrieved jobs by work_mode and/or location."""
    if not filters["work_mode"] and not filters["location"]:
        return jobs

    result = []
    for job in jobs:
        content = job.text_content.lower()
        fields = _parse_job_fields(job.text_content)
        mode_text = fields.get("Work Mode", "").lower()
        loc_text  = fields.get("Location", "").lower()

        if filters["work_mode"]:
            kws = _WORK_MODE_KEYWORDS[filters["work_mode"]]
            if not any(kw in mode_text or kw in content for kw in kws):
                continue
        if filters["location"]:
            aliases = _LOCATION_ALIASES[filters["location"]]
            if not any(a in loc_text for a in aliases):
                continue
        result.append(job)
    return result


def _parse_job_fields(text_content: str) -> dict:
    """Extract structured fields from the concatenated text_content string."""
    fields = {}
    for part in text_content.split(". "):
        if ": " in part:
            key, _, value = part.partition(": ")
            fields[key.strip()] = value.strip()
    return fields


def _build_search_prompt(question: str, jobs: list[JobResult], lang: str) -> str:
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

    return (
        f"[LANG={lang}] User query: {question}\n\n"
        f"=== {len(jobs)} JOBS FROM DATABASE ===\n\n"
        + "\n\n".join(job_blocks)
        + "\n\n=== END OF DATA ===\n"
        "Format each job as a card per the system prompt. "
        "Open with a 1-sentence summary. Close with 1 relevant tip."
    )


def _format_rows_as_table(rows: list[dict], limit: int = 20) -> str:
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
    classify → retrieve (Milvus or Trino) → generate

    search_job    → HybridSearchService → LLM (_SEARCH_SYSTEM_PROMPT)
    analytics     → SQLAgentService     → LLM (_ANALYTICS_SYSTEM_PROMPT, CoT)
    career_advice → (no retrieval)      → LLM (_CAREER_ADVICE_SYSTEM_PROMPT)
    out_of_scope  → fixed message (no LLM call)
    """

    def __init__(self) -> None:
        self._llm             = OpenAI(api_key=settings.openai_api_key)
        self._classifier      = QueryClassifier()
        self._query_processor = QueryProcessor()
        self._vector_search   = HybridSearchService()
        self._sql_agent       = SQLAgentService()

    def _generate(
        self,
        prompt: str,
        system_template: str,
        lang: str,
        history: list[dict] | None = None,
    ) -> str:
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

    def _resolve_query(self, message: str, history: list[dict]) -> str:
        """Rewrite a vague follow-up into a standalone query using recent context."""
        if not history:
            return message

        recent = history[-4:]
        context_lines = "\n".join(
            f"{m['role'].upper()}: {m['content'][:300]}" for m in recent
        )
        prompt = (
            f"Conversation history:\n{context_lines}\n\n"
            f'Current message: "{message}"\n\n'
            "If the current message is a vague follow-up that references prior context "
            "(pronouns, shorthand like 'lọc thêm', 'còn', 'remote', 'higher salary', etc.), "
            "rewrite it as a complete standalone query in the same language. "
            "If it is already self-contained, return it unchanged. "
            "Return ONLY the rewritten query — no explanation."
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
        history = history or []
        lang = _detect_lang(request.message)

        resolved = self._resolve_query(request.message, history) if history else request.message
        query_type = self._classifier.classify(resolved)

        # ── out_of_scope ─────────────────────────────────────────────────
        if query_type == QueryType.out_of_scope:
            logger.info("Out-of-scope: '%s...'", request.message[:50])
            answer = _OUT_OF_SCOPE_VI if lang == "Vietnamese" else _OUT_OF_SCOPE_EN
            return ChatResponse(answer=answer, query_type=query_type)

        # ── search_job ───────────────────────────────────────────────────
        if query_type == QueryType.search_job:
            try:
                expanded = self._query_processor.process(resolved)
                jobs = self._vector_search.search_multi(expanded)
            except Exception as exc:
                logger.error("Vector search failed: %s", exc, exc_info=True)
                jobs = []

            filters = _extract_filters(resolved)
            jobs = _post_filter(jobs, filters)
            logger.info("Post-filter %s → %d jobs remain", filters, len(jobs))

            prompt = _build_search_prompt(resolved, jobs, lang)
            answer = self._generate(prompt, _SEARCH_SYSTEM_PROMPT, lang, history)
            return ChatResponse(answer=answer, query_type=query_type, jobs=jobs)

        # ── analytics ────────────────────────────────────────────────────
        if query_type == QueryType.analytics:
            sql_query: str | None = None
            sql_result: list[dict] | None = None
            chart: dict | None = None
            try:
                sql_query, sql_result, chart = self._sql_agent.query(resolved)
                prompt = _build_analytics_prompt(resolved, sql_result, lang)
            except Exception as exc:
                logger.error("SQL agent failed: %s", exc, exc_info=True)
                prompt = (
                    f"[LANG={lang}] User query: {resolved}\n\n"
                    f"[ERROR] Database query failed: {exc}\n"
                    "Inform the user politely and suggest they try again."
                )
            answer = self._generate(prompt, _ANALYTICS_SYSTEM_PROMPT, lang, history)
            return ChatResponse(
                answer=answer,
                query_type=query_type,
                sql_query=sql_query,
                sql_result=sql_result,
                chart=chart,
            )

        # ── career_advice ────────────────────────────────────────────────
        prompt = f"[LANG={lang}] {request.message}"
        answer = self._generate(prompt, _CAREER_ADVICE_SYSTEM_PROMPT, lang, history)
        return ChatResponse(answer=answer, query_type=query_type)
