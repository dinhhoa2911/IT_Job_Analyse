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

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

# Used for search_job and analytics paths — has access to real retrieved data
_SYSTEM_PROMPT = """Bạn là IT Job Assistant — trợ lý AI thông minh chuyên về thị trường \
tuyển dụng CNTT Việt Nam, sử dụng dữ liệu thực tế từ ITviec.

Nguyên tắc trả lời:
- Trả lời bằng ngôn ngữ của người dùng (tiếng Việt hoặc tiếng Anh).
- Ngắn gọn, rõ ràng, có cấu trúc.
- Khi trình bày danh sách job: mỗi job trên một khối, gồm tên vị trí, công ty, \
địa điểm/skill (lấy từ text_content), và link.
- Khi trình bày thống kê: dùng số liệu cụ thể, đưa ra nhận xét ngắn.
- Không bịa đặt thông tin ngoài dữ liệu được cung cấp."""

# Used for career_advice — no retrieved data, Claude answers from its knowledge
# but strictly within the IT job market domain
_CAREER_ADVICE_SYSTEM_PROMPT = """Bạn là IT Job Assistant — chuyên gia tư vấn nghề nghiệp \
trong lĩnh vực CNTT tại Việt Nam.

Phạm vi trả lời của bạn CHỈ BAO GỒM:
- Lộ trình học tập và phát triển kỹ năng IT (roadmap, tech stack)
- Định hướng nghề nghiệp trong ngành CNTT
- Giải thích các vị trí công việc IT (backend, frontend, DevOps, Data...)
- Tư vấn về mức lương, kinh nghiệm, kỹ năng cần thiết
- Xu hướng công nghệ liên quan đến tuyển dụng

Trả lời bằng ngôn ngữ của người dùng. Ngắn gọn, thực tế, dựa trên thực tế \
thị trường IT Việt Nam."""

# Fixed message — returned WITHOUT any LLM call to save cost and latency
_OUT_OF_SCOPE_MESSAGE = (
    "Xin lỗi, tôi chỉ có thể hỗ trợ các câu hỏi liên quan đến:\n"
    "• Tìm kiếm việc làm CNTT\n"
    "• Phân tích thị trường tuyển dụng IT\n"
    "• Tư vấn nghề nghiệp trong ngành CNTT\n\n"
    "Bạn có thể hỏi tôi về việc làm, kỹ năng, lương, hoặc xu hướng tuyển dụng IT nhé!"
)

# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_search_prompt(question: str, jobs: list[JobResult]) -> str:
    if not jobs:
        return (
            f"Câu hỏi: {question}\n\n"
            "[Không tìm thấy job phù hợp trong database.]\n\n"
            "Hãy thông báo lịch sự và gợi ý người dùng thử tìm với từ khóa khác."
        )

    job_blocks = []
    for i, job in enumerate(jobs, 1):
        job_blocks.append(
            f"{i}. {job.text_content}\n"
            f"   Link: {job.job_link}"
        )

    return (
        f"Câu hỏi của người dùng: {question}\n\n"
        f"Kết quả tìm kiếm từ database ({len(jobs)} jobs):\n\n"
        + "\n\n".join(job_blocks)
        + "\n\nHãy trình bày danh sách job trên một cách rõ ràng, có cấu trúc cho người dùng."
    )


def _build_analytics_prompt(question: str, sql: str, rows: list[dict]) -> str:
    if not rows:
        return (
            f"Câu hỏi: {question}\n\nSQL đã chạy:\n{sql}\n\n"
            "[Truy vấn trả về 0 kết quả.]\n\n"
            "Hãy thông báo lịch sự rằng không có dữ liệu và gợi ý câu hỏi khác."
        )

    rows_text = "\n".join(str(row) for row in rows[:15])
    return (
        f"Câu hỏi: {question}\n\n"
        f"SQL đã chạy:\n{sql}\n\n"
        f"Kết quả ({len(rows)} dòng):\n{rows_text}\n\n"
        "Hãy phân tích và trả lời câu hỏi dựa trên dữ liệu trên. "
        "Đưa ra nhận xét ngắn nếu có insight thú vị."
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class RAGPipeline:
    """
    Orchestrates the full RAG flow:

        classify → retrieve (Milvus or Trino) → generate (Claude)

    QueryType routing:
        search_job    → HybridSearchService  → Claude (_SYSTEM_PROMPT)
        analytics     → SQLAgentService      → Claude (_SYSTEM_PROMPT)
        career_advice → (no retrieval)       → Claude (_CAREER_ADVICE_SYSTEM_PROMPT)
        out_of_scope  → (no retrieval)       → fixed decline message (no LLM call)
    """

    def __init__(self) -> None:
        self._llm             = OpenAI(api_key=settings.openai_api_key)
        self._classifier      = QueryClassifier()
        self._query_processor = QueryProcessor()
        self._vector_search   = HybridSearchService()
        self._sql_agent       = SQLAgentService()

    def _generate(self, prompt: str, system: str) -> str:
        response = self._llm.chat.completions.create(
            model=settings.openai_model,
            max_tokens=1024,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content

    def run(self, request: ChatRequest) -> ChatResponse:
        """
        Process *request* and return a structured ChatResponse.

        out_of_scope queries are short-circuited before any LLM call.
        All other errors are caught and surfaced as graceful messages.
        """
        query_type = self._classifier.classify(request.message)

        # ── out_of_scope: no LLM call, fixed message ────────────────────
        if query_type == QueryType.out_of_scope:
            logger.info("Out-of-scope query rejected: '%s...'", request.message[:50])
            return ChatResponse(
                answer=_OUT_OF_SCOPE_MESSAGE,
                query_type=query_type,
            )

        # ── search_job: query expansion → hybrid retrieval → Claude ────
        if query_type == QueryType.search_job:
            try:
                expanded_queries = self._query_processor.process(request.message)
                jobs = self._vector_search.search_multi(expanded_queries)
            except Exception as exc:
                logger.error("Vector search failed: %s", exc, exc_info=True)
                jobs = []
            prompt = _build_search_prompt(request.message, jobs)
            answer = self._generate(prompt, _SYSTEM_PROMPT)
            return ChatResponse(answer=answer, query_type=query_type, jobs=jobs)

        # ── analytics: SQL agent → Claude ───────────────────────────────
        if query_type == QueryType.analytics:
            sql_query: str | None = None
            sql_result: list[dict] | None = None
            try:
                sql_query, sql_result = self._sql_agent.query(request.message)
                prompt = _build_analytics_prompt(request.message, sql_query, sql_result)
            except Exception as exc:
                logger.error("SQL agent failed: %s", exc, exc_info=True)
                prompt = (
                    f"Câu hỏi: {request.message}\n\n"
                    f"[Lỗi khi truy vấn database: {exc}]\n\n"
                    "Hãy thông báo lịch sự rằng có lỗi xảy ra và gợi ý người dùng thử lại."
                )
            answer = self._generate(prompt, _SYSTEM_PROMPT)
            return ChatResponse(
                answer=answer,
                query_type=query_type,
                sql_query=sql_query,
                sql_result=sql_result,
            )

        # ── career_advice: Claude only, domain-restricted ───────────────
        answer = self._generate(request.message, _CAREER_ADVICE_SYSTEM_PROMPT)
        return ChatResponse(answer=answer, query_type=query_type)
