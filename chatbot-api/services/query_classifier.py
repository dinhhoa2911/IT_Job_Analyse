"""
@file query_classifier.py
@brief LLM-based query classifier that routes user messages to the correct pipeline branch.

Makes a single low-latency LLM call (max_tokens=16) to classify each incoming
message into one of four QueryType categories: search_job, analytics,
career_advice, or out_of_scope.  Falls back to career_advice on any API error
or unrecognised label to avoid silently dropping valid questions.
"""

import logging

from openai import OpenAI

from config import settings
from models.schemas import QueryType

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """Bạn là bộ phân loại câu hỏi cho hệ thống chatbot tuyển dụng CNTT.

Phân loại câu hỏi vào ĐÚNG MỘT trong 4 categories:

search_job
  Người dùng muốn TÌM KIẾM hoặc XEM job posting cụ thể.
  Ví dụ: "tìm job React developer", "việc làm Python remote lương cao",
          "có job backend nào ở Hà Nội không", "jobs at Shopee",
          "tìm vị trí Data Engineer ở HCM"

analytics
  Người dùng muốn THỐNG KÊ, XU HƯỚNG, hoặc PHÂN TÍCH thị trường tuyển dụng.
  Ví dụ: "skill nào hot nhất hiện tại", "lương trung bình Data Engineer",
          "công ty nào tuyển nhiều nhất", "xu hướng tuyển dụng AI 2024",
          "bao nhiêu job remote hiện tại", "top ngôn ngữ lập trình được tuyển"

career_advice
  Người dùng hỏi về ĐỊNH HƯỚNG NGHỀ NGHIỆP, LỘ TRÌNH HỌC, hoặc câu hỏi
  liên quan đến ngành CNTT/tuyển dụng nhưng không cần tìm job hay thống kê.
  Ví dụ: "nên học gì để làm backend", "roadmap frontend 2024",
          "fresher Java cần biết những gì", "sự khác nhau giữa DevOps và SRE",
          "học Data Science mất bao lâu", "kinh nghiệm 2 năm lương bao nhiêu là ổn"

out_of_scope
  Câu hỏi KHÔNG LIÊN QUAN đến tuyển dụng CNTT, nghề nghiệp IT, hoặc thị trường việc làm.
  Ví dụ: thời tiết, nấu ăn, thể thao, chính trị, viết thơ, dịch thuật,
          toán học thuần túy, lịch sử, câu hỏi cá nhân không liên quan IT.

Chỉ trả lời ĐÚNG MỘT từ: search_job, analytics, career_advice, out_of_scope.
Không giải thích, không dấu câu."""


class QueryClassifier:
    """
    @class QueryClassifier
    @brief Single-call LLM classifier with safe fallback to career_advice.

    Sends the user's message to the LLM with a Vietnamese system prompt that
    defines the four classification categories.  The LLM must return exactly
    one label word; any unexpected output or API error falls back to
    career_advice (safer than out_of_scope, which suppresses all retrieval).
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

    def classify(self, question: str) -> QueryType:
        """
        @brief Classify a user question into one of the four QueryType branches.

        Fallback logic:
          - Unknown label → career_advice  (safer than out_of_scope)
          - API error     → career_advice  (never silently drop a valid question)

        @param question  Resolved (possibly rewritten) user query string.
        @return          QueryType enum value for the detected intent.
        """
        try:
            response = self._client.chat.completions.create(
                model=settings.openai_model,
                max_tokens=16,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": question},
                ],
            )
            label = response.choices[0].message.content.strip().lower()
        except Exception as exc:
            logger.error("Classifier API error: %s — defaulting to career_advice", exc)
            return QueryType.career_advice

        try:
            query_type = QueryType(label)
        except ValueError:
            logger.warning(
                "Unknown label '%s' for question '%s...' — defaulting to career_advice",
                label, question[:50],
            )
            query_type = QueryType.career_advice

        logger.info("Classified '%s...' → %s", question[:50], query_type.value)
        return query_type
