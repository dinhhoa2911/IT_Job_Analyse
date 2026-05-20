"""
@file query_classifier.py
@brief LLM-based query classifier với multi-layer fast-path detection.

Priority order:
  1. Greeting / casual / off-topic fast-path → out_of_scope  (no LLM)
  2. Chart / visualization fast-path         → analytics     (no LLM)
  3. Short message without IT keywords       → out_of_scope  (no LLM)
  4. LLM call with enriched Vietnamese prompt
  5. Fallback to career_advice on error
"""

import logging
import re

from openai import OpenAI

from config import settings
from models.schemas import QueryType

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1. Greeting / casual fast-path
#    Lời chào, filler words, cảm ơn, tạm biệt, nonsense → out_of_scope ngay.
#    Phải check TRƯỚC resolve_query để tránh bị rewrite thành job query.
# ─────────────────────────────────────────────────────────────────────────────
_GREETING_RE = re.compile(
    r"^\s*("

    # ── Greetings (EN) ──
    r"hi+|hello+|hey+|howdy|sup|yo+|hiya|heya|helo+"

    # ── Greetings (VI) ──
    r"|xin chào|chào mừng|chào bạn|chào anh|chào chị|chào em|chào thầy|chào cô"
    r"|chào buổi sáng|chào buổi chiều|chào buổi tối|chào ngày mới"
    r"|chào|ê bạn|ê mày|ê|này bạn|này mày|nè"
    r"|alo+|a lô|alô"

    # ── Good morning / evening ──
    r"|good morning|good afternoon|good evening|good night|good day"

    # ── Thanks (EN + VI) ──
    r"|thank you|thanks+|thank u|ty|thx|thk|tks|cảm ơn bạn nhiều|cảm ơn nhiều"
    r"|cảm ơn bạn|cảm ơn|cám ơn|cảm ơn nhé|cảm ơn nha|cảm ơn mày"
    r"|không có gì|không có chi|không dám|không sao"

    # ── Affirmations / fillers ──
    r"|vâng|dạ|dạ vâng|dạ ok|yes|no|nope|yep|yeah|yup|nah"
    r"|ok+|oke+|okay+|okie+|okey+|k\b|okk+|oke bạn"
    r"|ừ+|ừa+|uh+|uhm+|hmm+|hm+|à+|ờ+|ô+|ổn|được|vậy à|vậy hả|ừ thì"
    r"|rồi|xong|thôi|được rồi|hiểu rồi|hiểu|oke hiểu"

    # ── Goodbye (EN + VI) ──
    r"|bye+|goodbye+|see you|see ya|later|cya"
    r"|tạm biệt|bái bai|bái|chào nhé|chào tạm biệt|hẹn gặp lại|hẹn gặp|đi nhé"

    # ── Laugh / reaction ──
    r"|haha+|hihi+|hehe+|hoho+|lol|lmao|rofl|😂|🤣|😊|😄|👍|👌|❤️|🙏"

    # ── Nonsense / test ──
    r"|test+|testing+|demo|ping|pong|check|thử|thử thôi|thử tí|thử xem"
    r"|123+|1234+|aaa+|bbb+|ccc+|zzz+|xyz|abc+|\.\.\.|…"

    # ── Filler questions ──
    r"|còn đó không|bạn có đó không|có ai đó không|đang online không"

    r")\s*[!?.,:;🙂😊👋🤗]*\s*$",
    re.IGNORECASE | re.UNICODE,
)

# ─────────────────────────────────────────────────────────────────────────────
# 2. IT keyword set — dùng để check short message có liên quan IT không
# ─────────────────────────────────────────────────────────────────────────────
_IT_KEYWORD_RE = re.compile(
    r"\b(job|việc|tuyển|lương|salary|wage|skill|kỹ năng|công ty|company|employer"
    r"|developer|engineer|programmer|coder|devops|fullstack|frontend|backend"
    r"|python|java|javascript|react|node|angular|vue|kotlin|swift|golang|rust|php|ruby"
    r"|sql|mysql|postgresql|mongodb|redis|kafka|docker|kubernetes|aws|azure|gcp|cloud"
    r"|data|ai|ml|machine learning|deep learning|nlp|llm|analyst|scientist"
    r"|intern|fresher|junior|senior|lead|manager|architect|cto|ceo"
    r"|remote|hybrid|onsite|hcm|hanoi|đà nẵng|da nang|hà nội|ho chi minh"
    r"|cv|resume|portfolio|interview|phỏng vấn|thi tuyển|ứng tuyển|apply"
    r"|thống kê|phân tích|xu hướng|thị trường|tuyển dụng|tìm việc|xin việc"
    r"|biểu đồ|chart|graph|trend|analytics|statistics)\b",
    re.IGNORECASE | re.UNICODE,
)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Forecast / prediction fast-path   (checked BEFORE chart fast-path)
#    "dự báo Python 3 tháng tới", "xu hướng Data Engineer năm sau",
#    "forecast backend jobs next quarter", "sẽ tăng hay giảm không?"
# ─────────────────────────────────────────────────────────────────────────────
_FORECAST_TIME = (
    r"(dự báo|dự đoán|forecast|predict|xu hướng.*tháng|tháng tới|năm tới|"
    r"tương lai|sắp tới|sẽ như thế nào|sẽ tăng|sẽ giảm|"
    r"trong \d+\s*tháng|next \d+\s*months?|next quarter|"
    r"trend|outlook|projection)"
)
_FORECAST_DOMAIN = (
    r"(job|việc|tuyển|developer|engineer|data|backend|frontend|python|java|"
    r"react|devops|mobile|fullstack|software|lập trình|kỹ sư|thị trường)"
)
_FORECAST_RE = re.compile(
    rf"(?:{_FORECAST_TIME}[\s\S]{{0,80}}{_FORECAST_DOMAIN})"
    rf"|(?:{_FORECAST_DOMAIN}[\s\S]{{0,80}}{_FORECAST_TIME})",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Chart / visualization fast-path
# ─────────────────────────────────────────────────────────────────────────────
_CHART_TRIGGER = r"(vẽ|show|draw|plot|hiển thị|cho.*xem|display)"
_CHART_NOUN    = r"(biểu đồ|chart|graph|histogram|pie|bar chart|line chart)"
_IT_DOMAIN     = r"(job|việc|tuyển|lương|skill|kỹ năng|công ty|thị trường|số lượng|thống kê|developer|engineer|salary)"

_CHART_RE = re.compile(
    rf"(?:{_CHART_TRIGGER}[\s\S]{{0,30}}{_CHART_NOUN})"
    rf"|(?:{_CHART_NOUN}[\s\S]{{0,60}}{_IT_DOMAIN})",
    re.IGNORECASE,
)


def is_greeting(text: str) -> bool:
    """Public helper — dùng trong rag_pipeline để check TRƯỚC resolve_query."""
    return bool(_GREETING_RE.match(text.strip()))


def is_short_off_topic(text: str) -> bool:
    """Message ngắn (< 8 từ) mà không có từ khóa IT → out_of_scope."""
    words = text.strip().split()
    if len(words) >= 8:
        return False
    return not bool(_IT_KEYWORD_RE.search(text))


_SYSTEM_PROMPT = """Bạn là bộ phân loại câu hỏi cho hệ thống chatbot tuyển dụng CNTT.

Phân loại câu hỏi vào ĐÚNG MỘT trong 5 categories:

search_job
  Người dùng muốn TÌM KIẾM hoặc XEM job posting cụ thể.
  Ví dụ: "tìm job React developer", "việc làm Python remote lương cao",
          "có job backend nào ở Hà Nội không", "jobs at Shopee",
          "tìm vị trí Data Engineer ở HCM"

analytics
  Người dùng muốn THỐNG KÊ, PHÂN TÍCH DỮ LIỆU HIỆN TẠI, hoặc VẼ BIỂU ĐỒ.
  Dữ liệu là THỰC TẾ ĐÃ CÓ, không phải dự báo tương lai.
  Ví dụ: "skill nào hot nhất hiện tại", "lương trung bình Data Engineer",
          "công ty nào tuyển nhiều nhất", "top ngôn ngữ lập trình được tuyển",
          "vẽ biểu đồ số lượng job theo tháng", "tỷ lệ remote vs onsite hiện tại",
          "top 10 công ty tuyển dụng nhiều nhất", "có bao nhiêu job Python tháng 3"

forecast
  Người dùng muốn DỰ BÁO, DỰ ĐOÁN, hoặc XEM XU HƯỚNG TƯƠNG LAI của thị trường IT.
  Từ khóa quan trọng: "dự báo", "dự đoán", "forecast", "tháng tới", "năm tới",
                       "sẽ tăng", "sẽ giảm", "trong X tháng tới", "tương lai", "xu hướng tới".
  Ví dụ: "dự báo số lượng job Python 3 tháng tới",
          "xu hướng tuyển dụng Backend năm sau như thế nào?",
          "forecast Data Engineer jobs next quarter",
          "DevOps sẽ tăng hay giảm trong 3 tháng tới?",
          "dự đoán thị trường Frontend năm 2026",
          "số lượng job Data sẽ thay đổi thế nào?",
          "trend tuyển dụng AI trong tương lai"

career_advice
  Người dùng hỏi về ĐỊNH HƯỚNG NGHỀ NGHIỆP, LỘ TRÌNH HỌC, hoặc câu hỏi
  liên quan đến ngành CNTT/tuyển dụng nhưng không cần tìm job hay thống kê.
  Ví dụ: "nên học gì để làm backend", "roadmap frontend 2024",
          "fresher Java cần biết những gì", "sự khác nhau giữa DevOps và SRE",
          "học Data Science mất bao lâu", "kinh nghiệm 2 năm lương bao nhiêu là ổn"

out_of_scope
  Câu hỏi KHÔNG LIÊN QUAN đến tuyển dụng CNTT, nghề nghiệp IT, hoặc thị trường việc làm.
  Bao gồm: lời chào, cảm ơn, tạm biệt, thời tiết, nấu ăn, thể thao, chính trị,
           câu hỏi ngắn vô nghĩa, tin nhắn thử nghiệm.

Chỉ trả lời ĐÚNG MỘT từ: search_job, analytics, forecast, career_advice, out_of_scope.
Không giải thích, không dấu câu."""


class QueryClassifier:
    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

    def classify(self, question: str) -> QueryType:
        stripped = question.strip()

        # ── Layer 1: Greeting / casual ────────────────────────────────────────
        if is_greeting(stripped):
            logger.info("Fast-path [greeting]: '%s' → out_of_scope", stripped[:50])
            return QueryType.out_of_scope

        # ── Layer 2: Forecast / prediction (checked BEFORE chart) ────────────
        if _FORECAST_RE.search(stripped):
            logger.info("Fast-path [forecast]: '%s' → forecast", stripped[:60])
            return QueryType.forecast

        # ── Layer 3: Chart / visualization ────────────────────────────────────
        if _CHART_RE.search(stripped):
            logger.info("Fast-path [chart]: '%s' → analytics", stripped[:60])
            return QueryType.analytics

        # ── Layer 4: Short message without IT keywords ─────────────────────────
        if is_short_off_topic(stripped):
            logger.info("Fast-path [short-off-topic]: '%s' → out_of_scope", stripped[:50])
            return QueryType.out_of_scope

        # ── Layer 5: LLM classifier ────────────────────────────────────────────
        try:
            response = self._client.chat.completions.create(
                model=settings.openai_model,
                max_tokens=16,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": stripped},
                ],
            )
            label = response.choices[0].message.content.strip().lower()
        except Exception as exc:
            logger.error("Classifier API error: %s — fallback to career_advice", exc)
            return QueryType.career_advice

        try:
            query_type = QueryType(label)
        except ValueError:
            logger.warning("Unknown label '%s' for '%s' — fallback to career_advice", label, stripped[:50])
            query_type = QueryType.career_advice

        logger.info("LLM classified '%s' → %s", stripped[:60], query_type.value)
        return query_type
