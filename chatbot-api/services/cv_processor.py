"""
@file cv_processor.py
@brief CV Processor — PDF text extraction and LLM-powered structured summarisation.

Pipeline:
  1. PDF bytes  →  raw text        (pdfplumber, page-by-page)
  2. Raw text   →  clean text      (whitespace and noise removal)
  3. Clean text →  CVProfile JSON  (OpenAI structured extraction, temperature=0.1)
  4. CVProfile  →  search_queries  (3-4 variants for multi-query hybrid search)

Design notes:
  - pdfplumber handles column-heavy CV layouts far better than pypdf.
  - LLM temperature=0.1 produces near-deterministic structured output.
  - queries[0] = summary_text; used as the cross-encoder anchor in search_multi.
  - CV text is truncated to 4 000 chars (~1 k tokens) before the LLM call to
    keep latency predictable within gpt-4o-mini's context window.
"""

import io
import json
import logging
import re
from typing import Optional

import pdfplumber
from openai import OpenAI

from config import settings
from models.schemas import CVProfile

logger = logging.getLogger(__name__)

# ── LLM prompt ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are an expert technical recruiter. Extract structured information from the CV/resume text below.
Return ONLY a valid JSON object — no markdown fences, no explanation.

Required JSON schema:
{
  "name": "Full Name or null",
  "skills": ["Skill1", "Skill2"],
  "experience_years": 2,
  "level": "intern|fresher|junior|mid|senior|lead",
  "preferred_roles": ["Backend Developer", "Data Engineer"],
  "preferred_locations": ["HCM", "Hanoi", "Remote"],
  "work_mode": "remote|hybrid|onsite|null",
  "education": "Degree and university or null",
  "summary_text": "2-4 sentence English description of what jobs this person should see."
}

EXTRACTION RULES:
- skills: technical only (languages, frameworks, databases, tools, cloud). Max 20 items.
- experience_years: total professional years as integer. 0 for fresh graduates / students.
- level: infer from years + job titles  (student/0 yr → intern or fresher, <1 yr → junior, 1-3 → junior, 3-6 → mid, 6-10 → senior, 10+ → lead).
- preferred_roles: job title categories in English. Infer from past titles and skills. Max 5.
- preferred_locations: only if explicitly stated in the CV; otherwise [].
- summary_text: MUST mention top 5 skills, level, years of experience, and target role type.
  Write as if addressing a job recommender engine. Example:
  "Mid-level Backend Developer with 3 years of experience in Python, FastAPI, PostgreSQL, Docker, and Redis.
   Looking for backend or data engineering roles. Strong in API design and microservices."
"""

# ── Text helpers ───────────────────────────────────────────────────────────────

_MULTI_BLANK = re.compile(r"\n{3,}")
_MULTI_SPACE = re.compile(r"[ \t]{2,}")
_PAGE_NUM    = re.compile(r"^\s*\d{1,3}\s*$", re.MULTILINE)


def _clean(raw: str) -> str:
    """
    @brief Normalise raw PDF text by collapsing excess whitespace and removing noise.

    @param raw  Raw text extracted page-by-page from pdfplumber.
    @return     Cleaned, stripped text ready for LLM ingestion.
    """
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    text = _PAGE_NUM.sub("", text)          # remove lone page numbers
    text = _MULTI_BLANK.sub("\n\n", text)   # collapse blank lines
    text = _MULTI_SPACE.sub(" ", text)      # collapse inline whitespace
    return text.strip()


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """
    @brief Extract plain text from a PDF using pdfplumber.

    @param pdf_bytes  Raw PDF file contents as bytes.
    @return           Concatenated text from all pages.
    @throws ValueError  If the PDF has no pages, exceeds 20 pages, or contains no readable text.
    """
    parts: list[str] = []

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if len(pdf.pages) == 0:
                raise ValueError("PDF has no pages.")
            if len(pdf.pages) > 20:
                raise ValueError(
                    f"PDF has {len(pdf.pages)} pages. Please upload a CV (max 20 pages)."
                )
            for i, page in enumerate(pdf.pages, 1):
                text = page.extract_text(x_tolerance=3, y_tolerance=3)
                if text:
                    parts.append(text)
                    logger.debug("Page %d: %d chars", i, len(text))
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Could not open PDF: {exc}") from exc

    if not parts:
        raise ValueError(
            "No readable text found in this PDF. "
            "The file may be a scanned image — please use a text-based PDF."
        )

    raw = "\n\n".join(parts)
    logger.info("PDF extracted: %d chars, %d page(s).", len(raw), len(parts))
    return raw


# ── Query builder ──────────────────────────────────────────────────────────────

def _build_search_queries(profile: CVProfile) -> list[str]:
    """
    @brief Generate 3-4 diverse query variants from a CVProfile for multi-query hybrid search.

    Query ordering is intentional — queries[0] is used as the cross-encoder
    anchor in HybridSearchService.search_multi(), so it must be the richest
    semantic representation:
      q0: summary_text        — dense recall + reranker anchor
      q1: role + skills + loc — BM25 keyword recall
      q2: level + role        — structured retrieval
      q3: alt role + skills   — widens recall for multi-title candidates (if applicable)

    @param profile  Structured CVProfile produced by CVProcessor._summarize().
    @return         Deduplicated list of query strings with the original query at index 0.
    """
    top_skills = profile.skills[:5]
    roles      = profile.preferred_roles or ["Software Engineer"]
    level      = profile.level or "mid"
    locs       = profile.preferred_locations[:1]

    queries: list[str] = []

    # q0 — semantic anchor
    if profile.summary_text:
        queries.append(profile.summary_text[:300])

    # q1 — keyword-heavy
    q1 = " ".join([roles[0]] + top_skills[:4] + locs)
    queries.append(q1)

    # q2 — level/role focus
    q2_parts = [f"{level} {roles[0]}"]
    if len(top_skills) > 3:
        q2_parts.append(" ".join(top_skills[3:5]))
    queries.append(" ".join(q2_parts))

    # q3 — alternative role (only if candidate lists more than one)
    if len(roles) > 1:
        q3 = " ".join([roles[1]] + top_skills[:3])
        queries.append(q3)

    # Deduplicate (preserving order)
    seen: set[str] = set()
    unique: list[str] = []
    for q in queries:
        q = q.strip()
        if q and q not in seen:
            seen.add(q)
            unique.append(q)

    return unique


# ── Main service ───────────────────────────────────────────────────────────────

class CVProcessor:
    """
    @class CVProcessor
    @brief Extract text from a CV PDF and summarise it into a structured CVProfile.

    Encapsulates the full four-step pipeline (extract → clean → LLM summarise →
    build search queries) as a single process() call.  The resulting CVProfile
    is consumed directly by CVMatcherService.match().
    """

    def __init__(self) -> None:
        self._llm = OpenAI(api_key=settings.openai_api_key)

    def process(self, pdf_bytes: bytes) -> CVProfile:
        """
        @brief Run the full pipeline: raw PDF bytes → CVProfile with search queries.

        @param pdf_bytes  Raw bytes of the uploaded PDF file.
        @return           Populated CVProfile including generated search_queries.
        @throws ValueError   For recoverable user errors (bad PDF, image-only, too many pages).
        @throws RuntimeError For unexpected LLM API failures.
        """
        # 1. Extract
        raw = _extract_pdf_text(pdf_bytes)

        # 2. Clean
        clean = _clean(raw)

        # 3. Truncate to token budget (~4 k chars ≈ 1 k tokens, well within gpt-4o-mini)
        budget = 4_000
        if len(clean) > budget:
            logger.info("CV text truncated %d → %d chars.", len(clean), budget)
            clean = clean[:budget]

        # 4. LLM extraction
        profile = self._summarize(clean)

        # 5. Build search queries
        profile.search_queries = _build_search_queries(profile)

        logger.info(
            "CV processed | name=%s | skills=%d | level=%s | queries=%d",
            profile.name or "unknown",
            len(profile.skills),
            profile.level,
            len(profile.search_queries),
        )
        return profile

    def _summarize(self, text: str) -> CVProfile:
        """
        @brief Call the LLM with the extraction prompt and parse the JSON response into a CVProfile.

        @param text  Cleaned, truncated CV text to send to the LLM.
        @return      Partially populated CVProfile (search_queries not yet filled).
        @throws RuntimeError  If the LLM call fails or returns malformed JSON.
        """
        try:
            resp = self._llm.chat.completions.create(
                model=settings.openai_model,
                temperature=0.1,
                max_tokens=700,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user",   "content": f"CV TEXT:\n{text}"},
                ],
            )
        except Exception as exc:
            raise RuntimeError(f"LLM call failed: {exc}") from exc

        raw_json = resp.choices[0].message.content.strip()

        # Strip accidental markdown fences
        raw_json = re.sub(r"^```(?:json)?\n?", "", raw_json)
        raw_json = re.sub(r"\n?```$",          "", raw_json)

        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            logger.error("LLM returned invalid JSON: %s\n---\n%s", exc, raw_json[:500])
            raise RuntimeError("CV analysis returned malformed data. Please try again.") from exc

        return CVProfile(
            name                = data.get("name"),
            skills              = (data.get("skills") or [])[:20],
            experience_years    = _safe_int(data.get("experience_years")),
            level               = data.get("level"),
            preferred_roles     = (data.get("preferred_roles") or [])[:5],
            preferred_locations = data.get("preferred_locations") or [],
            work_mode           = data.get("work_mode"),
            education           = data.get("education"),
            summary_text        = data.get("summary_text") or "",
            search_queries      = [],  # filled by caller
        )


def _safe_int(val) -> Optional[int]:
    """
    @brief Safely coerce a value to int, returning None on failure.

    @param val  Any value, typically from a JSON field (may be str, float, or None).
    @return     Integer representation of val, or None if conversion fails.
    """
    try:
        return int(val)
    except (TypeError, ValueError):
        return None
