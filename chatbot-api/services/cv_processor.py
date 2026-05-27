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

# ── PaddleOCR singleton (lazy init — model download happens on first call) ─────

_ocr_engine = None


def _get_ocr():
    """
    @brief Lazy-initialise the PaddleOCR 3.x engine (downloads models on first call).

    Singleton — models (~300 MB) are loaded once per process. PaddleOCR 3.x
    uses PaddleX under the hood; lang='en' selects the English recognition model.
    """
    global _ocr_engine
    if _ocr_engine is None:
        from paddleocr import PaddleOCR  # noqa: PLC0415
        logger.info("Initialising PaddleOCR engine (first use — models may download).")
        _ocr_engine = PaddleOCR(lang="en")
        logger.info("PaddleOCR engine ready.")
    return _ocr_engine


def _ocr_pdf_pages(pdf_bytes: bytes) -> str:
    """
    @brief Extract text from a scanned / image-only PDF using PaddleOCR 3.x.

    Pipeline per page:
      1. Render the PDF page to a bitmap via PyMuPDF at 2× resolution (~300 DPI).
      2. Convert the bitmap to a uint8 RGB numpy array.
      3. Run PaddleOCR predict() on the array (3.x API).
      4. Concatenate recognised text from rec_texts field.

    @param pdf_bytes  Raw PDF file contents as bytes.
    @return           Concatenated OCR text from all pages (pages separated by blank lines).
    """
    import fitz          # noqa: PLC0415 — lazy import, only used for scanned PDFs
    import numpy as np   # noqa: PLC0415

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    parts: list[str] = []

    try:
        ocr = _get_ocr()
        for page_num in range(len(doc)):
            page = doc[page_num]
            # 2× zoom → ~300 DPI for better character recognition on dense CVs
            mat = fitz.Matrix(2.0, 2.0)
            pix = page.get_pixmap(matrix=mat, alpha=False)

            img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                pix.height, pix.width, pix.n
            )
            if pix.n == 4:
                img = img[:, :, :3]

            # PaddleOCR 3.x API: predict() returns list[OCRResult]
            # OCRResult['rec_texts'] → list[str], OCRResult['rec_scores'] → list[float]
            result = ocr.predict(img)

            if result and result[0]:
                page_res = result[0]
                texts = page_res.get("rec_texts", [])
                scores = page_res.get("rec_scores", [])
                lines = [
                    t for t, s in zip(texts, scores)
                    if t and t.strip() and s >= 0.5
                ]
                if lines:
                    parts.append("\n".join(lines))
                    logger.debug("OCR page %d: %d lines", page_num + 1, len(lines))
    finally:
        doc.close()

    return "\n\n".join(parts)

# ── LLM prompt ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are an expert technical recruiter. Extract structured information from the CV/resume text below.
Return ONLY a valid JSON object — no markdown fences, no explanation.

Required JSON schema:
{
  "name": "Full Name or null",
  "skills": ["Skill1", "Skill2"],
  "languages": ["English (IELTS 6.0)", "Vietnamese (Native)"],
  "experience_years": 2,
  "level": "intern|fresher|junior|mid|senior|lead",
  "preferred_roles": ["Backend Developer", "Data Engineer"],
  "preferred_locations": ["HCM", "Hanoi", "Remote"],
  "work_mode": "remote|hybrid|onsite|null",
  "education": "Degree and university or null",
  "summary_text": "2-4 sentence English description of what jobs this person should see."
}

EXTRACTION RULES:
- skills: technical only (programming languages, frameworks, databases, tools, cloud platforms).
  Max 25 items. Prioritize the explicit "Skills" / "Technical Skills" / "Technologies" section
  over skills mentioned inside project descriptions. Extract the Skills section FIRST.
- languages: ALL spoken/written language proficiencies the candidate mentions.
  Include the certification name and score if available.
  Format: "Language (Certification Score)" or just "Language (Native/Fluent/etc.)".
  Examples: "English (IELTS 6.0)", "English (TOEIC 800)", "Vietnamese (Native)", "Japanese (N3)".
  If the CV lists English under Soft Skills or Certifications without a score, still extract it.
  NEVER leave this empty if the CV mentions any language or language certification.
- experience_years: count ONLY paid professional roles — full-time jobs, part-time jobs,
  internships at companies. Student projects, coursework, thesis work, university
  competitions, and personal side-projects do NOT count as experience.
  Set 0 for anyone currently enrolled as a student with no company work history.
- level: derive strictly from experience_years, do NOT use job title keywords to override.
  0 yr (student, no company experience) → "fresher".
  0 yr but CV explicitly seeks internship → "intern".
  > 0 and < 1 yr → "fresher".
  1–3 yr → "junior". 3–6 yr → "mid". 6–10 yr → "senior". 10+ yr → "lead".
- preferred_roles: job title categories in English. Infer from past titles and skills. Max 5.
- preferred_locations: only if explicitly stated in the CV; otherwise [].
- summary_text: MUST mention top 5 skills, level, years of experience, and target role type.
  Write as if addressing a job recommender engine. Example:
  "Junior Backend Developer with 1 year of experience in Java, Spring Boot, MySQL, JavaScript, and Python.
   Looking for backend or software engineering roles in HCM."
"""

# ── Rule-based skill validator ────────────────────────────────────────────────
# Blacklist: soft skills, personality traits, and generic phrases that the LLM
# might erroneously include when instructed to extract technical skills.
# Kept intentionally broad; the dim_skill gate provides the final authoritative check.

_SOFT_SKILL_BLACKLIST: frozenset[str] = frozenset({
    # Personality / attitude
    "hard working", "hardworking", "fast learner", "quick learner", "self motivated",
    "self-motivated", "proactive", "detail oriented", "detail-oriented", "creative",
    "passionate", "dedicated", "responsible", "reliable", "flexible", "adaptable",
    "punctual", "honest", "discipline", "critical thinking", "analytical",
    # Interpersonal
    "communication", "teamwork", "team work", "team player", "collaboration",
    "interpersonal", "presentation", "negotiation", "leadership", "mentoring",
    "coaching", "conflict resolution", "active listening",
    # Generic work skills
    "problem solving", "problem-solving", "time management", "multitasking",
    "organization", "planning", "decision making", "decision-making",
    "project management", "risk management", "attention to detail",
    # Vietnamese soft skills
    "kỹ năng giao tiếp", "làm việc nhóm", "kỹ năng thuyết trình",
    "tư duy phân tích", "giải quyết vấn đề", "quản lý thời gian",
    "chủ động", "sáng tạo", "trách nhiệm", "cẩn thận", "tỉ mỉ",
    # Non-skill labels
    "microsoft office", "office", "word", "excel", "powerpoint",
})

# Minimum token length to be a plausible technical skill name
_MIN_SKILL_LEN = 1  # "R", "C", "Go" are valid


def _is_technical_skill(skill: str) -> bool:
    """
    @brief Rule-based check: is this string plausibly a technical IT skill?

    Two-pass filter:
      1. Blacklist — reject known soft-skill phrases.
      2. Heuristic — keep anything that looks like a tool, language, or framework
         name (has uppercase acronym, digits, dots, slashes, +, #, etc.).
         Single-word all-lowercase entries < 3 chars are also kept (Go, R, C).

    @param skill  Raw skill string from LLM.
    @return       True if the skill should be kept in the technical list.
    """
    normalized = skill.strip().lower()
    if not normalized or len(normalized) < _MIN_SKILL_LEN:
        return False
    if normalized in _SOFT_SKILL_BLACKLIST:
        return False
    # Reject pure generic long phrases (>4 words, all lowercase, no digits/special)
    words = normalized.split()
    if len(words) > 4:
        return False
    if len(words) > 2 and normalized == normalized.lower() and not any(
        c in normalized for c in "0123456789.+#/-_@"
    ):
        return False
    return True


def _clean_extracted_skills(skills: list[str]) -> list[str]:
    """
    @brief Apply rule-based filter to the LLM-extracted skill list.

    @param skills  Raw list from LLM (may contain soft skills or hallucinations).
    @return        Filtered list containing only plausible technical skill names.
    """
    cleaned = [s for s in skills if _is_technical_skill(s)]
    removed = len(skills) - len(cleaned)
    if removed:
        logger.info(
            "Rule-based skill filter: %d removed, %d kept. "
            "Removed: %s",
            removed, len(cleaned),
            [s for s in skills if not _is_technical_skill(s)],
        )
    return cleaned


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
    @brief Extract plain text from a PDF.

    Tries pdfplumber first (fast, accurate for text-layer PDFs).  Falls back to
    PaddleOCR when pdfplumber yields no text — this handles scanned / image-only
    PDFs transparently without requiring the user to re-export their CV.

    @param pdf_bytes  Raw PDF file contents as bytes.
    @return           Concatenated text from all pages.
    @throws ValueError  If the PDF has no pages, exceeds 20 pages, or is unreadable
                        even after OCR.
    """
    parts: list[str] = []
    page_count: int = 0

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page_count = len(pdf.pages)
            if page_count == 0:
                raise ValueError("PDF has no pages.")
            if page_count > 20:
                raise ValueError(
                    f"PDF has {page_count} pages. Please upload a CV (max 20 pages)."
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

    if parts:
        raw = "\n\n".join(parts)
        logger.info("PDF extracted (pdfplumber): %d chars, %d page(s).", len(raw), page_count)
        return raw

    # ── OCR fallback ───────────────────────────────────────────────────────────
    logger.info(
        "pdfplumber found no text on %d page(s) — falling back to PaddleOCR.", page_count
    )
    try:
        ocr_text = _ocr_pdf_pages(pdf_bytes)
    except Exception as exc:
        logger.error("PaddleOCR failed: %s", exc, exc_info=True)
        raise ValueError(
            "Could not extract text from this PDF. "
            "The file appears to be a scanned image but OCR also failed. "
            "Please try re-exporting your CV as a text-based PDF."
        ) from exc

    if not ocr_text.strip():
        raise ValueError(
            "No text could be recognised in this PDF after OCR. "
            "Please ensure the scan is clear and try again, "
            "or re-export your CV as a text-based PDF."
        )

    logger.info("PDF extracted (PaddleOCR): %d chars, %d page(s).", len(ocr_text), page_count)
    return ocr_text


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
        full_clean = _clean(raw)   # keep full text for rule-based post-processing

        # 3. Smart truncation for LLM — keep head (experience) + tail (skills section).
        # Skills sections typically appear at the END of a CV.  A naive head-only
        # truncation at 4k chars cuts off page 3+ entirely on multi-page CVs.
        HEAD = 5_000
        TAIL = 3_000
        if len(full_clean) > HEAD + TAIL:
            logger.info(
                "CV text smart-truncated %d → %d chars (head=%d + tail=%d).",
                len(full_clean), HEAD + TAIL, HEAD, TAIL,
            )
            clean = full_clean[:HEAD] + "\n\n[...]\n\n" + full_clean[-TAIL:]
        else:
            clean = full_clean

        # 4. LLM extraction
        profile = self._summarize(clean)

        # 5. Rule-based skill filter — strip soft skills / hallucinated phrases
        #    before level normalization and language merge so those steps see
        #    only plausible technical terms.
        profile.skills = _clean_extracted_skills(profile.skills)

        # 6. Hard gate: if zero technical skills survive the rule filter, reject now.
        #    This runs before language merge so "Vietnamese" / "English" cannot
        #    rescue a non-IT CV.  No Trino needed — purely local, instant.
        if not profile.skills:
            logger.warning(
                "CV rejected (rule gate): 0 technical skills after filter. "
                "name=%s | education=%s",
                profile.name or "unknown", profile.education or "?",
            )
            raise ValueError(
                "CV của bạn không chứa kỹ năng IT kỹ thuật nào được nhận diện "
                "(ngôn ngữ lập trình, framework, công cụ, database, cloud, v.v.). "
                "Hệ thống chỉ hỗ trợ hồ sơ thuộc lĩnh vực Công nghệ Thông tin."
            )

        # 7. Deterministic level override — prevents LLM from inflating level
        #    (e.g. calling a student "junior" because they listed "Python Programmer" as title).
        corrected = _normalize_level(profile.experience_years, profile.level)
        if corrected != profile.level:
            logger.info(
                "Level corrected: LLM='%s' → rule='%s' (experience_years=%s)",
                profile.level, corrected, profile.experience_years,
            )
        profile.level = corrected

        # 8. Snapshot technical skills BEFORE language merge — used by the
        #    dim_skill gate in the router so languages like "Vietnamese" (which
        #    may exist in dim_skill as a soft requirement in job postings) cannot
        #    make a non-IT CV pass the gate.
        profile.technical_skills = list(profile.skills)

        # 9. Merge languages into skills so the skill gap analyzer can compare them
        #    against market requirements (e.g. "English" appears in 23% of job postings).
        for lang in profile.languages:
            lang_clean = lang.split("(")[0].strip()   # "English (IELTS 6.0)" → "English"
            if lang_clean and lang_clean.lower() not in {s.lower() for s in profile.skills}:
                profile.skills.append(lang_clean)

        # 9. Build search queries
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
                max_tokens=900,
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
            skills              = (data.get("skills") or [])[:25],
            languages           = (data.get("languages") or [])[:10],
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


_VALID_LEVELS = {"intern", "fresher", "junior", "mid", "senior", "lead"}


def _normalize_level(experience_years: Optional[int], llm_level: Optional[str]) -> str:
    """
    @brief Deterministically derive the seniority level from professional experience years.

    The LLM is instructed to set this correctly but may hallucinate (e.g. inferring
    "junior" from a title keyword for a student with 0 real experience). This function
    overrides any LLM error with a rule-based mapping so the displayed level is always
    consistent with the reported experience.

    Special case: if the LLM explicitly said "intern" AND experience_years == 0,
    keep "intern" since that signals the candidate is actively seeking an internship.

    @param experience_years  Professional years from LLM (may be None).
    @param llm_level         Level string from LLM (may be None or invalid).
    @return                  Normalised level string.
    """
    yrs = experience_years if isinstance(experience_years, int) else 0

    if yrs == 0:
        # Preserve "intern" if the LLM explicitly chose it; default to "fresher"
        if llm_level == "intern":
            return "intern"
        return "fresher"
    if yrs < 1:
        return "fresher"
    if yrs <= 3:
        return "junior"
    if yrs <= 6:
        return "mid"
    if yrs <= 10:
        return "senior"
    return "lead"
