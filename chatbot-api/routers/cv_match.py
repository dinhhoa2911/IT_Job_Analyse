"""
@file cv_match.py
@brief FastAPI router for the CV-matching endpoint (POST /match-cv).

Accepts a PDF CV upload (multipart/form-data), runs the full matching
pipeline, and returns structured results with matched jobs and CV profile.

Endpoint contract:
  POST /match-cv
    Content-Type: multipart/form-data
    Fields:
      file  : PDF file   (required, ≤ 5 MB)
      top_k : int 1-20   (optional, default 10)

  Response: CVMatchResponse JSON

Error codes:
  400 — wrong file type or empty file
  413 — file exceeds 5 MB limit
  422 — unprocessable PDF (image-only, encrypted, corrupted)
  500 — unexpected pipeline failure
"""

import logging
import time

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status

from models.schemas import CVMatchResponse
from services.cv_matcher import CVMatcherService
from services.cv_processor import CVProcessor
from services.skill_gap_analyzer import SkillGapAnalyzerService

logger = logging.getLogger(__name__)

router = APIRouter()

_MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB

# Module-level singletons — shared with chat router via lazy init
_cv_processor:   CVProcessor | None = None
_cv_matcher:     CVMatcherService | None = None
_skill_gap_analyzer: SkillGapAnalyzerService | None = None


def _get_processor() -> CVProcessor:
    """
    @brief Lazy-initialise and return the module-level CVProcessor singleton.

    @return  The shared CVProcessor instance.
    """
    global _cv_processor
    if _cv_processor is None:
        _cv_processor = CVProcessor()
    return _cv_processor


def _get_matcher() -> CVMatcherService:
    """
    @brief Lazy-initialise and return the module-level CVMatcherService singleton.

    Shares the HybridSearchService instance that was already warmed up by the
    chat router at startup.  Importing here (not at module level) avoids a
    circular-import during application startup.

    @return  The shared CVMatcherService instance.
    """
    global _cv_matcher
    if _cv_matcher is None:
        from routers.chat import _pipeline  # noqa: PLC0415
        _cv_matcher = CVMatcherService(_pipeline._vector_search)
    return _cv_matcher


def _get_skill_gap_analyzer() -> SkillGapAnalyzerService:
    """
    @brief Lazy-initialise and return the module-level SkillGapAnalyzerService singleton.

    @return  The shared SkillGapAnalyzerService instance.
    """
    global _skill_gap_analyzer
    if _skill_gap_analyzer is None:
        _skill_gap_analyzer = SkillGapAnalyzerService()
    return _skill_gap_analyzer


@router.post(
    "/match-cv",
    response_model=CVMatchResponse,
    summary="Upload CV (PDF) and receive top matching IT jobs",
    status_code=status.HTTP_200_OK,
)
async def match_cv(
    file:  UploadFile = File(..., description="CV / resume in PDF format (max 5 MB)"),
    top_k: int        = Form(default=10, ge=1, le=20, description="Number of jobs to return"),
) -> CVMatchResponse:
    """
    @brief Full CV-to-job matching pipeline executed in a single HTTP request.

    Steps:
      1. Validate PDF upload (type, size).
      2. Extract text from PDF via pdfplumber.
      3. LLM analysis → structured CVProfile (skills, level, roles, …).
      4. Generate 3-4 search query variants from the profile.
      5. Multi-query hybrid search (Dense + BM25 + RRF + cross-encoder rerank).
      6. Soft re-sort by preferred location / work mode.
      7. Normalise scores, annotate matched skills, assign match labels.
      8. Return CVMatchResponse.

    @param file   PDF file upload (multipart/form-data, max 5 MB).
    @param top_k  Number of job results to return (1-20, default 10).
    @return       CVMatchResponse with cv_profile, matched_jobs, and summary.
    @throws HTTPException  400 for wrong type or empty file.
    @throws HTTPException  413 when the file exceeds the 5 MB limit.
    @throws HTTPException  422 for unreadable or image-only PDFs.
    @throws HTTPException  500 for unexpected pipeline or LLM failures.
    """
    t0 = time.monotonic()

    # ── 1. Validate file type ──────────────────────────────────────────────────
    filename = (file.filename or "").lower()
    ct       = (file.content_type or "").lower()

    is_pdf_ct   = ct in ("application/pdf", "application/x-pdf")
    is_pdf_name = filename.endswith(".pdf")
    # Some browsers send application/octet-stream for PDFs
    is_octet    = ct == "application/octet-stream"

    if not (is_pdf_ct or (is_octet and is_pdf_name) or (not ct and is_pdf_name)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unsupported file type '{file.content_type}'. "
                "Please upload a PDF file (.pdf)."
            ),
        )

    # ── 2. Read & validate file size ──────────────────────────────────────────
    pdf_bytes = await file.read()

    if len(pdf_bytes) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )

    if len(pdf_bytes) > _MAX_FILE_SIZE_BYTES:
        mb = len(pdf_bytes) / (1024 * 1024)
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large ({mb:.1f} MB). Maximum allowed size is 5 MB.",
        )

    logger.info(
        "CV upload | file=%s | size=%d B | top_k=%d",
        file.filename, len(pdf_bytes), top_k,
    )

    # ── 3-4. Extract PDF text + LLM summarize + build queries ─────────────────
    try:
        profile = _get_processor().process(pdf_bytes)
    except ValueError as exc:
        # User-facing errors: bad PDF, image-only, too many pages, etc.
        logger.warning("CV processing rejected: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        # LLM failures
        logger.error("CV LLM error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CV analysis failed. Please try again.",
        ) from exc
    except Exception as exc:
        logger.error("Unexpected CV processor error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process CV. Ensure the PDF contains readable text.",
        ) from exc

    # ── 5. IT skill gate — cross-check against Gold dim_skill ────────────────────
    # Use technical_skills (rule-filtered, pre-language-merge) NOT profile.skills,
    # so "Vietnamese" / "English" cannot make a non-IT CV pass the gate.
    # Processor already guarantees technical_skills is non-empty at this point
    # (empty → ValueError → 422 above), so this gate targets hallucinated IT terms.
    it_check = _get_skill_gap_analyzer().has_it_skills(profile.technical_skills)
    if it_check is False:
        # Definitive: Trino confirmed 0 skills match any IT skill in the corpus.
        logger.warning(
            "CV rejected (dim_skill gate): 0 IT skills found | name=%s | skills=%s",
            profile.name or "unknown", profile.skills,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Không tìm thấy kỹ năng IT nào trong CV của bạn so với cơ sở dữ liệu "
                "việc làm hiện tại. Hệ thống chỉ hỗ trợ hồ sơ IT "
                "(lập trình, data, DevOps, v.v.)."
            ),
        )
    # it_check is None → Trino unavailable, proceed without gate (non-blocking)

    # ── 6-8. Hybrid search + annotation ──────────────────────────────────────
    try:
        matched_jobs = _get_matcher().match(profile, top_k=top_k)
    except ValueError as exc:
        # search_queries empty → LLM could not find IT-relevant skills in this CV
        logger.warning("CV rejected — not an IT profile (no search queries): %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "CV của bạn không chứa đủ thông tin kỹ năng IT để tìm kiếm việc làm phù hợp. "
                "Vui lòng tải lên CV ngành IT (lập trình, data, DevOps, v.v.)."
            ),
        ) from exc
    except Exception as exc:
        logger.error("CV matching error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job matching failed. Please try again.",
        ) from exc

    # ── 8. Skill gap analysis (non-blocking — Gold layer via Trino) ───────────
    skill_gaps = _get_skill_gap_analyzer().analyze_multi(profile)
    logger.info("Skill gaps | count=%d | categories=%s",
                len(skill_gaps), [g.role_category for g in skill_gaps])

    elapsed_ms = round((time.monotonic() - t0) * 1000)

    _MIN_SCORE = 15.0  # below this threshold → likely non-IT or weak CV

    # ── 8. Build summary message ───────────────────────────────────────────────
    if matched_jobs:
        top_ai        = matched_jobs[0]
        top_score     = max(matched_jobs, key=lambda j: j.match_score)
        best           = top_score.match_score
        role_hint     = profile.preferred_roles[0] if profile.preferred_roles else "IT"
        level_hint    = profile.level or "IT"

        if best < _MIN_SCORE:
            message = (
                f"Tìm thấy {len(matched_jobs)} việc làm nhưng độ phù hợp rất thấp "
                f"(cao nhất: {best:.0f}%). CV của bạn có thể không thuộc lĩnh vực IT "
                "hoặc thiếu kỹ năng kỹ thuật cụ thể. "
                "Hãy bổ sung các kỹ năng IT như ngôn ngữ lập trình, framework, công cụ."
            )
        elif top_ai.job_title == top_score.job_title:
            message = (
                f"Tìm thấy {len(matched_jobs)} việc làm phù hợp với hồ sơ "
                f"{level_hint} {role_hint} của bạn. "
                f"Phù hợp nhất: {top_ai.job_title} ({best:.0f}% match)."
            )
        else:
            message = (
                f"Tìm thấy {len(matched_jobs)} việc làm phù hợp với hồ sơ "
                f"{level_hint} {role_hint} của bạn. "
                f"AI gợi ý hàng đầu: {top_ai.job_title} ({top_ai.match_score:.0f}% match). "
                f"Tỉ lệ kỹ năng cao nhất: {top_score.job_title} ({best:.0f}% match)."
            )
    elif not profile.skills:
        message = (
            "CV của bạn không chứa kỹ năng IT nào được nhận diện. "
            "Hãy đảm bảo CV thuộc lĩnh vực IT và liệt kê cụ thể các kỹ năng kỹ thuật "
            "(ngôn ngữ lập trình, framework, công cụ, v.v.)."
        )
    else:
        message = (
            "Không tìm thấy việc làm phù hợp trong cơ sở dữ liệu hiện tại. "
            "Kỹ năng trong CV của bạn có thể chưa khớp với các vị trí IT hiện có. "
            "Hãy thử cập nhật CV với nhiều kỹ năng kỹ thuật cụ thể hơn."
        )

    logger.info(
        "CV match complete | jobs=%d | best=%.1f%% | elapsed=%d ms",
        len(matched_jobs),
        matched_jobs[0].match_score if matched_jobs else 0.0,
        elapsed_ms,
    )

    return CVMatchResponse(
        cv_profile         = profile,
        matched_jobs       = matched_jobs,
        total_found        = len(matched_jobs),
        processing_time_ms = elapsed_ms,
        message            = message,
        skill_gaps         = skill_gaps,
    )
