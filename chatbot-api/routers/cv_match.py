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

logger = logging.getLogger(__name__)

router = APIRouter()

_MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB

# Module-level singletons — shared with chat router via lazy init
_cv_processor: CVProcessor | None = None
_cv_matcher:   CVMatcherService | None = None


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

    # ── 5-7. Hybrid search + annotation ───────────────────────────────────────
    try:
        matched_jobs = _get_matcher().match(profile, top_k=top_k)
    except Exception as exc:
        logger.error("CV matching error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job matching failed. Please try again.",
        ) from exc

    elapsed_ms = round((time.monotonic() - t0) * 1000)

    # ── 8. Build summary message ───────────────────────────────────────────────
    if matched_jobs:
        top_ai        = matched_jobs[0]                                    # AI hybrid ranking
        top_score     = max(matched_jobs, key=lambda j: j.match_score)    # highest skill match
        role_hint     = profile.preferred_roles[0] if profile.preferred_roles else "IT"
        level_hint    = profile.level or "IT"

        if top_ai.job_title == top_score.job_title:
            # Cả 2 metrics đồng thuận → gộp 1 dòng
            message = (
                f"Tìm thấy {len(matched_jobs)} việc làm phù hợp với hồ sơ "
                f"{level_hint} {role_hint} của bạn. "
                f"Phù hợp nhất: {top_ai.job_title} ({top_score.match_score:.0f}% match)."
            )
        else:
            # 2 metrics khác nhau → giải thích rõ cả 2
            message = (
                f"Tìm thấy {len(matched_jobs)} việc làm phù hợp với hồ sơ "
                f"{level_hint} {role_hint} của bạn. "
                f"AI gợi ý hàng đầu: {top_ai.job_title} ({top_ai.match_score:.0f}% match). "
                f"Tỉ lệ kỹ năng cao nhất: {top_score.job_title} ({top_score.match_score:.0f}% match)."
            )
    else:
        message = (
            "Không tìm thấy việc làm phù hợp trong cơ sở dữ liệu hiện tại. "
            "Hãy thử cập nhật CV với nhiều kỹ năng kỹ thuật hơn."
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
    )
