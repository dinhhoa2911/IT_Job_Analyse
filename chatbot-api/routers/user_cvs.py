"""
@file user_cvs.py
@brief REST endpoints for user CV management.

Stores PDF files in MinIO and metadata in Iceberg (via Trino).
Provides upload, list, delete, and re-match operations.

Endpoints:
  POST   /user/{uid}/cvs              — Upload new CV
  GET    /user/{uid}/cvs              — List all CVs for user
  DELETE /user/{uid}/cvs/{cv_id}      — Delete a CV
  POST   /user/{uid}/cvs/{cv_id}/match — Re-run job matching on saved CV
"""

import logging
import time

from fastapi import APIRouter, File, HTTPException, UploadFile, status

import services.cv_storage as storage
from models.schemas import CVMatchResponse
from routers.cv_match import _get_matcher, _get_processor, _get_skill_gap_analyzer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/user", tags=["user-cvs"])


@router.post("/{uid}/cvs", status_code=status.HTTP_201_CREATED)
async def upload_cv(uid: str, file: UploadFile = File(...)):
    """Upload CV PDF → MinIO + Iceberg metadata."""
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    data = await file.read()
    if len(data) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large. Max 5 MB.")

    try:
        result = storage.upload_cv(uid, file.filename, data)
        return result
    except Exception as exc:
        logger.error("CV upload failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="CV upload failed.")


@router.get("/{uid}/cvs")
async def list_cvs(uid: str):
    """Lấy danh sách CV đã lưu của user."""
    try:
        cvs = storage.list_cvs(uid)
        return cvs
    except Exception as exc:
        logger.error("List CVs failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve CVs.")


@router.delete("/{uid}/cvs/{cv_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cv(uid: str, cv_id: str):
    """Xóa CV khỏi MinIO và Iceberg."""
    try:
        storage.delete_cv(uid, cv_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="CV not found.")
    except Exception as exc:
        logger.error("CV delete failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="CV delete failed.")


@router.post("/{uid}/cvs/{cv_id}/match")
async def match_saved_cv(uid: str, cv_id: str, top_k: int = 10):
    """Tải CV đã lưu từ MinIO và chạy lại job matching."""
    try:
        data, filename = storage.get_cv_bytes(uid, cv_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="CV not found.")
    except Exception as exc:
        logger.error("CV download failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to load CV from storage.")

    try:
        t0      = time.monotonic()
        profile = _get_processor().process(data)
        matched = _get_matcher().match(profile, top_k=top_k)
        skill_gaps = _get_skill_gap_analyzer().analyze_multi(profile)
        elapsed = round((time.monotonic() - t0) * 1000)

        # Giống hệt cv_match.py: message 2 metrics, dùng CVMatchResponse để serialize đúng
        if matched:
            top_ai    = matched[0]
            top_score = max(matched, key=lambda j: j.match_score)
            role_hint  = profile.preferred_roles[0] if profile.preferred_roles else "IT"
            level_hint = profile.level or "IT"
            if top_ai.job_title == top_score.job_title:
                message = (
                    f"Tìm thấy {len(matched)} việc làm phù hợp với hồ sơ "
                    f"{level_hint} {role_hint} của bạn. "
                    f"Phù hợp nhất: {top_ai.job_title} ({top_score.match_score:.0f}% match)."
                )
            else:
                message = (
                    f"Tìm thấy {len(matched)} việc làm phù hợp với hồ sơ "
                    f"{level_hint} {role_hint} của bạn. "
                    f"AI gợi ý hàng đầu: {top_ai.job_title} ({top_ai.match_score:.0f}% match). "
                    f"Tỉ lệ kỹ năng cao nhất: {top_score.job_title} ({top_score.match_score:.0f}% match)."
                )
        else:
            message = "Không tìm thấy việc làm phù hợp. Hãy thử cập nhật CV với nhiều kỹ năng cụ thể hơn."

        return CVMatchResponse(
            cv_profile         = profile,
            matched_jobs       = matched,
            total_found        = len(matched),
            processing_time_ms = elapsed,
            message            = message,
            skill_gaps         = skill_gaps,
        )
    except Exception as exc:
        logger.error("CV matching failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Job matching failed: {exc}")
