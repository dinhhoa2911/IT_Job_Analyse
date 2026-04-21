"""Chat router — POST /chat, GET /health."""

import logging

from fastapi import APIRouter, HTTPException, status

from models.schemas import ChatRequest, ChatResponse
from services.conversation_store import add_turn, get_history
from services.rag_pipeline import RAGPipeline

logger = logging.getLogger(__name__)

router = APIRouter()

# Module-level singleton — initialised once on first import.
_pipeline = RAGPipeline()


@router.get("/health", summary="Health check")
def health_check() -> dict:
    return {"status": "ok", "service": "IT Job Chatbot API"}


@router.post(
    "/chat",
    response_model=ChatResponse,
    summary="Send a message to the RAG chatbot",
    status_code=status.HTTP_200_OK,
)
def chat(request: ChatRequest) -> ChatResponse:
    """
    Process the user's message through the RAG pipeline and return a response.

    - **message**: Natural-language question or search query (max 2000 chars).
    - **session_id**: Client-generated session identifier (used for logging).
    - **conversation_id**: Client-generated conversation identifier for multi-turn memory.
    """
    logger.info(
        "[conv=%s session=%s] message=%r",
        request.conversation_id or "none",
        request.session_id,
        request.message[:80],
    )

    history = get_history(request.conversation_id) if request.conversation_id else []

    try:
        response = _pipeline.run(request, history=history)
    except Exception as exc:
        logger.error("Unhandled pipeline error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred. Please try again.",
        ) from exc

    if request.conversation_id:
        add_turn(request.conversation_id, request.message, response.answer)

    return response
