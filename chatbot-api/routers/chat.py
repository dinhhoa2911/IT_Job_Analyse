"""
@file chat.py
@brief FastAPI router for the chat endpoint (POST /chat) and health check (GET /health).

Holds the module-level RAGPipeline singleton and delegates each request to
RAGPipeline.run().  Conversation history is fetched from and written back to
ConversationStore so multi-turn context is maintained across calls.
"""

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
    """
    @brief Liveness probe — returns a static JSON payload confirming the API is running.

    @return  Dict with ``status`` and ``service`` keys.
    """
    return {"status": "ok", "service": "IT Job Chatbot API"}


@router.post(
    "/chat",
    response_model=ChatResponse,
    summary="Send a message to the RAG chatbot",
    status_code=status.HTTP_200_OK,
)
def chat(request: ChatRequest) -> ChatResponse:
    """
    @brief Process the user's message through the RAG pipeline and return a structured response.

    Retrieves existing conversation history (if ``conversation_id`` is provided),
    runs the full RAGPipeline, then appends the new turn to the store.

    @param request  ChatRequest containing the user message, session ID, and conversation ID.
    @return         ChatResponse with the LLM-generated answer and optional structured data.
    @throws HTTPException  500 if an unhandled error occurs inside the pipeline.
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
