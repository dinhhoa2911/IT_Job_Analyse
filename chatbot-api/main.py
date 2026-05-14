"""
@file main.py
@brief FastAPI application entry point for the IT Job Chatbot API.

Configures structured logging, registers the CORS middleware, mounts the
chat and CV-match routers, and pre-warms the HybridSearchService during the
FastAPI lifespan so the first user request does not pay a cold-start penalty.
"""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.chat import router, _pipeline
from routers.cv_match import router as cv_router

# ---------------------------------------------------------------------------
# Logging — structured, goes to stdout so Docker can capture it.
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lifespan — pre-load models & BM25 index at startup so the first
# user request doesn't pay the cold-start penalty (~5-10 s).
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    @brief FastAPI lifespan context manager — warm-up on startup, teardown on shutdown.

    Calls HybridSearchService.warmup() so dense/sparse models and the BM25
    index are cached before the first request arrives.  A Milvus connection
    failure is logged as a warning rather than crashing the server, allowing
    the API to start in a degraded-but-alive state during local development.

    @param app  The FastAPI application instance (injected by FastAPI).
    """
    logger.info("=== Startup: warming up HybridSearchService ===")
    try:
        _pipeline._vector_search.warmup()
        logger.info("=== Warmup complete. API is ready. ===")
    except Exception as exc:
        # Milvus may not be running in dev — log but don't crash the server
        logger.warning("Warmup skipped (Milvus unavailable?): %s", exc)
    yield
    logger.info("=== Shutdown ===")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="IT Job Chatbot API",
    description=(
        "RAG-powered chatbot for the IT job market analysis system. "
        "Combines hybrid search (Dense + BM25 + RRF + Rerank) with "
        "SQL analytics (Trino) and Claude LLM."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Allow the React dev server (localhost:3000) and any deployed origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(cv_router)
