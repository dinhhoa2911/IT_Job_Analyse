"""
@file config.py
@brief Application-wide settings loaded from the environment / .env file.

All tuneable knobs (model names, pool sizes, service hostnames) are centralised
here via Pydantic's BaseSettings so they can be overridden without code changes.
Import the ``settings`` singleton to access values throughout the application.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    @class Settings
    @brief Pydantic settings model — values are read from environment variables or .env.

    Field groups:
      - Anthropic  : LLM model and API key.
      - Milvus     : Vector database connection and target collection.
      - Trino      : Analytical query engine connection.
      - Embedding  : OpenAI embedding model, aligned with the indexing script.
      - Reranker   : CrossEncoder model for final relevance scoring.
      - Search pool: Controls candidate counts at each stage of the hybrid pipeline.
    """
    # Anthropic
    anthropic_api_key: str = Field(min_length=1, repr=False)
    anthropic_model: str = "claude-sonnet-4-6"

    # OpenAI — used for text-embedding-3-large dense encoder
    openai_api_key: str = Field(default="", repr=False)

    # Milvus
    milvus_host: str = "milvus-standalone"
    milvus_port: int = 19530
    milvus_collection: str = "it_jobs_rag"

    # Trino
    trino_host: str = "trino"
    trino_port: int = 8080          # internal container port
    trino_catalog: str = "iceberg"
    trino_user: str = "chatbot"

    # Embedding — must match the model used in Vectorize_To_Milvus.py
    embedding_model: str = "text-embedding-3-large"
    embedding_dim: int = 3072

    # Reranker — cross-encoder for final relevance scoring
    # multilingual alternative: cross-encoder/mmarco-mMiniLMv2-L12-H384-v1
    reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    # MinIO — object storage for user CV files
    minio_endpoint: str  = "minio:9000"
    minio_access_key: str = Field(min_length=1, repr=False)
    minio_secret_key: str = Field(min_length=1, repr=False)
    minio_cv_bucket: str  = "user-cvs"

    # Search pool sizes (final top_k is the result count returned to user)
    top_k_results: int = 10      # final results returned to user
    retrieval_k: int = 30        # candidates fetched per retriever (dense & sparse)
    rerank_k: int = 20           # candidates passed to cross-encoder after RRF

    # C?c timeout v?n ??t t?i call site, gi? nguy?n gi? tr? m?c ??nh.
    trino_metadata_timeout: int = 5
    trino_query_timeout: int = 8
    trino_forecast_timeout: int = 10
    trino_skill_gap_timeout: int = 15
    trino_learning_timeout: int = 20

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", hide_input_in_errors=True,
    )


settings = Settings()
