from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"

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
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384

    # Reranker — cross-encoder for final relevance scoring
    # multilingual alternative: cross-encoder/mmarco-mMiniLMv2-L12-H384-v1
    reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    # Search pool sizes (final top_k is the result count returned to user)
    top_k_results: int = 5       # final results returned to user
    retrieval_k: int = 20        # candidates fetched per retriever (dense & sparse)
    rerank_k: int = 10           # candidates passed to cross-encoder after RRF

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
