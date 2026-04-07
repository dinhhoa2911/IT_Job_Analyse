"""
Hybrid retrieval pipeline:
  Step 1 — Dense search   : Milvus ANN (cosine similarity on 384-dim embeddings)
  Step 2 — Sparse search  : BM25 (exact keyword / TF-IDF matching)
  Step 3 — RRF fusion     : Reciprocal Rank Fusion (Cormack et al. 2009, k=60)
  Step 4 — Rerank         : Cross-Encoder (ms-marco-MiniLM-L-6-v2)

Why hybrid?
  Dense alone misses exact keyword matches (company names, specific skills).
  BM25 alone misses semantic similarity (paraphrases, Vietnamese queries).
  RRF combines both rank lists without needing to normalise raw scores.
  Cross-encoder re-scores (query, document) pairs jointly for final precision.
"""

import logging
import re

try:
    from underthesea import word_tokenize as _viet_tokenize
    _HAS_UNDERTHESEA = True
except ImportError:
    _HAS_UNDERTHESEA = False
import numpy as np
from pymilvus import Collection, connections, utility
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder, SentenceTransformer

from config import settings
from models.schemas import JobResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_HAS_VIET = re.compile(
    r'[àáảãạăắặằẳẵâấầẩẫậđèéẻẽẹêếềểễệ'
    r'ìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữự'
    r'ỳýỷỹỵ]'
)

_TOKENS = re.compile(r'\.?\w+(?:[+#]+|(?:\.\w+)+)?')

def _tokenize(text: str) -> list[str]:
    text = text.lower()

    # Tách tại ranh giới Việt / ASCII
    segments = re.split(r'([a-z0-9][a-z0-9\s\.\+\#]*[a-z0-9]|[a-z0-9]+)', text)

    tokens = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue

        if _HAS_VIET.search(seg) and _HAS_UNDERTHESEA:
            # Tiếng Việt → underthesea ghép từ ghép đúng
            segmented = _viet_tokenize(seg, format="text")
            tokens.extend(_TOKENS.findall(segmented))
        else:
            # ASCII → regex thẳng, không rủi ro
            tokens.extend(_TOKENS.findall(seg))

    return [t for t in tokens if len(t) > 1]

# Internal document type — rich dict used throughout the pipeline
_Doc = dict  # keys: job_id, job_title, job_link, text_content + score fields


# ---------------------------------------------------------------------------
# HybridSearchService
# ---------------------------------------------------------------------------

class HybridSearchService:
    """
    Lazy-initialised hybrid retrieval service.

    All heavy resources (models, Milvus connection, BM25 index) are loaded
    on first use and cached for the lifetime of the process.
    """

    # RRF constant from the original paper — 60 is the community standard
    _RRF_K: int = 60

    def __init__(self) -> None:
        self._encoder: SentenceTransformer | None = None
        self._reranker: CrossEncoder | None = None
        self._collection: Collection | None = None

        # BM25 state — built once from the full Milvus corpus
        self._corpus: list[_Doc] | None = None
        self._bm25: BM25Okapi | None = None

    # ------------------------------------------------------------------
    # Lazy loaders
    # ------------------------------------------------------------------

    def _get_encoder(self) -> SentenceTransformer:
        """Dense embedding model (must match Vectorize_To_Milvus.py)."""
        if self._encoder is None:
            logger.info("Loading dense encoder: %s", settings.embedding_model)
            self._encoder = SentenceTransformer(settings.embedding_model)
        return self._encoder

    def _get_reranker(self) -> CrossEncoder:
        """Cross-encoder for final relevance re-scoring."""
        if self._reranker is None:
            logger.info("Loading cross-encoder reranker: %s", settings.reranker_model)
            self._reranker = CrossEncoder(settings.reranker_model)
        return self._reranker

    def _get_collection(self) -> Collection:
        """Connect to Milvus and load the job collection."""
        if self._collection is None:
            logger.info(
                "Connecting to Milvus at %s:%s",
                settings.milvus_host,
                settings.milvus_port,
            )
            connections.connect(
                alias="default",
                host=settings.milvus_host,
                port=settings.milvus_port,
            )
            if not utility.has_collection(settings.milvus_collection):
                raise RuntimeError(
                    f"Milvus collection '{settings.milvus_collection}' not found. "
                    "Run scripts/Vectorize_To_Milvus.py first."
                )
            self._collection = Collection(settings.milvus_collection)
            self._collection.load()
            logger.info("Milvus collection '%s' loaded.", settings.milvus_collection)
        return self._collection

    def _get_bm25(self) -> tuple[list[_Doc], BM25Okapi]:
        """
        Fetch the full corpus from Milvus and build the BM25 index.
        Executed once; subsequent calls return the cached objects.
        """
        if self._corpus is None or self._bm25 is None:
            logger.info("Fetching corpus from Milvus to build BM25 index...")
            collection = self._get_collection()

            # Retrieve every document (dataset is ~3 k rows, fits in memory)
            raw = collection.query(
                expr="job_id > 0",
                output_fields=["job_id", "job_link", "job_title", "text_content"],
                limit=16_384,
            )

            self._corpus = raw
            tokenized_corpus = [_tokenize(doc["text_content"]) for doc in raw]
            self._bm25 = BM25Okapi(tokenized_corpus)
            logger.info("BM25 index built over %d documents.", len(raw))

        return self._corpus, self._bm25

    # ------------------------------------------------------------------
    # Step 1 — Dense retrieval
    # ------------------------------------------------------------------

    def _dense_search(self, query: str, k: int) -> list[_Doc]:
        """
        Embed the query with SentenceTransformer and run Milvus ANN search.

        Returns up to *k* candidates with their cosine similarity score.
        """
        encoder = self._get_encoder()
        collection = self._get_collection()

        query_vec = encoder.encode([query])[0].tolist()

        hits = collection.search(
            data=[query_vec],
            anns_field="embedding",
            param={
                "metric_type": "COSINE",
                # ef controls recall vs speed trade-off (higher = better recall)
                "params": {"ef": 256},
            },
            limit=k,
            output_fields=["job_id", "job_link", "job_title", "text_content"],
        )

        results: list[_Doc] = []
        for hit in hits[0]:
            entity = hit.entity
            results.append(
                {
                    "job_id":       entity.get("job_id"),
                    "job_title":    entity.get("job_title") or "",
                    "job_link":     entity.get("job_link") or "",
                    "text_content": entity.get("text_content") or "",
                    "dense_score":  round(float(hit.score), 6),
                }
            )

        logger.info("Dense search → %d candidates.", len(results))
        return results

    # ------------------------------------------------------------------
    # Step 2 — Sparse retrieval (BM25)
    # ------------------------------------------------------------------

    def _sparse_search(self, query: str, k: int) -> list[_Doc]:
        """
        Score all corpus documents with BM25 and return the top *k*.

        BM25 rewards exact token overlap — strong for skill names, company
        names, and location keywords that dense embeddings can under-weight.
        """
        corpus, bm25 = self._get_bm25()

        tokens = _tokenize(query)
        scores: np.ndarray = bm25.get_scores(tokens)  # shape (N,)

        # argsort descending, take top k with a positive score
        top_indices = np.argsort(scores)[::-1][:k]

        results: list[_Doc] = []
        for idx in top_indices:
            if scores[idx] <= 0.0:
                break  # remaining scores are all zero — no keyword overlap
            results.append(
                {
                    **corpus[idx],
                    "sparse_score": round(float(scores[idx]), 6),
                }
            )

        logger.info("Sparse (BM25) search → %d candidates.", len(results))
        return results

    # ------------------------------------------------------------------
    # Step 3 — Reciprocal Rank Fusion
    # ------------------------------------------------------------------

    def _rrf_fusion(
        self,
        dense: list[_Doc],
        sparse: list[_Doc],
        top_n: int,
    ) -> list[_Doc]:
        """
        Merge two ranked lists using Reciprocal Rank Fusion.

        Formula (Cormack et al. 2009):
            score(d) = Σ  1 / (rank_i(d) + k)
        where the sum is over each retriever that returned document d,
        rank_i(d) is 1-based rank in retriever i, and k=60.

        Advantages over score normalisation:
          - Rank positions are scale-invariant (cosine vs BM25 live in different spaces).
          - Proven robust across retrieval tasks.
          - No hyper-parameter tuning beyond k.
        """
        rrf_scores: dict[int, float] = {}
        doc_map: dict[int, _Doc] = {}

        for rank, doc in enumerate(dense, start=1):
            jid = doc["job_id"]
            rrf_scores[jid] = rrf_scores.get(jid, 0.0) + 1.0 / (rank + self._RRF_K)
            doc_map[jid] = doc

        for rank, doc in enumerate(sparse, start=1):
            jid = doc["job_id"]
            rrf_scores[jid] = rrf_scores.get(jid, 0.0) + 1.0 / (rank + self._RRF_K)
            if jid not in doc_map:
                doc_map[jid] = doc

        sorted_ids = sorted(rrf_scores, key=rrf_scores.__getitem__, reverse=True)

        fused: list[_Doc] = []
        for jid in sorted_ids[:top_n]:
            fused.append({**doc_map[jid], "rrf_score": round(rrf_scores[jid], 8)})

        logger.info(
            "RRF fusion: %d dense + %d sparse → %d unique candidates.",
            len(dense), len(sparse), len(fused),
        )
        return fused

    # ------------------------------------------------------------------
    # Step 4 — Cross-Encoder Rerank
    # ------------------------------------------------------------------

    def _rerank(self, query: str, candidates: list[_Doc], top_k: int) -> list[_Doc]:
        """
        Re-score candidates using a cross-encoder model.

        Unlike bi-encoders (which embed query and document separately),
        a cross-encoder receives the concatenated (query, document) pair
        and outputs a single relevance logit — much more accurate but
        computationally heavier, hence applied only to a short candidate list.

        Model: cross-encoder/ms-marco-MiniLM-L-6-v2
          - Trained on MS MARCO passage retrieval (650k QA pairs)
          - Input:  (query text, passage text)
          - Output: relevance logit (higher = more relevant, no fixed range)
          - Size:   ~22 MB — fast CPU inference
        """
        if not candidates:
            return []

        reranker = self._get_reranker()

        pairs = [(query, doc["text_content"]) for doc in candidates]
        scores: np.ndarray = reranker.predict(pairs)

        for doc, score in zip(candidates, scores):
            doc["rerank_score"] = round(float(score), 6)

        candidates.sort(key=lambda d: d["rerank_score"], reverse=True)

        logger.info(
            "Rerank: %d candidates → top %d selected. "
            "Score range [%.3f, %.3f]",
            len(candidates),
            min(top_k, len(candidates)),
            float(scores.min()),
            float(scores.max()),
        )
        return candidates[:top_k]

    # ------------------------------------------------------------------
    # Public API — warmup
    # ------------------------------------------------------------------

    def warmup(self) -> None:
        """
        Pre-load all models and build the BM25 index at startup.
        Call this from the FastAPI lifespan to avoid cold-start latency
        on the first user request.
        """
        logger.info("Warming up HybridSearchService...")
        self._get_encoder()
        self._get_reranker()
        self._get_bm25()   # also triggers Milvus connection + corpus fetch
        logger.info("HybridSearchService warmup complete.")

    # ------------------------------------------------------------------
    # Public API — search
    # ------------------------------------------------------------------

    def search(self, query: str, top_k: int | None = None) -> list[JobResult]:
        """
        Execute the full hybrid pipeline and return the final ranked results.

        Pool sizes:
          - Dense  : retrieval_k  candidates  (wide net for recall)
          - Sparse : retrieval_k  candidates  (wide net for recall)
          - RRF    : rerank_k     candidates  (deduplicated, merged)
          - Rerank : top_k        candidates  (final precision)

        Args:
            query:  User's natural-language search query.
            top_k:  Number of final results (default: settings.top_k_results).

        Returns:
            List of JobResult sorted by cross-encoder relevance (highest first).
            The `score` field contains the rerank logit.
        """
        top_k = top_k or settings.top_k_results

        # --- Step 1: Dense retrieval ---
        dense_candidates = self._dense_search(query, k=settings.retrieval_k)

        # --- Step 2: Sparse retrieval ---
        sparse_candidates = self._sparse_search(query, k=settings.retrieval_k)

        # --- Step 3: RRF fusion ---
        fused_candidates = self._rrf_fusion(
            dense_candidates,
            sparse_candidates,
            top_n=settings.rerank_k,
        )

        # --- Step 4: Cross-encoder rerank ---
        reranked = self._rerank(query, fused_candidates, top_k=top_k)

        logger.info(
            "Hybrid search complete | query='%s...' | final=%d results",
            query[:50],
            len(reranked),
        )

        return [
            JobResult(
                job_id=doc["job_id"],
                job_title=doc["job_title"],
                job_link=doc["job_link"],
                text_content=doc["text_content"],
                score=doc["rerank_score"],
            )
            for doc in reranked
        ]

    # ------------------------------------------------------------------
    # Multi-query RRF (used with query expansion / decomposition)
    # ------------------------------------------------------------------

    def _rrf_fusion_multi(
        self,
        rank_lists: list[list[_Doc]],
        top_n: int,
    ) -> list[_Doc]:
        """
        Generalised RRF across an arbitrary number of ranked lists.

        Used when QueryProcessor returns multiple query variants.
        Each variant produces its own dense + sparse rank lists, so the
        total number of lists passed here is  2 × len(queries).

        Formula (same as _rrf_fusion, extended to N lists):
            score(d) = Σ_i  1 / (rank_i(d) + k)

        A document appearing in MANY lists (i.e. relevant to many query
        variants) accumulates a higher score — exactly the desired behaviour
        for query expansion/decomposition.
        """
        rrf_scores: dict[int, float] = {}
        doc_map: dict[int, _Doc] = {}

        for rank_list in rank_lists:
            for rank, doc in enumerate(rank_list, start=1):
                jid = doc["job_id"]
                rrf_scores[jid] = rrf_scores.get(jid, 0.0) + 1.0 / (rank + self._RRF_K)
                if jid not in doc_map:
                    doc_map[jid] = doc

        sorted_ids = sorted(rrf_scores, key=rrf_scores.__getitem__, reverse=True)

        fused = [
            {**doc_map[jid], "rrf_score": round(rrf_scores[jid], 8)}
            for jid in sorted_ids[:top_n]
        ]

        logger.info(
            "Multi-query RRF: %d lists × avg %.0f docs → %d unique candidates.",
            len(rank_lists),
            sum(len(l) for l in rank_lists) / max(len(rank_lists), 1),
            len(fused),
        )
        return fused

    def search_multi(
        self,
        queries: list[str],
        top_k: int | None = None,
    ) -> list[JobResult]:
        """
        Hybrid search over multiple query variants (from query expansion /
        decomposition) with a single global RRF + cross-encoder rerank pass.

        Pipeline:
          For each query qᵢ in queries:
            dense_i  = _dense_search(qᵢ,  k=retrieval_k)
            sparse_i = _sparse_search(qᵢ, k=retrieval_k)
          all_lists = [dense_0, sparse_0, dense_1, sparse_1, ...]
          fused     = _rrf_fusion_multi(all_lists, top_n=rerank_k)
          reranked  = _rerank(queries[0], fused, top_k)   ← original query

        The cross-encoder always uses queries[0] (the original user query,
        guaranteed to be first by QueryProcessor.process()) so the final
        relevance scores reflect true user intent rather than any expanded
        variant.

        Args:
            queries: List produced by QueryProcessor.process().
                     queries[0] must be the original user query.
            top_k:  Final result count (default: settings.top_k_results).

        Returns:
            List of JobResult sorted by cross-encoder relevance.
        """
        top_k = top_k or settings.top_k_results

        # Collect dense + sparse rank lists for every query variant
        all_rank_lists: list[list[_Doc]] = []
        for q in queries:
            all_rank_lists.append(self._dense_search(q,  k=settings.retrieval_k))
            all_rank_lists.append(self._sparse_search(q, k=settings.retrieval_k))

        # Global RRF across all 2×N rank lists
        fused = self._rrf_fusion_multi(all_rank_lists, top_n=settings.rerank_k)

        # Rerank against the ORIGINAL query (queries[0])
        reranked = self._rerank(queries[0], fused, top_k=top_k)

        logger.info(
            "search_multi complete | %d queries | final=%d results",
            len(queries), len(reranked),
        )

        return [
            JobResult(
                job_id=doc["job_id"],
                job_title=doc["job_title"],
                job_link=doc["job_link"],
                text_content=doc["text_content"],
                score=doc["rerank_score"],
            )
            for doc in reranked
        ]
