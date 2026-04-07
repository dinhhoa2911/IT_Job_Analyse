"""
Pre-retrieval query enhancement — runs BEFORE the hybrid search step.

Two techniques are applied sequentially:

1. Query Decomposition
   A complex query with multiple distinct aspects is split into 2-3
   focused sub-queries so each retrieval call has a single, clear intent.

   Example:
     Input : "tìm job React senior remote ở HCM lương 30M"
     Output: ["React senior developer",
              "remote job Ho Chi Minh City",
              "frontend engineer high salary Vietnam"]

   A simple, single-intent query is returned as-is (no decomposition).

2. Query Expansion
   Each sub-query is expanded with 2 semantic variants using different
   terminology — paraphrases, synonyms, tech stack aliases — to widen
   the vocabulary coverage of both dense and sparse retrievers.

   Example:
     Input : "Python backend developer"
     Output: ["Python backend developer",
              "Python software engineer server-side API",
              "backend engineer Django Flask REST"]

Why pre-retrieval enhancement?
   Dense embeddings capture semantics but may miss exact terminology.
   BM25 captures exact terms but misses paraphrases.
   Expanding + decomposing the query improves recall from BOTH retrievers
   before the RRF fusion and cross-encoder rerank narrow results down.
"""

import json
import logging

from openai import OpenAI

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_DECOMPOSE_SYSTEM = """You are a search query analyst for an IT job search engine.

Task: Decide if the query contains MULTIPLE distinct search aspects.
If yes, split it into 2-3 focused sub-queries (one aspect per sub-query).
If no (single clear intent), return the query unchanged as a one-element list.

Rules:
- Each sub-query must be a standalone search phrase in English or Vietnamese.
- Keep sub-queries concise (3-6 words).
- Do NOT add information not present in the original query.
- Return a JSON array of strings ONLY. No explanation, no markdown.

Examples:
Input:  "tìm job React senior remote HCM lương cao"
Output: ["React senior developer", "remote job Ho Chi Minh", "high salary frontend engineer"]

Input:  "Python developer"
Output: ["Python developer"]

Input:  "backend Java Spring Boot Hà Nội không cần kinh nghiệm"
Output: ["Java Spring Boot backend developer", "entry level junior Java", "job Ha Noi"]"""


_EXPAND_SYSTEM = """You are a search query expansion specialist for an IT job search engine.

Task: Given a job search query, generate exactly 2 semantic variants using
different but equivalent terminology (synonyms, tech aliases, paraphrases).

Rules:
- Return a JSON array of exactly 3 strings: [original, variant_1, variant_2].
- Variants must preserve the SAME intent and requirements as the original.
- Use common IT industry terminology (English preferred for tech terms).
- Do NOT add new requirements not in the original.
- No markdown, no explanation.

Examples:
Input:  "Python backend developer"
Output: ["Python backend developer",
         "Python software engineer REST API server-side",
         "backend engineer Django Flask microservices"]

Input:  "tìm job Data Engineer remote"
Output: ["tìm job Data Engineer remote",
         "Data Engineer work from home pipeline ETL",
         "remote data pipeline engineer Spark Kafka"]

Input:  "React senior HCM"
Output: ["React senior HCM",
         "senior React developer Ho Chi Minh City frontend",
         "experienced ReactJS engineer HCMC JavaScript"]"""


# ---------------------------------------------------------------------------
# QueryProcessor
# ---------------------------------------------------------------------------

class QueryProcessor:
    """
    Runs decomposition then expansion on a raw user query.

    Output is a deduplicated list of query strings ready to be passed
    to HybridSearchService.search_multi().
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call(self, system: str, user: str, max_tokens: int = 256) -> list[str]:
        """Call OpenAI and parse the JSON array response."""
        response = self._client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw = response.choices[0].message.content.strip()

        # Strip accidental markdown code fences
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw = "\n".join(lines[1:-1]).strip()

        try:
            result = json.loads(raw)
            if isinstance(result, list) and all(isinstance(s, str) for s in result):
                return [s.strip() for s in result if s.strip()]
        except (json.JSONDecodeError, ValueError):
            pass

        # Fallback: return the original user text unparsed
        logger.warning("Failed to parse LLM JSON response: %r", raw[:120])
        return [user]

    def _decompose(self, query: str) -> list[str]:
        """
        Split a complex query into focused sub-queries.
        Returns [query] unchanged if the query is already single-intent.
        """
        sub_queries = self._call(_DECOMPOSE_SYSTEM, query, max_tokens=200)
        if len(sub_queries) > 1:
            logger.info(
                "Decomposed '%s...' → %d sub-queries: %s",
                query[:50], len(sub_queries), sub_queries,
            )
        return sub_queries

    def _expand(self, query: str) -> list[str]:
        """
        Expand a single query into 3 semantic variants (original + 2).
        """
        variants = self._call(_EXPAND_SYSTEM, query, max_tokens=200)
        # Guarantee the original is always first
        if query not in variants:
            variants.insert(0, query)
        logger.info("Expanded '%s...' → %d variants", query[:50], len(variants))
        return variants

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, query: str) -> list[str]:
        """
        Full pre-retrieval enhancement pipeline:
          decompose(query) → [sub_q1, sub_q2, ...]
          expand(sub_qi)   → [sub_qi, variant_1, variant_2]

        Returns a flat, deduplicated list of query strings.
        The original query is always index 0 (used later for reranking).

        Args:
            query: Raw user message classified as search_job.

        Returns:
            List of 3-9 query strings (depending on decomposition depth).
        """
        # Step 1 — decompose
        sub_queries = self._decompose(query)

        # Step 2 — expand each sub-query
        all_queries: list[str] = []
        for sq in sub_queries:
            expanded = self._expand(sq)
            all_queries.extend(expanded)

        # Deduplicate while preserving order; ensure original is first
        seen: set[str] = set()
        deduped: list[str] = []
        # Always put original query first so reranker uses the true intent
        for q in [query] + all_queries:
            if q not in seen:
                seen.add(q)
                deduped.append(q)

        logger.info(
            "QueryProcessor: '%s...' → %d final queries",
            query[:50], len(deduped),
        )
        return deduped
