"""
@file query_processor.py
@brief Pre-retrieval query enhancement using a fast-path / slow-path architecture.

Fast-path (0 LLM calls): for simple, single-skill queries.
    - All lookup metadata (dim_skill, dim_location, dim_work_mode, role patterns)
      is loaded from Trino ONCE at startup and cached in memory.
    - Fields are extracted via regex + lookup sets in under 1 ms.
    - Three query variants are built from role-pattern templates derived from
      actual Silver-layer job title data.

Slow-path (2 LLM calls): for complex, ambiguous, or multi-intent queries.
    - LLM Decompose step splits a multi-aspect query into 2-3 sub-queries.
    - LLM Expand step generates 2 semantic variants per sub-query.

Routing: _is_simple() checks four deterministic conditions with no LLM call.
If Trino is unavailable at startup, fast-path is disabled and all queries fall
through to slow-path gracefully.
"""

import json
import logging
import re

import trino
import anthropic

from config import settings
from constants import LOCATION_ALIAS_TO_NAME, LOCATION_CITY_NAME, SKILL_QUERY_ALIASES

logger = logging.getLogger(__name__)


# ── Stable constants ──────────────────────────────────────────────────────────
# These are language/domain constants, not IT-market data → safe to define here.

_LEVEL_MAP: dict[str, str] = {
    "senior": "senior",   "sr": "senior",
    "junior": "junior",   "jr": "junior",
    "fresher": "fresher", "intern": "intern",
    "entry level": "entry level", "entry-level": "entry level",
    "lead": "lead",       "tech lead": "lead",
    "manager": "manager", "mid level": "mid-level",
    "mid-level": "mid-level", "middle": "mid-level",
}

# LOCATION_ALIAS_TO_NAME imported from constants — maps any alias → canonical city name
# e.g. "hcm" → "Ho Chi Minh", "sài gòn" → "Ho Chi Minh"

# Shorter display names for query variant generation (derived from constants)
_LOCATION_SHORT: dict[str, str] = {
    LOCATION_CITY_NAME["hcm"]:    "HCMC",
    LOCATION_CITY_NAME["hanoi"]:  "Hanoi",
    LOCATION_CITY_NAME["danang"]: "Da Nang",
    "remote": "remote",
}

# SKILL_QUERY_ALIASES imported from constants — maps user-typed variants to DB canonical.
# e.g. "golang" → "go", "k8s" → "kubernetes", "react native" → "react_native"
# See constants.py for the full ground-truth list with DB job counts.

# Tokens that signal advisory / ambiguous intent → route to slow-path.
_AMBIGUITY_TOKENS = {
    "nên", "muốn", "gợi ý", "thế nào", "như thế nào",
    "hay là", "tốt hơn", "phù hợp", "khuyên", "hay không",
    "should", "recommend", "suggest", "which", "better",
}

# Tokens that signal an analytics / open question → not a job search.
_QUESTION_TOKENS = {
    "?", "bao nhiêu", "bao lâu",
    "how many", "how much", "what is", "what are",
}


# ── LLM prompts (slow-path, unchanged) ───────────────────────────────────────

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
- IMPORTANT — normalize skill names to canonical forms in variants:
  Alias      → Canonical (always include in at least one variant)
  golang/Golang → Go  |  k8s/k8 → Kubernetes  |  k8 → Kubernetes
  nodejs/NodeJS → Node.js  |  vuejs/vue.js → Vue  |  reactjs → React
  react native/react-native/reactnative → React Native
  ci/cd/cicd → CI/CD  |  microservices → Microservices
  scikit-learn/sklearn → Scikit-learn  |  dotnet/.net → .NET
  elastic search → Elasticsearch  |  elk → ELK Stack
  airflow → Apache Airflow  |  postgres → PostgreSQL
  powerbi → Power BI  |  tailwindcss → Tailwind
- Handle typos and unusual spellings by normalizing to standard names in variants.
  If user typed "golng" → treat as Golang/Go.
  If user typed "k8" → treat as Kubernetes.
  If user typed "reactnative" → treat as React Native.
- Do NOT add new requirements not in the original.
- No markdown, no explanation.

Examples:
Input:  "Python backend developer"
Output: ["Python backend developer",
         "Python software engineer REST API server-side",
         "backend engineer Django Flask microservices"]

Input:  "React senior HCM"
Output: ["React senior HCM",
         "senior React developer Ho Chi Minh City frontend",
         "experienced ReactJS engineer HCMC JavaScript"]

Input:  "Golang Kubernetes DevOps engineer"
Output: ["Golang Kubernetes DevOps engineer",
         "Go developer Kubernetes infrastructure DevOps",
         "Golang engineer container orchestration CI/CD K8s"]

Input:  "golng k8 devops"
Output: ["golng k8 devops",
         "Go developer Kubernetes DevOps engineer",
         "Golang engineer Kubernetes infrastructure CI/CD"]"""


# ── QueryProcessor ────────────────────────────────────────────────────────────

class QueryProcessor:
    """
    @class QueryProcessor
    @brief Pre-retrieval query enhancer — routes to fast-path or slow-path based on query complexity.

    Fast-path (0 LLM calls):
      Metadata (skills, locations, work modes, role patterns) is loaded from
      Trino at construction time.  Simple single-skill queries are processed
      entirely with regex + dict lookups, generating three diverse variants
      using data-driven role-title suffixes.

    Slow-path (2 LLM calls):
      Complex or ambiguous queries go through an LLM decompose step (splits
      into sub-queries) followed by an LLM expand step (synonyms/paraphrases
      per sub-query).

    The original query is always preserved at index 0 of the returned list
    so the cross-encoder reranker evaluates against true user intent.
    """

    def __init__(self) -> None:
        self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        # Fast-path state (populated by _load_db_metadata)
        self._skill_map: dict[str, str] = {}       # lowercase → original casing
        self._location_lookup: dict[str, str] = {} # alias/canonical → canonical
        self._mode_set: frozenset[str] = frozenset()
        self._role_patterns: list[str] = []        # top role suffixes from data

        self._load_db_metadata()

    # ── DB metadata loader ────────────────────────────────────────────────────

    def _query_trino(self, sql: str) -> list[tuple]:
        """
        @brief Execute a SQL string on Trino and return raw result tuples.

        @param sql  Valid Trino SQL string.
        @return     List of row tuples (column order matches the SELECT list).
        @throws Exception  Propagated from the Trino driver on connection or query failure.
        """
        conn = trino.dbapi.connect(
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog=settings.trino_catalog,
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    def _load_db_metadata(self) -> None:
        """
        Load all fast-path lookup structures from Trino at startup.
        On any failure, logs a warning and disables fast-path (slow-path takes over).
        """
        try:
            # 1. Skills: lowercase → original casing from dim_skill
            rows = self._query_trino(
                "SELECT skill_name FROM iceberg.gold.dim_skill"
            )
            self._skill_map = {r[0].lower(): r[0] for r in rows}

            # 2. Locations: canonical names from dim_location + user-typed aliases
            rows = self._query_trino(
                "SELECT city_name FROM iceberg.gold.dim_location"
            )
            canonical = {r[0].lower(): r[0] for r in rows}
            # LOCATION_ALIAS_TO_NAME takes priority (more specific user-typed variants)
            self._location_lookup = {**canonical, **LOCATION_ALIAS_TO_NAME}

            # 3. Work modes from dim_work_mode
            rows = self._query_trino(
                "SELECT work_mode FROM iceberg.gold.dim_work_mode"
            )
            self._mode_set = frozenset(r[0].lower() for r in rows)

            # 4. Top role-title patterns from actual job data (silver layer)
            rows = self._query_trino("""
                SELECT pattern, COUNT(*) AS cnt FROM (
                    SELECT
                        CASE
                            WHEN LOWER(job_title) LIKE '%software engineer%' THEN 'software engineer'
                            WHEN LOWER(job_title) LIKE '%engineer%'          THEN 'engineer'
                            WHEN LOWER(job_title) LIKE '%developer%'         THEN 'developer'
                            WHEN LOWER(job_title) LIKE '%programmer%'        THEN 'programmer'
                            ELSE NULL
                        END AS pattern
                    FROM iceberg.silver.it_jobs_clean
                )
                WHERE pattern IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC
                LIMIT 3
            """)
            self._role_patterns = (
                [r[0] for r in rows] or ["developer", "engineer", "software engineer"]
            )

            logger.info(
                "Fast-path metadata loaded | skills=%d  locations=%d  modes=%s  patterns=%s",
                len(self._skill_map),
                len(self._location_lookup),
                sorted(self._mode_set),
                self._role_patterns,
            )

        except Exception as exc:
            logger.warning(
                "Trino metadata load failed (%s) — fast-path disabled, all queries use slow-path.",
                exc,
            )
            self._skill_map = {}

    # ── Fast-path helpers ─────────────────────────────────────────────────────

    def _detect_skills(self, query_lower: str) -> list[str]:
        """
        @brief Return lowercase skill keys found in the query using word-boundary-aware matching.

        Checks _SKILL_ALIASES first (e.g. "golang" → "go", "k8s" → "kubernetes") so
        user-typed variants resolve to the canonical DB skill name before the full
        skill_map scan.  Handles dotted/special names (node.js, c#, vue.js).

        @param query_lower  Lowercased query string.
        @return             List of matched skill keys from self._skill_map (no duplicates).
        """
        found: list[str] = []
        # Resolve aliases first (longer aliases take priority via sorted order)
        for alias, canonical in sorted(SKILL_QUERY_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
            escaped = re.escape(alias)
            if re.search(rf"(?<![a-z0-9.]){escaped}(?![a-z0-9.])", query_lower):
                if canonical in self._skill_map and canonical not in found:
                    found.append(canonical)
        # Then scan the full skill_map for direct matches
        for skill_lower in self._skill_map:
            if skill_lower in found:
                continue
            escaped = re.escape(skill_lower)
            if re.search(rf"(?<![a-z0-9.]){escaped}(?![a-z0-9.])", query_lower):
                found.append(skill_lower)
        return found

    def _is_simple(self, query: str) -> bool:
        """
        @brief Return True if the query can be fully processed by the fast-path.

        All five conditions must hold simultaneously:
          1. DB metadata was loaded successfully (skill_map is non-empty).
          2. Query is ≤ 8 words (single-intent heuristic).
          3. Exactly one known skill is detected.
          4. No ambiguity or advisory tokens are present.
          5. Query is not an open statistical question.

        @param query  Raw user query string.
        @return       True if the fast-path should handle this query.
        """
        if not self._skill_map:
            return False

        q = query.lower().strip()

        if len(q.split()) > 8:
            return False

        if len(self._detect_skills(q)) != 1:
            return False

        if any(token in q for token in _AMBIGUITY_TOKENS):
            return False

        if any(token in q for token in _QUESTION_TOKENS):
            return False

        return True

    def _fast_process(self, query: str) -> list[str]:
        """
        @brief Build 3 query variants from extracted fields using data-driven role patterns.

        Extracts skill, level prefix, location, and work mode from the query
        via regex + lookup tables, then generates one variant per role pattern
        loaded from the Silver layer at startup.  Zero LLM calls.

        @param query  Simple query string that passed _is_simple().
        @return       Deduplicated list of query variants with the original at index 0.
        """
        q = query.lower()

        # Skill (guaranteed by _is_simple)
        skill_lower = self._detect_skills(q)[0]
        skill = self._skill_map[skill_lower]          # original casing

        # Level prefix (e.g. "senior ")
        level_prefix = ""
        for token, normalized in _LEVEL_MAP.items():
            if re.search(rf"\b{re.escape(token)}\b", q):
                level_prefix = normalized + " "
                break

        # Location
        location = ""
        location_short = ""
        for alias, canonical in self._location_lookup.items():
            if alias in q:
                location = canonical
                location_short = _LOCATION_SHORT.get(canonical, canonical)
                break

        # Work mode (only if not already captured as location)
        mode = ""
        for m in self._mode_set:
            if m in q and m != location.lower():
                mode = m
                break

        def _join(*parts: str) -> str:
            return " ".join(p for p in parts if p).strip()

        suffix       = _join(location, mode)
        suffix_short = _join(location_short, mode)

        # Build 3 variants using top role patterns from silver layer
        variants: list[str] = []
        for i, pattern in enumerate(self._role_patterns[:3]):
            loc = suffix_short if i == 2 else suffix
            variants.append(_join(level_prefix + skill, pattern, loc))

        # Deduplicate, original query always first
        seen: set[str] = set()
        result: list[str] = []
        for s in [query, *variants]:
            if s not in seen:
                seen.add(s)
                result.append(s)

        logger.info("[FAST] '%s...' → %d variants: %s", query[:50], len(result), result)
        return result

    # ── Slow-path (unchanged from original) ──────────────────────────────────

    def _call(self, system: str, user: str, max_tokens: int = 256) -> list[str]:
        """
        @brief Make a single LLM call expecting a JSON array of strings as the response.

        Falls back to [user] (the original input) if the response cannot be parsed.

        @param system      System prompt for the LLM call.
        @param user        User-turn content (the query to decompose or expand).
        @param max_tokens  Token budget for the LLM response (default 256).
        @return            Parsed list of strings, or [user] on parse failure.
        """
        response = self._client.messages.create(
            model=settings.anthropic_model,
            max_tokens=max_tokens,
            system=system,
            messages=[
                {"role": "user", "content": user},
            ],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw = "\n".join(lines[1:-1]).strip()
        try:
            result = json.loads(raw)
            if isinstance(result, list) and all(isinstance(s, str) for s in result):
                return [s.strip() for s in result if s.strip()]
        except (json.JSONDecodeError, ValueError):
            pass
        logger.warning("Failed to parse LLM JSON response: %r", raw[:120])
        return [user]

    def _decompose(self, query: str) -> list[str]:
        """
        @brief Split a complex query into 2-3 focused sub-queries via LLM.

        @param query  Complex or multi-intent query string.
        @return       List of sub-query strings (1 element if query is single-intent).
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
        @brief Generate 2 semantic variants of a query (synonyms/paraphrases) via LLM.

        @param query  Sub-query string to expand.
        @return       List of 3 strings: [original, variant_1, variant_2].
        """
        variants = self._call(_EXPAND_SYSTEM, query, max_tokens=200)
        if query not in variants:
            variants.insert(0, query)
        logger.info("Expanded '%s...' → %d variants", query[:50], len(variants))
        return variants

    def _slow_process(self, query: str) -> list[str]:
        """
        @brief Full slow-path: decompose the query then expand each sub-query.

        @param query  Complex or ambiguous query string.
        @return       Deduplicated list of all expanded variants with the original at index 0.
        """
        sub_queries = self._decompose(query)
        all_queries: list[str] = []
        for sq in sub_queries:
            all_queries.extend(self._expand(sq))

        seen: set[str] = set()
        result: list[str] = []
        for s in [query, *all_queries]:
            if s not in seen:
                seen.add(s)
                result.append(s)

        logger.info("[SLOW] '%s...' → %d final queries", query[:50], len(result))
        return result

    # ── Public API ────────────────────────────────────────────────────────────

    def process(self, query: str) -> list[str]:
        """
        @brief Route the query to fast-path or slow-path and return the expanded query list.

        The returned list is passed directly to HybridSearchService.search_multi().
        The original query is always at index 0 so the cross-encoder reranker
        evaluates candidates against the true user intent.

        @param query  Resolved, standalone user query string.
        @return       Deduplicated list of query variants; queries[0] is the original.
        """
        if self._is_simple(query):
            return self._fast_process(query)
        return self._slow_process(query)
