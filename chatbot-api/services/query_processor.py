"""
Pre-retrieval query enhancement — Fast-path / Slow-path architecture.

Fast-path (0 LLM calls): simple, skill-specific queries.
    • All metadata loaded from Trino ONCE at startup
      (dim_skill, dim_location, dim_work_mode, silver job titles)
    • Fields extracted via regex + lookup sets in <1ms
    • 3 query variants built from role-pattern templates derived from actual data

Slow-path (2 LLM calls): complex / ambiguous / multi-intent queries.
    • Unchanged: LLM Decompose → LLM Expand per sub-query

Routing: _is_simple() checks 4 deterministic conditions, no LLM involved.
"""

import json
import logging
import re

import trino
from openai import OpenAI

from config import settings

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

# User-typed location aliases → canonical city name stored in dim_location.
# dim_location only stores canonical forms ("Ho Chi Minh City"), so the informal
# aliases ("hcm", "sài gòn", …) cannot be derived from DB and are listed here.
_LOCATION_ALIASES: dict[str, str] = {
    "hcm": "Ho Chi Minh City",
    "tphcm": "Ho Chi Minh City",
    "hồ chí minh": "Ho Chi Minh City",
    "ho chi minh": "Ho Chi Minh City",
    "saigon": "Ho Chi Minh City",
    "sài gòn": "Ho Chi Minh City",
    "hn": "Ha Noi",
    "hà nội": "Ha Noi",
    "ha noi": "Ha Noi",
    "hanoi": "Ha Noi",
    "đà nẵng": "Da Nang",
    "da nang": "Da Nang",
    "remote": "remote",
    "wfh": "remote",
    "từ xa": "remote",
}

# Shorter aliases used in the 3rd query variant to widen vocabulary coverage.
_LOCATION_SHORT: dict[str, str] = {
    "Ho Chi Minh City": "HCMC",
    "Ha Noi": "Hanoi",
    "Da Nang": "Da Nang",
    "remote": "remote",
}

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
         "experienced ReactJS engineer HCMC JavaScript"]"""


# ── QueryProcessor ────────────────────────────────────────────────────────────

class QueryProcessor:
    """
    Routes each query to fast-path (0 LLM calls) or slow-path (2 LLM calls).

    Fast-path metadata is loaded from Trino once in __init__. If Trino is
    unavailable at startup, _skill_map is empty and every query falls to
    slow-path gracefully — no crash, no degraded results.
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

        # Fast-path state (populated by _load_db_metadata)
        self._skill_map: dict[str, str] = {}       # lowercase → original casing
        self._location_lookup: dict[str, str] = {} # alias/canonical → canonical
        self._mode_set: frozenset[str] = frozenset()
        self._role_patterns: list[str] = []        # top role suffixes from data

        self._load_db_metadata()

    # ── DB metadata loader ────────────────────────────────────────────────────

    def _query_trino(self, sql: str) -> list[tuple]:
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
            # _LOCATION_ALIASES takes priority (more specific user-typed variants)
            self._location_lookup = {**canonical, **_LOCATION_ALIASES}

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
        """Return lowercase skill keys found in the query (word-boundary aware)."""
        found = []
        for skill_lower in self._skill_map:
            escaped = re.escape(skill_lower)
            # Handles dotted/special names like "node.js", "c#", "vue.js"
            if re.search(rf"(?<![a-z0-9.]){escaped}(?![a-z0-9.])", query_lower):
                found.append(skill_lower)
        return found

    def _is_simple(self, query: str) -> bool:
        """
        Return True iff the query can be fully processed by the fast-path.

        Conditions (ALL must pass):
          1. Metadata was loaded successfully
          2. ≤ 8 words (short = single intent)
          3. Exactly 1 known skill detected
          4. No ambiguity/advisory tokens
          5. Not an open question
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
        Build 3 query variants from extracted fields + data-driven role patterns.
        Zero LLM calls.
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
        response = self._client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw = response.choices[0].message.content.strip()
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
        sub_queries = self._call(_DECOMPOSE_SYSTEM, query, max_tokens=200)
        if len(sub_queries) > 1:
            logger.info(
                "Decomposed '%s...' → %d sub-queries: %s",
                query[:50], len(sub_queries), sub_queries,
            )
        return sub_queries

    def _expand(self, query: str) -> list[str]:
        variants = self._call(_EXPAND_SYSTEM, query, max_tokens=200)
        if query not in variants:
            variants.insert(0, query)
        logger.info("Expanded '%s...' → %d variants", query[:50], len(variants))
        return variants

    def _slow_process(self, query: str) -> list[str]:
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
        Entry point: route to fast-path or slow-path, return deduplicated
        query list for HybridSearchService.search_multi().
        Original query is always index 0 (used by reranker for intent alignment).
        """
        if self._is_simple(query):
            return self._fast_process(query)
        return self._slow_process(query)
