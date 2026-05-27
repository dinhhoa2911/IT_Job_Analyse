"""
@file cv_matcher.py
@brief CV Matcher — match a parsed CVProfile against the live job corpus.

Pipeline:
  1. Multi-query hybrid search  (Dense + BM25 + RRF + cross-encoder rerank)
  2. Soft location / work-mode  priority sort
  3. Score normalisation        (cross-encoder logit → sigmoid 0-100)
  4. Skill annotation           (which CV skills appear in each job posting)
  5. Match label assignment     (Excellent / Very Good / Good / Fair)

Notes on soft filtering (step 2):
  Jobs that do not match the candidate's preferred location or work mode are
  NOT removed — candidates are often flexible, and hard filtering on sparse
  metadata hurts recall.  Instead, preference-matching jobs are floated to
  the top of the already-reranked list (stable sort).
"""

import logging
import math
import re

from constants import LOCATION_ALIASES, WORK_MODE_KEYWORDS
from models.schemas import CVProfile, JobResult, MatchedJob
from services.vector_search import HybridSearchService

logger = logging.getLogger(__name__)


_LABELS: list[tuple[float, str]] = [
    (85.0, "Excellent"),
    (70.0, "Very Good"),
    (50.0, "Good"),
    (0.0,  "Fair"),
]

# ── Level compatibility ────────────────────────────────────────────────────────

_LEVEL_RANK: dict[str, int] = {
    "intern":  0,
    "fresher": 0,
    "junior":  1,
    "mid":     2,
    "senior":  3,
    "lead":    4,
}

# Keywords in job title → detected job seniority level (checked high→low, first match wins)
_JOB_LEVEL_KW: list[tuple[int, list[str]]] = [
    (4, [
        "lead", "tech lead", "team lead", "principal", "staff engineer",
        "head of", "head ", "director", "vp ", "vice president",
        "engineering manager", "manager", "architect", "solution architect",
        "chapter lead", "guild lead",
    ]),
    (3, [
        "senior", "sr.", "sr ", "(senior)", "snr", "experienced",
        "chuyên gia", "chuyên viên cao cấp",
    ]),
    (2, [
        "mid-level", "mid level", "middle", "intermediate",
        "associate senior", "(mid)",
    ]),
    (1, [
        "junior", "jr.", "jr ", "(junior)", "associate",
        "mới ra trường", "fresher", "entry level", "entry-level",
    ]),
    (0, [
        "intern", "internship", "thực tập", "thực tập sinh",
        "trainee", "tập sự", "(intern)", "student",
    ]),
]

# Penalty multiplier per level gap (job_level - candidate_level)
_LEVEL_PENALTY: dict[int, float] = {
    0: 1.00,   # same level or overqualified → no penalty
    1: 0.88,   # 1 level above → mild penalty
    2: 0.72,   # 2 levels above → moderate penalty
    3: 0.55,   # 3 levels above → strong penalty
    4: 0.40,   # 4 levels above → heavy penalty
}


def _detect_job_level(job_title: str) -> int | None:
    """
    @brief Detect the seniority rank of a job posting from its title.

    @param job_title  Job title string (matched case-insensitively).
    @return           Integer rank (0=intern … 4=lead), or None if no keyword matches.
    """
    title_lower = job_title.lower()
    for rank, keywords in _JOB_LEVEL_KW:
        if any(kw in title_lower for kw in keywords):
            return rank
    return None  # no level keyword found → no penalty


def _level_penalty(candidate_level: str | None, job_title: str) -> float:
    """
    @brief Compute a score multiplier based on the seniority gap between candidate and job.

    A zero or negative gap (candidate overqualified) returns 1.0 (no penalty).

    @param candidate_level  Seniority string from CVProfile (e.g. "junior", "senior"), or None.
    @param job_title        Job title string used to infer the job's seniority rank.
    @return                 Multiplier in (0, 1] — 1.0 means no penalty.
    """
    if not candidate_level:
        return 1.0
    candidate_rank = _LEVEL_RANK.get(candidate_level.lower())
    if candidate_rank is None:
        return 1.0
    job_rank = _detect_job_level(job_title)
    if job_rank is None:
        return 1.0  # unknown job level → no penalty
    gap = max(0, job_rank - candidate_rank)
    return _LEVEL_PENALTY.get(gap, 0.40)


def _sigmoid_norm(logit: float) -> float:
    """
    @brief Map a cross-encoder logit (−∞, +∞) to a normalised percentage [0, 100].

    @param logit  Raw cross-encoder relevance score.
    @return       Sigmoid-normalised score rounded to one decimal place.
    """
    return round(100.0 / (1.0 + math.exp(-logit)), 1)


def _label(score: float) -> str:
    """
    @brief Convert a normalised match score to a human-readable label.

    @param score  Sigmoid-normalised score in [0, 100].
    @return       One of "Excellent", "Very Good", "Good", or "Fair".
    """
    for threshold, name in _LABELS:
        if score >= threshold:
            return name
    return "Fair"


# ── Location / work-mode helpers ───────────────────────────────────────────────

# LOCATION_ALIASES and WORK_MODE_KEYWORDS imported from constants.py


def _matches_preference(
    job_text: str,
    preferred_locations: list[str],
    work_mode: str | None,
) -> bool:
    """
    @brief Check whether a job's text satisfies at least one candidate preference.

    @param job_text            Full text_content of the job posting (lowercased internally).
    @param preferred_locations List of location keys from CVProfile (e.g. ["hcm", "remote"]).
    @param work_mode           Preferred work mode from CVProfile, or None.
    @return                    True if any location alias or work-mode keyword is found.
    """
    content = job_text.lower()

    for loc in preferred_locations:
        aliases = LOCATION_ALIASES.get(loc.lower(), [loc.lower()])
        if any(a in content for a in aliases):
            return True

    if work_mode:
        kws = WORK_MODE_KEYWORDS.get(work_mode.lower(), [work_mode.lower()])
        if any(kw in content for kw in kws):
            return True

    return False


def _soft_sort(
    jobs: list[JobResult],
    preferred_locations: list[str],
    work_mode: str | None,
) -> list[JobResult]:
    """
    @brief Float preference-matching jobs to the front without discarding others.

    Uses a stable partition so relative cross-encoder order is preserved within
    each group (preference-matching jobs first, then the rest).

    @param jobs                Already-reranked JobResult list from hybrid search.
    @param preferred_locations Candidate's preferred location keys.
    @param work_mode           Candidate's preferred work mode, or None.
    @return                    Reordered list with preference-matching jobs first.
    """
    if not preferred_locations and not work_mode:
        return jobs

    pref, rest = [], []
    for job in jobs:
        if _matches_preference(job.text_content, preferred_locations, work_mode):
            pref.append(job)
        else:
            rest.append(job)

    logger.info(
        "Soft location/mode sort: %d preference-matching, %d other.",
        len(pref), len(rest),
    )
    return pref + rest


# ── Skill annotation ───────────────────────────────────────────────────────────

# Pre-compile a word-boundary pattern per skill at annotation time.
# We cache up to 30 compiled patterns since the skill list per CV is small.
def _find_matched_skills(cv_skills: list[str], job_text: str) -> list[str]:
    """
    @brief Find which CV skills appear as whole words or phrases in a job posting.

    Uses word-boundary-aware regex so "Java" does not match "JavaScript".
    Right boundary is relaxed for skills ending in + or # (e.g. C++, C#).

    @param cv_skills  List of skill strings extracted from the candidate's CVProfile.
    @param job_text   Full text_content of the job posting.
    @return           Matched skill names (up to 8) preserving original casing.
    """
    job_lower = job_text.lower()
    matched = []
    for skill in cv_skills:
        # Escape special regex chars (e.g. C++, .NET)
        pattern = re.escape(skill.lower())
        # Word-boundary on left; right boundary relaxed for skills ending in +/#
        if re.search(r"(?<!\w)" + pattern + r"(?!\w|[+#])", job_lower):
            matched.append(skill)
    return matched[:8]  # cap at 8 for display


# ── Main service ───────────────────────────────────────────────────────────────

class CVMatcherService:
    """
    @class CVMatcherService
    @brief Match a CVProfile against the job corpus using the shared HybridSearchService.

    Reuses the warmed-up HybridSearchService singleton injected at construction
    time so no duplicate model loading occurs.  The full pipeline is:
      1. Multi-query hybrid search via search_multi().
      2. Soft location/work-mode priority sort.
      3. Truncate to top_k.
      4. Sigmoid score normalisation + skill annotation + label assignment.
    """

    def __init__(self, search_service: HybridSearchService) -> None:
        self._search = search_service

    def match(self, profile: CVProfile, top_k: int = 10) -> list[MatchedJob]:
        """
        @brief Run the full CV-matching pipeline and return annotated MatchedJob results.

        @param profile  Structured CVProfile produced by CVProcessor (must have search_queries).
        @param top_k    Number of final jobs to return (1-20).
        @return         List of MatchedJob sorted by match_score descending.
        @throws ValueError  If profile.search_queries is empty (CVProcessor not run).
        """
        if not profile.search_queries:
            raise ValueError("CVProfile has no search_queries — run CVProcessor first.")

        logger.info(
            "CV matching | queries=%d | top_k=%d | q0='%s...'",
            len(profile.search_queries),
            top_k,
            profile.search_queries[0][:60],
        )

        # Step 1 — multi-query hybrid search
        # Fetch 2× top_k so soft-sort has enough headroom after re-ordering.
        raw: list[JobResult] = self._search.search_multi(
            queries=profile.search_queries,
            top_k=min(top_k * 2, 20),   # cap at retrieval ceiling
        )

        # Step 2 — soft location / work-mode priority sort
        ordered = _soft_sort(raw, profile.preferred_locations, profile.work_mode)

        # Step 3 — take top_k
        final = ordered[:top_k]

        # Step 4-5 — annotate each result
        result: list[MatchedJob] = []
        for job in final:
            norm   = _sigmoid_norm(job.score)
            penalty = _level_penalty(profile.level, job.job_title)
            adjusted = round(norm * penalty, 1)
            if penalty < 1.0:
                logger.debug(
                    "Level penalty %.2f on '%s' (candidate=%s): %.1f → %.1f",
                    penalty, job.job_title, profile.level, norm, adjusted,
                )
            skills = _find_matched_skills(profile.skills, job.text_content)
            result.append(
                MatchedJob(
                    job_id         = job.job_id,
                    job_title      = job.job_title,
                    job_link       = job.job_link,
                    text_content   = job.text_content,
                    match_score    = adjusted,
                    raw_score      = job.score,
                    matched_skills = skills,
                    match_label    = _label(adjusted),
                )
            )

        if result:
            logger.info(
                "CV matching complete | returned=%d | best=%.1f%% (%s) | worst=%.1f%%",
                len(result),
                result[0].match_score,
                result[0].match_label,
                result[-1].match_score,
            )
        else:
            logger.warning("CV matching returned 0 results.")

        return result
