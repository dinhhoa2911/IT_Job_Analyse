"""
@file schemas.py
@brief Pydantic request/response schemas shared across routers and services.

Defines the data contracts for the chat endpoint, the CV-match endpoint, and
the internal pipeline types (QueryType, JobResult, MarketInsight).  All fields
carry validation constraints so FastAPI can enforce them at the HTTP boundary.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueryType(str, Enum):
    """
    @class QueryType
    @brief Enumeration of pipeline branches recognised by the query classifier.

    Each value maps to a distinct retrieval strategy inside RAGPipeline.run().
    """
    search_job     = "search_job"
    analytics      = "analytics"
    agent          = "agent"          # Agentic RAG — LLM selected multiple tools
    career_advice  = "career_advice"
    learning_path  = "learning_path"  # Data-driven skill learning roadmap (Gold layer)
    out_of_scope   = "out_of_scope"


class ChatRequest(BaseModel):
    """
    @class ChatRequest
    @brief Incoming payload for POST /chat.
    """
    message:         str = Field(..., min_length=1, max_length=2000)
    session_id:      str = Field(default="default", max_length=128)
    conversation_id: str = Field(default="", max_length=128)


class JobResult(BaseModel):
    """
    @class JobResult
    @brief A single job retrieved and ranked by the hybrid search pipeline.

    The ``score`` field holds the raw cross-encoder logit (unbounded); higher
    values indicate greater relevance.  Use CVMatcherService._sigmoid_norm()
    to convert to a 0-100 percentage for display.
    """
    job_id:       int
    job_title:    str
    job_link:     str
    text_content: str
    score:        float  # cross-encoder logit — unbounded, higher = more relevant


class MarketInsight(BaseModel):
    """
    @class MarketInsight
    @brief Real-time market intelligence pulled from the Gold layer (Trino).

    Attached to every search_job response so the frontend can render a market
    context card alongside the job list.  Populated by MarketContextService;
    absent (None) when Trino is unavailable or no primary skill is detected.
    """
    primary_skill:   str
    total_jobs:      int
    top_companies:   list[str]       = Field(default_factory=list)  # ["VNG (24)", "Tiki (18)"]
    work_mode_dist:  dict[str, int]  = Field(default_factory=dict)  # {"Remote": 45, "Hybrid": 30}
    related_skills:  list[str]       = Field(default_factory=list)  # ["FastAPI", "PostgreSQL"]
    location_filter:  Optional[str]   = None                         # "Ho Chi Minh" | None
    work_mode_filter: Optional[str]   = None                         # "At Office" | "Remote" | "Hybrid" | None
    level_filter:     Optional[str]   = None                         # "Senior" | "Junior" | None
    region_filter:    Optional[str]   = None                         # "South" | "North" | "Central" | None
    date_filter:      Optional[str]   = None                         # "2025" | "Q1/2025" | "tháng 3/2025" | None
    category_filter:  Optional[str]   = None                         # "Backend Development" | None
    company_filter:   Optional[str]   = None                         # "VNG" | None
    category_dist:    dict[str, int]  = Field(default_factory=dict)  # {"Backend Development": 120}
    region_dist:      dict[str, int]  = Field(default_factory=dict)  # {"South": 5264, "North": 3075}
    co_skills:            list[str]       = Field(default_factory=list)  # ["KUBERNETES"] when joint filter active
    or_skill_totals:      dict[str, int]  = Field(default_factory=dict)  # {"ORACLE": 294, "POSTGRESQL": 427} for OR queries
    related_skill_counts: dict[str, int]  = Field(default_factory=dict)  # {"C++": 98, "Linux": 45} co-occurrence counts


class ChatResponse(BaseModel):
    """
    @class ChatResponse
    @brief Full response returned by POST /chat.

    Fields are populated selectively based on ``query_type``:
      - search_job    → jobs + market_insight filled; sql_* are None.
      - analytics     → sql_query, sql_result, chart filled; jobs are None.
      - agent         → any combination; charts[] may contain multiple Chart.js specs.
      - career_advice / out_of_scope → only ``answer`` and ``query_type`` are set.
    """
    answer:              str
    query_type:          QueryType
    jobs:                Optional[list[JobResult]]       = None
    sql_query:           Optional[str]                   = None
    sql_result:          Optional[list[dict]]            = None
    chart:               Optional[dict]                  = None
    charts:              list[dict]                      = Field(default_factory=list)
    market_insight:      Optional[MarketInsight]         = None
    learning_path:       Optional["LearningPathResult"]  = None


# ── Learning Path schemas ─────────────────────────────────────────────────────

class LearningPathStep(BaseModel):
    """
    @class LearningPathStep
    @brief A single skill recommended in the learning roadmap.

    ``market_freq``  = % of target-category jobs requiring this skill.
    ``bridge_score`` = % of target-category jobs that require BOTH the user's
                       known skills AND this skill — measures how well the skill
                       connects to what the user already knows.
    ``rank_score``   = 0.6 × market_freq + 0.4 × bridge_score (composite ranking).
    """
    skill_name:   str
    skill_group:  str
    market_freq:  float  = Field(..., description="% of target-category jobs requiring this skill")
    bridge_score: float  = Field(..., description="% of target-category jobs needing known_skills + this skill")
    rank_score:   float  = Field(..., description="Composite score: 0.6×market_freq + 0.4×bridge_score")
    market_count: int    = Field(..., description="Raw count of distinct job postings requiring this skill")


class LearningPathResult(BaseModel):
    """
    @class LearningPathResult
    @brief Data-driven skill learning roadmap from the Gold lakehouse.

    ``steps`` are ordered by rank_score descending — learn step[0] first.
    When ``known_skills`` is empty, bridge_score is 0 for all steps and
    ranking falls back to market_freq only.
    """
    target_role:   str            = Field(..., description="Target role as stated by the user")
    role_category: str            = Field(..., description="Gold dim_job_category name inferred from target_role")
    total_jobs:    int            = Field(..., description="Total distinct jobs in the role category")
    known_skills:  list[str]      = Field(default_factory=list, description="Skills the user already has")
    steps:         list[LearningPathStep] = Field(
        default_factory=list,
        description="Top-10 skills to learn next, sorted by rank_score descending"
    )


# ── Skill Gap schemas ─────────────────────────────────────────────────────────

class SkillGap(BaseModel):
    """
    @class SkillGap
    @brief A single market skill with its frequency in the target role category.

    Returned as part of SkillGapAnalysis.  ``market_frequency`` is the percentage
    of distinct job postings in the role category that require this skill.
    """
    skill_name:        str
    market_frequency:  float   # % of jobs in the category requiring this skill
    job_count:         int     # raw count of distinct job_link requiring this skill
    skill_group:       str     # Backend | Frontend | Data & Cloud | Other


class SkillGapAnalysis(BaseModel):
    """
    @class SkillGapAnalysis
    @brief Skill gap report comparing a CVProfile against Gold-layer market data.

    Populated by SkillGapAnalyzerService.analyze() via Trino queries on the
    Gold star schema (fact_job_posting ⋈ dim_skill ⋈ dim_job_category).

    ``cv_coverage_score`` = (# of top-15 market skills found in CV) / 15 × 100
    ``present_skills``    = CV skills that appear in market's top-15
    ``missing_skills``    = top-10 most in-demand skills absent from the CV, sorted by
                            market_frequency descending — these are the skills the candidate
                            should prioritise learning to improve their market fit.
    ``all_market_skills`` = top-15 market skills for the inferred role category (for chart)
    """
    role_category:        str   = Field(..., description="Job category inferred from your CV (from Gold dim_job_category — reflects actual market data, not hard-coded names)")
    total_jobs_analyzed:  int   = Field(..., description="Total distinct job postings analysed for this category")
    cv_coverage_score:    float = Field(..., description="Percentage of top-15 market skills already present in your CV (0–100)")
    cv_skill_count:       int   = Field(..., description="Total number of skills extracted from your CV")
    present_skills:       list[str]      = Field(default_factory=list, description="Your CV skills that already appear in the market's top-15 — strengths to highlight")
    missing_skills:       list[SkillGap] = Field(default_factory=list, description="Top 10 most in-demand skills NOT found in your CV, sorted by market demand (highest first) — these are the skills you should learn/enhance to maximise job opportunities")
    all_market_skills:    list[SkillGap] = Field(default_factory=list, description="Top-15 market skills for this category (used for radar/bar charts)")


# ── CV Matching schemas ────────────────────────────────────────────────────────

class CVProfile(BaseModel):
    """
    @class CVProfile
    @brief Structured candidate profile extracted from a CV/resume PDF by CVProcessor.

    ``search_queries`` is generated by CVProcessor._build_search_queries() after
    LLM extraction; index 0 is always the richest semantic query used as the
    cross-encoder anchor in HybridSearchService.search_multi().
    """
    name:                Optional[str]       = None
    skills:              list[str]           = Field(default_factory=list)
    languages:           list[str]           = Field(default_factory=list)  # ["English (IELTS 6.0)"]
    experience_years:    Optional[int]       = None
    level:               Optional[str]       = None   # intern|junior|mid|senior|lead
    preferred_roles:     list[str]           = Field(default_factory=list)
    preferred_locations: list[str]           = Field(default_factory=list)
    work_mode:           Optional[str]       = None   # remote|hybrid|onsite
    education:           Optional[str]       = None
    summary_text:        str                 = ""     # LLM-generated search anchor
    search_queries:      list[str]           = Field(default_factory=list)
    # Snapshot of rule-filtered technical skills BEFORE language merge.
    # Used by the dim_skill gate so languages ("Vietnamese") cannot mask a non-IT CV.
    technical_skills:    list[str]           = Field(default_factory=list)


class MatchedJob(BaseModel):
    """
    @class MatchedJob
    @brief A job matched against a CV, annotated with relevance metadata.

    ``match_score`` is the sigmoid-normalised percentage (0-100) derived from
    the raw cross-encoder logit stored in ``raw_score``.
    ``matched_skills`` lists up to 8 CV skills found verbatim in the job text.
    """
    job_id:         int
    job_title:      str
    job_link:       str
    text_content:   str
    match_score:    float       # sigmoid-normalised 0–100
    raw_score:      float       # raw cross-encoder logit (unbounded)
    matched_skills: list[str]   # CV skills found in this job posting
    match_label:    str         # Excellent | Very Good | Good | Fair


class CVMatchResponse(BaseModel):
    """
    @class CVMatchResponse
    @brief Response returned by POST /match-cv.

    Contains the structured CV profile, the ranked list of matched jobs,
    total count, wall-clock processing time, a brief Vietnamese summary,
    and an optional skill gap analysis against the Gold lakehouse layer.
    """
    cv_profile:         CVProfile
    matched_jobs:       list[MatchedJob]
    total_found:        int
    processing_time_ms: int
    message:            str                          # brief Vietnamese summary
    skill_gaps:         list[SkillGapAnalysis]       = Field(default_factory=list)
