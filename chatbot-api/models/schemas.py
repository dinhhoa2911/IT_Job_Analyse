from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueryType(str, Enum):
    search_job   = "search_job"
    analytics    = "analytics" 
    career_advice = "career_advice"
    out_of_scope = "out_of_scope"  


class ChatRequest(BaseModel):
    message:         str = Field(..., min_length=1, max_length=2000)
    session_id:      str = Field(default="default", max_length=128)
    conversation_id: str = Field(default="", max_length=128)


class JobResult(BaseModel):
    job_id:       int
    job_title:    str
    job_link:     str
    text_content: str
    score:        float  # cross-encoder logit — unbounded, higher = more relevant


class ChatResponse(BaseModel):
    answer:     str
    query_type: QueryType
    jobs:       Optional[list[JobResult]] = None
    sql_query:  Optional[str] = None
    sql_result: Optional[list[dict]] = None
    chart:      Optional[dict] = None  # Chart.js spec — None nếu không vẽ được
