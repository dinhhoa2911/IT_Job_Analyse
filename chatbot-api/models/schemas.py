from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueryType(str, Enum):
    search_job   = "search_job"    # Tìm job cụ thể → Milvus hybrid search 
    analytics    = "analytics"     # Thống kê thị trường → Trino SQL
    career_advice = "career_advice" # Tư vấn nghề nghiệp IT → Claude (domain-restricted)
    out_of_scope = "out_of_scope"  # Ngoài phạm vi → từ chối, không gọi LLM


class ChatRequest(BaseModel):
    message:    str = Field(..., min_length=1, max_length=2000)
    session_id: str = Field(default="default", max_length=128)


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
