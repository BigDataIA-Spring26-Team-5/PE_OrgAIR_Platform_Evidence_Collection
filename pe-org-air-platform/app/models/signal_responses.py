"""
Signal API Response Models
app/models/signal_responses.py

Pydantic models for API responses from signals endpoints.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field


class JobPostingResponse(BaseModel):
    """Job posting response model for API."""
    id: str
    company_id: str
    company_name: str
    title: str
    description: str
    location: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: str = "unknown"
    url: Optional[str] = None
    ai_keywords_found: List[str] = Field(default_factory=list)
    techstack_keywords_found: List[str] = Field(default_factory=list)
    is_ai_role: bool = False
    ai_score: float = Field(default=0.0, ge=0, le=100)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class PatentResponse(BaseModel):
    """Patent response model for API."""
    id: str
    company_id: str
    company_name: str
    patent_id: str
    patent_number: str
    title: str
    abstract: str = ""
    patent_date: Optional[datetime] = None
    patent_type: str = ""
    assignees: List[str] = Field(default_factory=list)
    inventors: List[str] = Field(default_factory=list)
    cpc_codes: List[str] = Field(default_factory=list)
    ai_keywords_found: List[str] = Field(default_factory=list)
    is_ai_patent: bool = False
    ai_score: float = Field(default=0.0, ge=0, le=100)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class TechStackResponse(BaseModel):
    """Tech stack response model for API."""
    company_id: str
    company_name: str
    techstack_keywords: List[str] = Field(default_factory=list)
    ai_tools_found: List[str] = Field(default_factory=list)
    techstack_score: float = Field(default=0.0, ge=0, le=100)
    total_keywords: int = 0
    total_ai_tools: int = 0


class PipelineError(BaseModel):
    """Error details from pipeline execution."""
    step: str
    company_id: str
    error: str
    timestamp: Optional[str] = None


class JobPostingsResponse(BaseModel):
    """Response model for job postings endpoint."""
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    total_count: int
    ai_count: int
    job_market_score: Optional[float] = None
    job_postings: List[JobPostingResponse] = Field(default_factory=list)
    errors: List[PipelineError] = Field(default_factory=list)


class PatentsResponse(BaseModel):
    """Response model for patents endpoint."""
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    total_count: int
    ai_count: int
    patent_portfolio_score: Optional[float] = None
    patents: List[PatentResponse] = Field(default_factory=list)


class TechStacksResponse(BaseModel):
    """Response model for tech stacks endpoint."""
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    techstack_score: Optional[float] = None
    techstacks: List[TechStackResponse] = Field(default_factory=list)


class AllSignalsResponse(BaseModel):
    """Response model for all signals endpoint."""
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    job_market_score: Optional[float] = None
    patent_portfolio_score: Optional[float] = None
    techstack_score: Optional[float] = None
    total_jobs: int = 0
    ai_jobs: int = 0
    total_patents: int = 0
    ai_patents: int = 0
    job_postings: List[JobPostingResponse] = Field(default_factory=list)
    patents: List[PatentResponse] = Field(default_factory=list)
    techstacks: List[TechStackResponse] = Field(default_factory=list)


# ============================================
# Request/Response models for POST /collect
# ============================================

class SignalCollectRequest(BaseModel):
    """Request model for signal collection endpoint."""
    company_id: UUID = Field(..., description="Company ID from Snowflake database")
    collect_jobs: bool = Field(default=True, description="Collect job postings")
    collect_patents: bool = Field(default=True, description="Collect patents")
    patents_years_back: int = Field(default=5, ge=1, le=20, description="Years back to search for patents")


class SignalCollectResponse(BaseModel):
    """Response model for signal collection endpoint."""
    status: str = Field(..., description="Collection status: queued, completed, or failed")
    message: str = Field(..., description="Status message")
    company_id: Optional[UUID] = None
    company_name: Optional[str] = None
    data_path: Optional[str] = None


class StoredSignalSummary(BaseModel):
    """Summary of stored signals for a company."""
    company_id: str
    company_name: str
    collected_at: str
    total_jobs: int = 0
    ai_jobs: int = 0
    job_market_score: Optional[float] = None
    total_patents: int = 0
    ai_patents: int = 0
    patent_portfolio_score: Optional[float] = None
    techstack_score: Optional[float] = None
    techstack_keywords: List[str] = Field(default_factory=list)