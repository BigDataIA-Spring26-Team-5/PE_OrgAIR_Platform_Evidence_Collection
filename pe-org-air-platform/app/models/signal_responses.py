"""
Signal API Response Models
app/models/signal_responses.py

Pydantic models for API responses from signals endpoints.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field



# Base Models for Reuse


class DateTimeConfigMixin:
    """Mixin for datetime JSON serialization configuration."""
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class CompanyInfoMixin(BaseModel):
    """Mixin for common company information fields."""
    company_id: str
    company_name: str


class UUIDCompanyInfoMixin(BaseModel):
    """Mixin for company information with UUID."""
    company_id: Optional[UUID] = None
    company_name: Optional[str] = None


class OptionalCompanyInfoMixin(BaseModel):
    """Mixin for optional company information fields."""
    company_id: Optional[str] = None
    company_name: Optional[str] = None


class AIScoreMixin(BaseModel):
    """Mixin for AI-related scoring fields."""
    ai_keywords_found: List[str] = Field(default_factory=list)
    is_ai_related: bool = False
    ai_score: float = Field(default=0.0, ge=0, le=100)


class TotalCountMixin(BaseModel):
    """Mixin for total and AI counts."""
    total_count: int = 0
    ai_count: int = 0



# Core Response Models


class JobPostingResponse(CompanyInfoMixin, AIScoreMixin, DateTimeConfigMixin):
    """Job posting response model for API."""
    id: str
    title: str
    description: str
    location: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: str = "unknown"
    url: Optional[str] = None
    techstack_keywords_found: List[str] = Field(default_factory=list)


class PatentResponse(CompanyInfoMixin, AIScoreMixin, DateTimeConfigMixin):
    """Patent response model for API."""
    id: str
    patent_id: str
    patent_number: str
    title: str
    abstract: str = ""
    patent_date: Optional[datetime] = None
    patent_type: str = ""
    assignees: List[str] = Field(default_factory=list)
    inventors: List[str] = Field(default_factory=list)
    cpc_codes: List[str] = Field(default_factory=list)


class TechStackResponse(CompanyInfoMixin):
    """Tech stack response model for API."""
    techstack_keywords: List[str] = Field(default_factory=list)
    ai_tools_found: List[str] = Field(default_factory=list)
    techstack_score: float = Field(default=0.0, ge=0, le=100)
    total_keywords: int = 0
    total_ai_tools: int = 0



# Pipeline and Error Models


class PipelineError(BaseModel):
    """Error details from pipeline execution."""
    step: str
    company_id: str
    error: str
    timestamp: Optional[str] = None



# Collection Response Models


class BaseCollectionResponse(OptionalCompanyInfoMixin):
    """Base model for collection responses."""
    pass


class JobPostingsResponse(BaseCollectionResponse, TotalCountMixin):
    """Response model for job postings endpoint."""
    job_market_score: Optional[float] = None
    job_postings: List[JobPostingResponse] = Field(default_factory=list)
    errors: List[PipelineError] = Field(default_factory=list)


class PatentsResponse(BaseCollectionResponse, TotalCountMixin):
    """Response model for patents endpoint."""
    patent_portfolio_score: Optional[float] = None
    patents: List[PatentResponse] = Field(default_factory=list)


class TechStacksResponse(BaseCollectionResponse):
    """Response model for tech stacks endpoint."""
    techstack_score: Optional[float] = None
    techstacks: List[TechStackResponse] = Field(default_factory=list)


class AllSignalsResponse(BaseCollectionResponse, TotalCountMixin):
    """Response model for all signals endpoint."""
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



# Request/Response models for POST /collect


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



# Summary and Score Models


class BaseScoreSummary(CompanyInfoMixin):
    """Base model for signal score summaries."""
    ticker: str = ""
    job_market_score: Optional[float] = None
    patent_portfolio_score: Optional[float] = None
    techstack_score: Optional[float] = None
    total_jobs: int = 0
    ai_jobs: int = 0
    total_patents: int = 0
    ai_patents: int = 0
    techstack_keywords: List[str] = Field(default_factory=list)


class StoredSignalSummary(BaseScoreSummary, DateTimeConfigMixin):
    """Summary of stored signals for a company."""
    collected_at: str


class SignalScoresResponse(BaseScoreSummary, DateTimeConfigMixin):
    """
    Response model for signal scores stored in Snowflake.
    
    Scores:
    - hiring_score: Job market/hiring signal (0-100)
    - innovation_score: Patent/innovation signal (0-100)
    - tech_stack_score: Tech stack signal (0-100)
    - leadership_score: Leadership signal (0-100) - optional, blank for now
    - composite_score: Weighted average of available scores
    """
    hiring_score: Optional[float] = Field(None, ge=0, le=100, description="Job market/hiring score")
    innovation_score: Optional[float] = Field(None, ge=0, le=100, description="Patent/innovation score")
    tech_stack_score: Optional[float] = Field(None, ge=0, le=100, description="Tech stack score")
    leadership_score: Optional[float] = Field(None, ge=0, le=100, description="Leadership score (blank for now)")
    composite_score: Optional[float] = Field(None, ge=0, le=100, description="Composite score")
    s3_jobs_key: Optional[str] = None
    s3_patents_key: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None