from pydantic import BaseModel, Field, model_validator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from enum import Enum


class SignalCategory(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class SignalSource(str, Enum):
    # Job Sources
    LINKEDIN = "linkedin"
    INDEED = "indeed"
    GLASSDOOR = "glassdoor"
    # Patent Source
    USPTO = "uspto"
    # Tech Stack Source
    BUILTWITH = "builtwith"
    # Other Sources
    PRESS_RELEASE = "press_release"
    COMPANY_WEBSITE = "company_website"
    # SEC Filing Source (for Leadership)
    SEC_FILING = "sec_filing"


class ExternalSignal(BaseModel):
    """A single external signal observation."""
    id: UUID = Field(default_factory=uuid4)
    company_id: UUID
    category: SignalCategory
    source: SignalSource
    signal_date: datetime
    raw_value: str  # Original observation summary
    normalized_score: float = Field(ge=0, le=100)
    confidence: float = Field(default=0.8, ge=0, le=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CompanySignalSummary(BaseModel):
    """Aggregated signals for a company."""
    company_id: UUID
    ticker: str
    technology_hiring_score: Optional[float] = Field(default=None, ge=0, le=100)
    innovation_activity_score: Optional[float] = Field(default=None, ge=0, le=100)
    digital_presence_score: Optional[float] = Field(default=None, ge=0, le=100)
    leadership_signals_score: Optional[float] = Field(default=None, ge=0, le=100)
    composite_score: Optional[float] = Field(default=None, ge=0, le=100)
    signal_count: int = 0
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode='after')
    def calculate_composite(self) -> 'CompanySignalSummary':
        """Calculate weighted composite score only if ALL 4 signals exist."""
        scores = [
            self.technology_hiring_score,
            self.innovation_activity_score,
            self.digital_presence_score,
            self.leadership_signals_score
        ]
        
        # Only calculate if all scores are present
        if all(s is not None for s in scores):
            self.composite_score = (
                0.30 * self.technology_hiring_score +
                0.25 * self.innovation_activity_score +
                0.25 * self.digital_presence_score +
                0.20 * self.leadership_signals_score
            )
        else:
            self.composite_score = None
            
        return self



# DATA MODELS (Moved from separate files)


class JobPosting(BaseModel):
    """Individual job posting from JobSpy."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    company_id: str
    company_name: str
    title: str
    description: str
    location: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: str = Field(default="unknown", description="linkedin, indeed, etc.")
    url: Optional[str] = None

    # Computed fields
    ai_keywords_found: List[str] = Field(default_factory=list)
    techstack_keywords_found: List[str] = Field(default_factory=list)
    is_ai_role: bool = False
    ai_score: float = Field(default=0.0, ge=0, le=100)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class Patent(BaseModel):
    """Individual patent from PatentsView API."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    company_id: str
    company_name: str
    patent_id: str
    patent_number: str
    title: str
    abstract: str = ""
    patent_date: Optional[datetime] = None
    patent_type: str = ""  # utility, design, plant, reissue
    assignees: List[str] = Field(default_factory=list)
    inventors: List[str] = Field(default_factory=list)
    cpc_codes: List[str] = Field(default_factory=list)  # CPC classification codes

    # Computed fields
    ai_keywords_found: List[str] = Field(default_factory=list)
    is_ai_patent: bool = False
    ai_score: float = Field(default=0.0, ge=0, le=100)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }



# JOB SIGNAL SPECIFIC MODELS


class JobScoreBreakdown(BaseModel):
    """Detailed breakdown of job market signal score."""
    ratio_score: float = Field(ge=0, le=40, description="AI jobs ratio component")
    volume_bonus: float = Field(ge=0, le=30, description="Volume bonus component")
    diversity_score: float = Field(ge=0, le=30, description="Keyword diversity component")
    total_score: float = Field(ge=0, le=100)


class JobAnalysisResult(BaseModel):
    """Result of job market analysis for a company."""
    ticker: str
    company_id: str
    total_jobs: int = Field(ge=0)
    ai_jobs: int = Field(ge=0)
    normalized_score: float = Field(ge=0, le=100)
    confidence: float = Field(ge=0, le=1)
    breakdown: JobScoreBreakdown
    ai_keywords_found: List[str]
    sources: List[str]
    job_postings_analyzed: int = Field(ge=0)
    collection_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class JobAnalysisResponse(BaseModel):
    """API response for job market analysis."""
    ticker: str
    status: str
    signals_created: int = Field(ge=0)
    summary_updated: bool
    result: Optional[JobAnalysisResult] = None



# TECH SIGNAL SPECIFIC MODELS


class TechScoreBreakdown(BaseModel):
    """Detailed breakdown of tech stack signal score."""
    base_score: float = Field(ge=0, le=50, description="AI tools ratio component")
    volume_bonus: float = Field(ge=0, le=30, description="Tech stack volume bonus")
    top_tools_bonus: float = Field(ge=0, le=20, description="Top AI tools bonus")
    total_score: float = Field(ge=0, le=100)


class TechAnalysisResult(BaseModel):
    """Result of tech stack analysis for a company."""
    ticker: str
    company_id: str
    normalized_score: float = Field(ge=0, le=100)
    confidence: float = Field(ge=0, le=1)
    breakdown: TechScoreBreakdown
    techstack_keywords: List[str]
    ai_tools_found: List[str]
    top_tech_keywords: List[str]
    data_source: str = Field(default="job_postings", description="Source of tech stack data")
    collection_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TechAnalysisResponse(BaseModel):
    """API response for tech stack analysis."""
    ticker: str
    status: str
    signals_created: int = Field(ge=0)
    summary_updated: bool
    result: Optional[TechAnalysisResult] = None



# PATENT SIGNAL SPECIFIC MODELS


class PatentScoreBreakdown(BaseModel):
    """Detailed breakdown of patent innovation signal score."""
    ratio_score: float = Field(ge=0, le=40, description="AI patents ratio component")
    volume_bonus: float = Field(ge=0, le=30, description="Patent volume bonus")
    recency_score: float = Field(ge=0, le=20, description="Recent patents bonus")
    diversity_score: float = Field(ge=0, le=10, description="Keyword diversity component")
    total_score: float = Field(ge=0, le=100)


class PatentAnalysisResult(BaseModel):
    """Result of patent analysis for a company."""
    ticker: str
    company_id: str
    total_patents: int = Field(ge=0)
    ai_patents: int = Field(ge=0)
    normalized_score: float = Field(ge=0, le=100)
    confidence: float = Field(ge=0, le=1)
    breakdown: PatentScoreBreakdown
    ai_keywords_found: List[str]
    cpc_codes: List[str]
    recent_patents: int = Field(ge=0, description="Patents from last 2 years")
    collection_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PatentAnalysisResponse(BaseModel):
    """API response for patent analysis."""
    ticker: str
    status: str
    signals_created: int = Field(ge=0)
    summary_updated: bool
    result: Optional[PatentAnalysisResult] = None



# LEADERSHIP SIGNAL SPECIFIC MODELS


class LeadershipScoreBreakdown(BaseModel):
    """Detailed breakdown of leadership signal score."""
    tech_exec_score: float = Field(ge=0, le=30)
    keyword_score: float = Field(ge=0, le=30)
    performance_metric_score: float = Field(ge=0, le=25)
    board_tech_score: float = Field(ge=0, le=15)
    total_score: float = Field(ge=0, le=100)


class LeadershipAnalysisResult(BaseModel):
    """Result of leadership analysis for a company."""
    ticker: str
    company_id: str
    filing_count_analyzed: int
    normalized_score: float
    confidence: float
    breakdown: LeadershipScoreBreakdown
    tech_execs_found: List[str]
    keyword_counts: Dict[str, int]
    tech_linked_metrics_found: List[str]
    board_tech_indicators: List[str]
    filing_dates: List[str]


class LeadershipAnalysisResponse(BaseModel):
    """API response for leadership analysis."""
    ticker: str
    status: str
    signals_created: int
    summary_updated: bool
    result: Optional[LeadershipAnalysisResult] = None



# COMBINED RESPONSE MODELS


class SignalSummaryResponse(BaseModel):
    """API response for signal summary table."""
    report_generated_at: datetime
    companies: List[Dict[str, Any]]


class CombinedAnalysisResponse(BaseModel):
    """Combined response for all signal analyses."""
    ticker: str
    status: str
    job_analysis: Optional[JobAnalysisResponse] = None
    tech_analysis: Optional[TechAnalysisResponse] = None
    patent_analysis: Optional[PatentAnalysisResponse] = None
    leadership_analysis: Optional[LeadershipAnalysisResponse] = None
    composite_score: Optional[float] = Field(default=None, ge=0, le=100)
    summary_updated: bool
