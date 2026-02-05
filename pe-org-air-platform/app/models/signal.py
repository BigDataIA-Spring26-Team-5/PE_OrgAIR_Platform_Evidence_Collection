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


# ============================================================
# LEADERSHIP SIGNAL SPECIFIC MODELS
# ============================================================

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


class SignalSummaryResponse(BaseModel):
    """API response for signal summary table."""
    report_generated_at: datetime
    companies: List[Dict[str, Any]]
