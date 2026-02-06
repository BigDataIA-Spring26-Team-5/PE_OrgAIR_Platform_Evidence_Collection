from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from enum import Enum


class DocumentEvidence(BaseModel):
    """A single SEC filing document."""
    id: str
    filing_type: str
    filing_date: Optional[date] = None
    source_url: Optional[str] = None
    s3_key: Optional[str] = None
    word_count: Optional[int] = None
    chunk_count: Optional[int] = None
    status: str
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None


class SignalEvidence(BaseModel):
    """A single external signal observation."""
    id: str
    category: str
    source: str
    signal_date: Optional[datetime] = None
    raw_value: Optional[str] = None
    normalized_score: Optional[float] = None
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


class SignalSummary(BaseModel):
    """Aggregated signal scores for a company."""
    technology_hiring_score: Optional[float] = None
    innovation_activity_score: Optional[float] = None
    digital_presence_score: Optional[float] = None
    leadership_signals_score: Optional[float] = None
    composite_score: Optional[float] = None
    signal_count: int = 0
    last_updated: Optional[datetime] = None


class CompanyEvidenceResponse(BaseModel):
    """Combined evidence response for a company."""
    company_id: str
    company_name: str
    ticker: str
    documents: List[DocumentEvidence] = []
    document_count: int = 0
    signals: List[SignalEvidence] = []
    signal_count: int = 0
    signal_summary: Optional[SignalSummary] = None


# =============================================================================
# Backfill Models
# =============================================================================

class BackfillStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class BackfillResponse(BaseModel):
    """Returned immediately when a backfill is triggered."""
    task_id: str
    status: BackfillStatus
    message: str


class CompanyBackfillResult(BaseModel):
    """Result of backfill for a single company."""
    ticker: str
    status: str
    sec_result: Optional[Dict[str, Any]] = None
    signal_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class BackfillProgress(BaseModel):
    """Progress info for a backfill task."""
    companies_completed: int = 0
    total_companies: int = 0
    current_company: Optional[str] = None


class BackfillTaskStatus(BaseModel):
    """Full status response for a backfill task."""
    task_id: str
    status: BackfillStatus
    progress: BackfillProgress
    company_results: List[CompanyBackfillResult] = []
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
