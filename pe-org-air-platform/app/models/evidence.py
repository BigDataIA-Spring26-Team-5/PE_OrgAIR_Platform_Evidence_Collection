from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date


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
