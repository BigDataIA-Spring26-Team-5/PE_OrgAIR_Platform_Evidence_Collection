"""
Patent Signals Data Models
app/models/patent_signals.py
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
from uuid import uuid4

from pydantic import BaseModel, Field


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
