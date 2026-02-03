"""
Pipeline 2 Data Models - Job Postings Only
app/models/pipeline2_models.py
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List
from uuid import uuid4

from pydantic import BaseModel, Field


class JobPosting(BaseModel):
    """Individual job posting from JobSpy."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    company_id: str
    company_name: str
    title: str
    description: str
    location: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: str = "unknown"  # linkedin, indeed, etc.
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
