from __future__ import annotations

from enum import Enum
from typing import Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field, model_validator


class SignalCategory(str, Enum):
    regulatory_claims = "regulatory_claims"     # SEC filings
    job_market = "job_market"                   # Job postings
    digital_presence = "digital_presence"       # Tech stack
    innovation_activity = "innovation_activity" # Patents


class SignalSource(str, Enum):
    sec = "sec"
    linkedin = "linkedin"
    naukri = "naukri"
    builtwith = "builtwith"
    similartech = "similartech"
    uspto = "uspto"
    other = "other"


class ExternalSignal(BaseModel):
    """
    Represents one scored signal from an external source.
    Maps to the `external_signals` table.
    """
    signal_id: str
    company_id: str
    category: SignalCategory
    source: SignalSource

    score: float = Field(..., ge=0, le=100)
    evidence_count: int = Field(0, ge=0)

    summary: Optional[str] = None
    raw_payload: Optional[dict] = None

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class CompanySignalSummary(BaseModel):
    """
    Aggregated company-level scores.
    Maps to `company_signal_summaries`.
    """
    company_id: str

    regulatory_claims_score: float = Field(0, ge=0, le=100)
    job_market_score: float = Field(0, ge=0, le=100)
    digital_presence_score: float = Field(0, ge=0, le=100)
    innovation_activity_score: float = Field(0, ge=0, le=100)

    composite_score: float = Field(0, ge=0, le=100)
    gap_score: float = Field(0, ge=0, le=100)

    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @model_validator(mode="after")
    def compute_scores(self):
        """
        Composite score weights:
        - SEC claims: 30%
        - Jobs: 25%
        - Tech: 25%
        - Patents: 20%
        """
        composite = (
            0.30 * self.regulatory_claims_score
            + 0.25 * self.job_market_score
            + 0.25 * self.digital_presence_score
            + 0.20 * self.innovation_activity_score
        )
        self.composite_score = round(composite, 2)

        # Gap = claims minus actual capability
        reality_avg = (
            self.job_market_score
            + self.digital_presence_score
            + self.innovation_activity_score
        ) / 3.0

        self.gap_score = round(
            max(0.0, self.regulatory_claims_score - reality_avg),
            2
        )
        return self
