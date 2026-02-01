from pydantic import BaseModel, Field
from uuid import UUID, uuid4
from datetime import datetime, timezone


class IndustryBase(BaseModel):
    """
    Base Pydantic model for Industry.
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Industry name (e.g., Manufacturing, Healthcare Services)"
    )

    sector: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="High-level sector classification"
    )

    h_r_base: float = Field(
        ...,
        ge=0,
        le=100,
        description="Baseline human-readiness score (0-100)"
    )


class IndustryCreate(IndustryBase):
    """
    Model for creating a new industry.
    """
    pass


class IndustryResponse(IndustryBase):
    """
    Model returned in API responses.
    """

    id: UUID = Field(
        default_factory=uuid4,
        description="Unique industry identifier"
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Record creation timestamp"
    )

    class Config:
        from_attributes = True
