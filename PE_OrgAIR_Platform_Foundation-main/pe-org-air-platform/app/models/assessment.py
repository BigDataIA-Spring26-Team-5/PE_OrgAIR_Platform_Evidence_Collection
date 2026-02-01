from pydantic import BaseModel, Field, model_validator
from uuid import UUID, uuid4
from datetime import date, datetime, timezone
from typing import Optional, List

from app.models.enumerations import AssessmentType, AssessmentStatus


class AssessmentBase(BaseModel):
    """
    Base Pydantic model for Assessment.
    """

    company_id: UUID = Field(
        ...,
        description="Foreign key reference to Company"
    )

    assessment_type: AssessmentType = Field(
        ...,
        description="Type of assessment (screening, due_diligence, quarterly, exit_prep)"
    )

    assessment_date: date = Field(
        ...,
        description="Date of the assessment"
    )

    primary_assessor: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Name of the primary assessor"
    )

    secondary_assessor: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Name of the secondary assessor"
    )


class AssessmentCreate(AssessmentBase):
    """
    Model for creating a new assessment.
    """
    pass


class AssessmentResponse(AssessmentBase):
    """
    Model returned in API responses.
    """

    id: UUID = Field(
        default_factory=uuid4,
        description="Unique assessment identifier"
    )

    status: AssessmentStatus = Field(
        default=AssessmentStatus.DRAFT,
        description="Current status of the assessment"
    )

    v_r_score: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Value-Readiness score (calculated in later case studies)"
    )

    confidence_lower: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Lower bound of confidence interval"
    )

    confidence_upper: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Upper bound of confidence interval"
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Record creation timestamp (UTC)"
    )

    @model_validator(mode="after")
    def validate_confidence_interval(self):
        """Ensure confidence_upper >= confidence_lower when both are set."""
        if self.confidence_lower is not None and self.confidence_upper is not None:
            if self.confidence_upper < self.confidence_lower:
                raise ValueError("confidence_upper must be >= confidence_lower")
        return self

    class Config:
        from_attributes = True


class StatusUpdate(BaseModel):
    """
    Model for updating assessment status.
    """

    status: AssessmentStatus = Field(
        ...,
        description="New status for the assessment"
    )


class PaginatedAssessmentResponse(BaseModel):
    """
    Paginated response for listing assessments.
    """

    items: List[AssessmentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int

class ErrorResponse(BaseModel):
    """
    Standard error response model.
    """

    error_code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[dict] = Field(default=None, description="Additional error details")
    timestamp: datetime = Field(..., description="Error occurrence timestamp")
