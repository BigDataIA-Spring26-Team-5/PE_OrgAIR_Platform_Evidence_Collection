"""
Assessment Router - PE Org-AI-R Platform
app/routers/assessments.py

Handles assessment CRUD operations with Snowflake storage and Redis caching.
"""

from datetime import datetime, timezone
from typing import Dict, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse

from app.core.dependencies import get_assessment_repository, get_company_repository
from app.models.assessment import (
    AssessmentCreate,
    AssessmentResponse,
    ErrorResponse,
    PaginatedAssessmentResponse,
    StatusUpdate,
)
from app.models.enumerations import AssessmentStatus, AssessmentType
from app.repositories.assessment_repository import AssessmentRepository
from app.repositories.company_repository import CompanyRepository
from app.services.cache import get_cache, TTL_ASSESSMENT

router = APIRouter(prefix="/api/v1/assessments", tags=["Assessments"])




#  Custom Exception Handlers 
# Add these to your main.py: app.add_exception_handler(RequestValidationError, validation_exception_handler)

FIELD_MESSAGES = {
    "company_id": {
        "missing": "Company ID is required",
        "uuid_parsing": "Company ID must be a valid UUID format",
        "uuid_type": "Company ID must be a valid UUID",
    },
    "assessment_type": {
        "missing": "Assessment type is required",
        "enum": "Assessment type must be one of: screening, due_diligence, quarterly, exit_prep",
    },
    "assessment_date": {
        "missing": "Assessment date is required",
        "date_parsing": "Assessment date must be a valid date format (YYYY-MM-DD)",
        "date_type": "Assessment date must be a valid date",
    },
    "assessment_id": {
        "uuid_parsing": "Assessment ID must be a valid UUID format",
        "uuid_type": "Assessment ID must be a valid UUID",
    },
    "status": {
        "missing": "Status is required",
        "enum": "Status must be one of: draft, in_progress, submitted, approved, superseded",
    },
    "primary_assessor": {
        "string_too_long": "Primary assessor name must not exceed 255 characters",
        "string_type": "Primary assessor must be a string",
    },
    "secondary_assessor": {
        "string_too_long": "Secondary assessor name must not exceed 255 characters",
        "string_type": "Secondary assessor must be a string",
    },
    "page": {
        "greater_than_equal": "Page must be greater than or equal to 1",
        "int_type": "Page must be an integer",
        "int_parsing": "Page must be a valid integer",
    },
    "page_size": {
        "greater_than_equal": "Page size must be greater than or equal to 1",
        "less_than_equal": "Page size must not exceed 100",
        "int_type": "Page size must be an integer",
        "int_parsing": "Page size must be a valid integer",
    },
}

DEFAULT_MESSAGES = {
    "missing": "Field '{field}' is required",
    "string_too_short": "Field '{field}' is too short",
    "string_too_long": "Field '{field}' is too long",
    "string_pattern_mismatch": "Field '{field}' has invalid format",
    "less_than_equal": "Field '{field}' exceeds maximum allowed value",
    "greater_than_equal": "Field '{field}' is below minimum allowed value",
    "uuid_parsing": "Field '{field}' must be a valid UUID",
    "uuid_type": "Field '{field}' must be a valid UUID",
    "string_type": "Field '{field}' must be a string",
    "date_parsing": "Field '{field}' must be a valid date",
    "date_type": "Field '{field}' must be a valid date",
    "int_type": "Field '{field}' must be an integer",
    "int_parsing": "Field '{field}' must be a valid integer",
    "enum": "Field '{field}' has an invalid value",
    "json_invalid": "Malformed JSON request body",
    "extra_forbidden": "Unknown field '{field}' is not allowed",
}


def get_validation_message(field: str, error_type: str) -> str:
    if field in FIELD_MESSAGES:
        field_msgs = FIELD_MESSAGES[field]
        for key in field_msgs:
            if key in error_type:
                return field_msgs[key]

    for key, template in DEFAULT_MESSAGES.items():
        if key in error_type:
            return template.format(field=field)

    return f"Invalid value for field '{field}'"


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()

    if not errors:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error_code": "VALIDATION_ERROR",
                "message": "Request validation failed",
                "details": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    err = errors[0]
    error_type = err.get("type", "")
    loc = err.get("loc", [])

    if "json_invalid" in error_type:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "error_code": "INVALID_REQUEST",
                "message": "Malformed JSON request body",
                "details": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    field = ".".join(str(l) for l in loc if l != "body")
    message = get_validation_message(field, error_type)

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": message,
            "details": {"field": field, "type": error_type} if field else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


#  Exception Helpers 

def raise_error(status_code: int, error_code: str, message: str):
    raise HTTPException(
        status_code=status_code,
        detail=ErrorResponse(
            error_code=error_code,
            message=message,
            timestamp=datetime.now(timezone.utc)
        ).model_dump(mode="json")
    )


def raise_bad_request(msg: str = "Malformed JSON request"):
    raise_error(status.HTTP_400_BAD_REQUEST, "INVALID_REQUEST", msg)


def raise_assessment_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "ASSESSMENT_NOT_FOUND", "Assessment not found")


def raise_company_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "COMPANY_NOT_FOUND", "Company does not exist")


def raise_validation_error(msg: str):
    raise_error(status.HTTP_422_UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", msg)


def raise_internal_error():
    raise_error(status.HTTP_500_INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "Unexpected server error")


def invalidate_assessment_cache(assessment_id: UUID):
    """Invalidate assessment cache entries in Redis."""
    cache = get_cache()
    if cache:
        try:
            cache.delete(f"assessment:{assessment_id}")
        except Exception:
            pass  # Log error but don't fail the request


#  Routes 

@router.post(
    "",
    response_model=AssessmentResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Invalid request",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INVALID_REQUEST",
                        "message": "Malformed JSON request body",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Company not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "COMPANY_NOT_FOUND",
                        "message": "Company does not exist",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Company ID must be a valid UUID format",
                        "details": {"field": "company_id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Create a new assessment",
    description="Creates a new assessment for a company. Validates that the company exists and sets initial status to 'draft'.",
)
async def create_assessment(
    payload: AssessmentCreate,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> AssessmentResponse:
    # Company existence check
    if not company_repo.exists_active(payload.company_id):
        raise_company_not_found()

    # Create assessment in Snowflake
    assessment_data = assessment_repo.create(
        company_id=payload.company_id,
        assessment_type=payload.assessment_type,
        assessment_date=payload.assessment_date,
        primary_assessor=payload.primary_assessor,
        secondary_assessor=payload.secondary_assessor,
    )

    return AssessmentResponse(**assessment_data)


@router.get(
    "",
    response_model=PaginatedAssessmentResponse,
    responses={
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Page must be greater than or equal to 1",
                        "details": {"field": "page", "type": "greater_than_equal"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="List assessments",
    description="Returns a paginated list of assessments with optional filtering by company_id, assessment_type, and status.",
)
async def list_assessments(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    company_id: Optional[UUID] = Query(default=None),
    assessment_type: Optional[AssessmentType] = Query(default=None),
    status_filter: Optional[AssessmentStatus] = Query(default=None, alias="status"),
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> PaginatedAssessmentResponse:
    # Fetch from Snowflake with filters
    assessments, total = assessment_repo.get_all(
        page=page,
        page_size=page_size,
        company_id=company_id,
        assessment_type=assessment_type,
        status=status_filter,
    )

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    return PaginatedAssessmentResponse(
        items=[AssessmentResponse(**a) for a in assessments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get(
    "/{assessment_id}",
    response_model=AssessmentResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Assessment not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "ASSESSMENT_NOT_FOUND",
                        "message": "Assessment not found",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Assessment ID must be a valid UUID format",
                        "details": {"field": "assessment_id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Get assessment by ID",
    description="Retrieves a single assessment by its UUID, including associated dimension scores.",
)
async def get_assessment(
    assessment_id: UUID,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> AssessmentResponse:
    cache_key = f"assessment:{assessment_id}"
    cache = get_cache()

    # Try cache first (with graceful failure)
    if cache:
        try:
            cached = cache.get(cache_key, AssessmentResponse)
            if cached:
                return cached  # Cache hit!
        except Exception:
            pass  # Redis failed, continue to database

    # Cache miss - query Snowflake
    assessment_data = assessment_repo.get_by_id(assessment_id)

    if not assessment_data:
        raise_assessment_not_found()

    assessment = AssessmentResponse(**assessment_data)

    # Cache the result
    if cache:
        try:
            cache.set(cache_key, assessment, TTL_ASSESSMENT)
        except Exception:
            pass  # Don't fail if cache write fails

    return assessment


@router.patch(
    "/{assessment_id}/status",
    response_model=AssessmentResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Assessment not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "ASSESSMENT_NOT_FOUND",
                        "message": "Assessment not found",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Status must be one of: draft, in_progress, submitted, approved, superseded",
                        "details": {"field": "status", "type": "enum"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Update assessment status",
    description="Updates the status of an existing assessment. Valid transitions depend on current status.",
)
async def update_assessment_status(
    assessment_id: UUID,
    payload: StatusUpdate,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> AssessmentResponse:
    # Check if assessment exists
    if not assessment_repo.exists(assessment_id):
        raise_assessment_not_found()

    # Update status in Snowflake
    updated_data = assessment_repo.update_status(assessment_id, payload.status)

    # Invalidate cache after update
    invalidate_assessment_cache(assessment_id)

    return AssessmentResponse(**updated_data)
