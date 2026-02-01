# app/routers/dimensionScores.py

from datetime import datetime
from typing import Dict, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator

from app.core.dependencies import get_dimension_score_repository
from app.models.dimension import (
    DIMENSION_WEIGHTS,
    DimensionScoreCreate,
    DimensionScoreResponse,
    DimensionScoreUpdate,
)
from app.models.enumerations import Dimension
from app.repositories.dimension_score_repository import DimensionScoreRepository
from app.services.cache import get_cache, TTL_ASSESSMENT, TTL_DIMENSION_WEIGHTS


# ROUTER CONFIGURATION


router = APIRouter(
    prefix="/api/v1",
    tags=["Dimension Scores"]
)



# ERROR RESPONSE SCHEMA WITH EXAMPLES


class ErrorResponse(BaseModel):
    """Standard error response schema"""
    error: str = Field(..., examples=["Bad Request"])
    message: str = Field(..., examples=["Invalid score data provided"])
    timestamp: datetime = Field(default_factory=datetime.utcnow, examples=["2024-01-15T10:30:00Z"])


class DimensionWeightsResponse(BaseModel):
    """Response schema for dimension weights configuration"""
    weights: Dict[str, float]
    total: float
    is_valid: bool
    timestamp: str



# RESPONSE EXAMPLES FOR SWAGGER UI


# POST endpoint response examples
POST_RESPONSES = {
    201: {
        "description": "Dimension score created successfully",
        "content": {
            "application/json": {
                "example": {
                    "id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
                    "assessment_id": "550e8400-e29b-41d4-a716-446655440000",
                    "dimension": "data_infrastructure",
                    "score": 85.5,
                    "weight": 0.25,
                    "confidence": 0.92,
                    "evidence_count": 5,
                    "created_at": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    400: {
        "description": "Invalid score data",
        "content": {
            "application/json": {
                "example": {
                    "error": "Bad Request",
                    "message": "Assessment ID in request body does not match path parameter",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    401: {
        "description": "Unauthorized - Authentication required",
        "content": {
            "application/json": {
                "example": {
                    "error": "Unauthorized",
                    "message": "Authentication token is missing or invalid",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    403: {
        "description": "Forbidden - Insufficient permissions",
        "content": {
            "application/json": {
                "example": {
                    "error": "Forbidden",
                    "message": "You do not have permission to add scores to this assessment",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    404: {
        "description": "Assessment not found",
        "content": {
            "application/json": {
                "example": {
                    "error": "Not Found",
                    "message": "Assessment with the specified ID not found",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    409: {
        "description": "Conflict - Score already exists for this dimension",
        "content": {
            "application/json": {
                "example": {
                    "error": "Conflict",
                    "message": "A score already exists for dimension 'data_infrastructure' in this assessment",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    422: {
        "description": "Validation Error - Invalid score value",
        "content": {
            "application/json": {
                "example": {
                    "error": "Unprocessable Entity",
                    "message": "Score value must be between 0 and 100",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    500: {
        "description": "Internal Server Error",
        "content": {
            "application/json": {
                "example": {
                    "error": "Internal Server Error",
                    "message": "Failed to add dimension score due to server error",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    }
}

# GET endpoint response examples
GET_RESPONSES = {
    200: {
        "description": "Dimension scores retrieved successfully",
        "content": {
            "application/json": {
                "example": [
                    {
                        "id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
                        "assessment_id": "550e8400-e29b-41d4-a716-446655440000",
                        "dimension": "data_infrastructure",
                        "score": 85.5,
                        "weight": 0.25,
                        "confidence": 0.92,
                        "evidence_count": 5,
                        "created_at": "2024-01-15T10:30:00Z"
                    },
                    {
                        "id": "b2c3d4e5-6789-01bc-def2-2345678901bc",
                        "assessment_id": "550e8400-e29b-41d4-a716-446655440000",
                        "dimension": "ai_governance",
                        "score": 72.0,
                        "weight": 0.20,
                        "confidence": 0.88,
                        "evidence_count": 3,
                        "created_at": "2024-01-15T10:35:00Z"
                    }
                ]
            }
        }
    },
    404: {
        "description": "Assessment not found or no scores available",
        "content": {
            "application/json": {
                "example": {
                    "error": "Not Found",
                    "message": "Assessment not found or no dimension scores available",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    500: {
        "description": "Internal Server Error",
        "content": {
            "application/json": {
                "example": {
                    "error": "Internal Server Error",
                    "message": "Failed to retrieve dimension scores due to server error",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    }
}

# PUT endpoint response examples
PUT_RESPONSES = {
    200: {
        "description": "Dimension score updated successfully",
        "content": {
            "application/json": {
                "example": {
                    "id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
                    "assessment_id": "550e8400-e29b-41d4-a716-446655440000",
                    "dimension": "data_infrastructure",
                    "score": 90.0,
                    "weight": 0.25,
                    "confidence": 0.95,
                    "evidence_count": 7,
                    "created_at": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    400: {
        "description": "Invalid update data",
        "content": {
            "application/json": {
                "example": {
                    "error": "Bad Request",
                    "message": "No valid fields provided for update",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    404: {
        "description": "Score not found",
        "content": {
            "application/json": {
                "example": {
                    "error": "Not Found",
                    "message": "Dimension score with the specified ID not found",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    422: {
        "description": "Validation Error - Invalid score value",
        "content": {
            "application/json": {
                "example": {
                    "error": "Unprocessable Entity",
                    "message": "Score value must be between 0 and 100",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    },
    500: {
        "description": "Internal Server Error",
        "content": {
            "application/json": {
                "example": {
                    "error": "Internal Server Error",
                    "message": "Failed to update dimension score due to server error",
                    "timestamp": "2024-01-15T10:30:00Z"
                }
            }
        }
    }
}



# PYDANTIC VALIDATOR: WEIGHTS SUM TO 1.0


class WeightValidationMixin(BaseModel):
    """Mixin class to validate that dimension weights sum to 1.0"""
    
    @model_validator(mode='after')
    def validate_weights_sum(self) -> 'WeightValidationMixin':
        total_weight = sum(DIMENSION_WEIGHTS.values())
        if not (0.999 <= total_weight <= 1.001):
            raise ValueError(f"Dimension weights must sum to 1.0, but got {total_weight}")
        return self



# BULK CREATE REQUEST SCHEMA WITH WEIGHT VALIDATION


class BulkDimensionScoreCreate(BaseModel):
    """Schema for bulk creating dimension scores with weight validation."""
    scores: List[DimensionScoreCreate] = Field(..., description="List of dimension scores to create")
    
    @model_validator(mode='after')
    def validate_custom_weights_sum(self) -> 'BulkDimensionScoreCreate':
        custom_weights = [score.weight for score in self.scores if score.weight is not None]
        if custom_weights:
            total_custom_weight = sum(custom_weights)
            if not (0.999 <= total_custom_weight <= 1.001):
                raise ValueError(f"Custom weights must sum to 1.0, but got {total_custom_weight}")
        return self



# HELPER FUNCTIONS


def validate_weights_sum_to_one():
    """Standalone function to validate DIMENSION_WEIGHTS sum to 1.0"""
    total = sum(DIMENSION_WEIGHTS.values())
    if not (0.999 <= total <= 1.001):
        raise ValueError(f"DIMENSION_WEIGHTS must sum to 1.0, but got {total}.")
    return True


# Validate weights sum to 1.0 at module load time
validate_weights_sum_to_one()


def invalidate_assessment_cache(assessment_id: UUID):
    """Invalidate assessment cache when scores are modified."""
    cache = get_cache()
    if cache:
        try:
            cache.delete(f"assessment:{assessment_id}")
        except Exception:
            pass  # Log error but don't fail the request



# API ENDPOINTS


@router.post(
    "/assessments/{id}/scores",
    response_model=DimensionScoreResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add dimension scores",
    description="Adds dimension scores to a specific assessment.",
    responses=POST_RESPONSES
)
async def add_dimension_score(
    id: UUID,
    score_data: DimensionScoreCreate,
    repo: DimensionScoreRepository = Depends(get_dimension_score_repository),
) -> DimensionScoreResponse:
    """Add a new dimension score to an assessment."""
    try:
        if score_data.assessment_id != id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Bad Request",
                    "message": "Assessment ID in request body does not match path parameter",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        # Check for existing dimension score
        if repo.check_dimension_exists(id, score_data.dimension):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "Conflict",
                    "message": f"A score already exists for dimension '{score_data.dimension.value}' in this assessment",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        # Create in Snowflake
        new_score = repo.create(
            assessment_id=score_data.assessment_id,
            dimension=score_data.dimension,
            score=score_data.score,
            weight=score_data.weight,
            confidence=score_data.confidence,
            evidence_count=score_data.evidence_count,
        )

        # Invalidate assessment cache (scores changed)
        invalidate_assessment_cache(id)

        return DimensionScoreResponse(**new_score)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": f"Failed to add dimension score due to server error: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@router.get(
    "/assessments/{id}/scores",
    response_model=List[DimensionScoreResponse],
    status_code=status.HTTP_200_OK,
    summary="Get all dimension scores",
    description="Retrieves all dimension scores for a specific assessment.",
    responses=GET_RESPONSES
)
async def get_dimension_scores(
    id: UUID,
    repo: DimensionScoreRepository = Depends(get_dimension_score_repository),
) -> List[DimensionScoreResponse]:
    """Retrieve all dimension scores for an assessment."""
    try:
        scores = repo.get_by_assessment_id(id)

        if not scores:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Not Found",
                    "message": "Assessment not found or no dimension scores available",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        return [DimensionScoreResponse(**score) for score in scores]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": f"Failed to retrieve dimension scores due to server error: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@router.put(
    "/scores/{id}",
    response_model=DimensionScoreResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a dimension score",
    description="Updates an existing dimension score by its ID.",
    responses=PUT_RESPONSES
)
async def update_dimension_score(
    id: UUID,
    update_data: DimensionScoreUpdate,
    repo: DimensionScoreRepository = Depends(get_dimension_score_repository),
) -> DimensionScoreResponse:
    """Update an existing dimension score."""
    try:
        # Check if score exists
        existing_score = repo.get_by_id(id)

        if not existing_score:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Not Found",
                    "message": "Dimension score with the specified ID not found",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        update_dict = update_data.model_dump(exclude_unset=True, exclude_none=True)

        if not update_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Bad Request",
                    "message": "No valid fields provided for update",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        # Update in Snowflake
        updated_score = repo.update(
            score_id=id,
            score=update_dict.get("score"),
            weight=update_dict.get("weight"),
            confidence=update_dict.get("confidence"),
            evidence_count=update_dict.get("evidence_count"),
            dimension=update_dict.get("dimension"),
        )

        # Invalidate assessment cache (scores changed)
        invalidate_assessment_cache(existing_score["assessment_id"])

        return DimensionScoreResponse(**updated_score)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": f"Failed to update dimension score due to server error: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@router.get(
    "/dimensions/weights",
    summary="Get dimension weights configuration",
    description="Returns the current dimension weights configuration and validates they sum to 1.0",
    responses={
        200: {
            "description": "Weights configuration retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "weights": {
                            "data_infrastructure": 0.25,
                            "ai_governance": 0.20,
                            "technology_stack": 0.15,
                            "talent_skills": 0.15,
                            "leadership_vision": 0.10,
                            "use_case_portfolio": 0.10,
                            "culture_change": 0.05
                        },
                        "total": 1.0,
                        "is_valid": True,
                        "timestamp": "2024-01-15T10:30:00Z"
                    }
                }
            }
        },
        500: {
            "description": "Internal Server Error - Weights misconfigured",
            "content": {
                "application/json": {
                    "example": {
                        "error": "Internal Server Error",
                        "message": "Dimension weights misconfigured. Sum is 0.95, expected 1.0",
                        "timestamp": "2024-01-15T10:30:00Z"
                    }
                }
            }
        }
    }
)
async def get_dimension_weights() -> DimensionWeightsResponse:
    """Get the current dimension weights configuration."""
    cache_key = "dimension:weights"
    cache = get_cache()

    # Try cache first (with graceful failure)
    if cache:
        try:
            cached = cache.get(cache_key, DimensionWeightsResponse)
            if cached:
                return cached  # Cache hit!
        except Exception:
            pass  # Redis failed, continue

    try:
        total_weight = sum(DIMENSION_WEIGHTS.values())
        is_valid = 0.999 <= total_weight <= 1.001

        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error": "Internal Server Error",
                    "message": f"Dimension weights misconfigured. Sum is {total_weight}, expected 1.0",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        response = DimensionWeightsResponse(
            weights={dim.value: weight for dim, weight in DIMENSION_WEIGHTS.items()},
            total=total_weight,
            is_valid=is_valid,
            timestamp=datetime.utcnow().isoformat()
        )

        # Cache the result
        if cache:
            try:
                cache.set(cache_key, response, TTL_DIMENSION_WEIGHTS)
            except Exception:
                pass  # Don't fail if cache write fails

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": f"Failed to retrieve weights configuration: {str(e)}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )