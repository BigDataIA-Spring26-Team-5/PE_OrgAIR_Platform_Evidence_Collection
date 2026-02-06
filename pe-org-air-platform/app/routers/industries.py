"""
Industry Router - PE Org-AI-R Platform
app/routers/industries.py

Handles industry-related endpoints with Redis caching.
"""

import time
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, Field

from app.core.dependencies import get_industry_repository
from app.repositories.industry_repository import IndustryRepository
from app.services.cache import get_cache, TTL_INDUSTRY

router = APIRouter(prefix="/api/v1", tags=["industries"])





#  Schemas


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CacheInfo(BaseModel):
    """Cache metadata for debugging - shows if Redis is working."""
    hit: bool                          # True = data from cache, False = data from database
    source: str                        # "redis" or "database"
    key: str                           # Redis key used
    latency_ms: float                  # Time taken in milliseconds
    ttl_seconds: int                   # Cache TTL setting
    message: str                       # Human-readable status


class IndustryResponse(BaseModel):
    id: UUID
    name: str
    sector: str
    h_r_base: float
    cache: Optional[CacheInfo] = None  # Cache info for debugging

    class Config:
        from_attributes = True


class IndustryListResponse(BaseModel):
    items: list[IndustryResponse]
    total: int
    cache: Optional[CacheInfo] = None  # Cache info for debugging



#  Exception Helpers


def raise_error(status_code: int, error_code: str, message: str):
    raise HTTPException(
        status_code=status_code,
        detail=ErrorResponse(error_code=error_code, message=message).model_dump(mode="json"),
    )


def raise_industry_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "INDUSTRY_NOT_FOUND", "Industry not found")


def raise_internal_error():
    raise_error(status.HTTP_500_INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "Unexpected server error")



#  Cache Helpers


CACHE_KEY_INDUSTRY_LIST = "industry:list"
CACHE_KEY_INDUSTRY_PREFIX = "industry:"


def get_industry_cache_key(industry_id: UUID) -> str:
    """Generate cache key for a single industry."""
    return f"{CACHE_KEY_INDUSTRY_PREFIX}{industry_id}"


def create_cache_info(hit: bool, key: str, latency_ms: float, ttl: int) -> CacheInfo:
    """Create CacheInfo object with human-readable message."""
    if hit:
        return CacheInfo(
            hit=True,
            source="redis",
            key=key,
            latency_ms=round(latency_ms, 3),
            ttl_seconds=ttl,
            message=f"✅ Cache HIT - Data served from Redis in {latency_ms:.3f}ms",
        )
    else:
        return CacheInfo(
            hit=False,
            source="database",
            key=key,
            latency_ms=round(latency_ms, 3),
            ttl_seconds=ttl,
            message=f"❌ Cache MISS - Data fetched from database in {latency_ms:.3f}ms, now cached for {ttl}s",
        )


def invalidate_industry_cache(industry_id: Optional[UUID] = None) -> None:
    """Invalidate industry cache entries."""
    cache = get_cache()
    if cache:
        try:
            cache.delete(CACHE_KEY_INDUSTRY_LIST)
            if industry_id:
                cache.delete(get_industry_cache_key(industry_id))
        except Exception:
            pass



#  Helper Functions


def row_to_response(row: dict, cache_info: Optional[CacheInfo] = None) -> IndustryResponse:
    """Convert database row to response model."""
    return IndustryResponse(
        id=UUID(row["id"]),
        name=row["name"],
        sector=row["sector"],
        h_r_base=float(row["h_r_base"]),
        cache=cache_info,
    )



#  Routes


@router.get(
    "/industries",
    response_model=IndustryListResponse,
    responses={
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T01:19:36.806Z",
                    }
                }
            },
        },
    },
    summary="List industries",
    description="Returns all available industries. Cached for 1 hour.",
)
async def list_industries(
    repo: IndustryRepository = Depends(get_industry_repository),
) -> IndustryListResponse:
    """
    List all industries with Redis caching.

    Cache Strategy:
    - Key: "industry:list"
    - TTL: 1 hour (3600 seconds)
    - Invalidation: When any industry is modified
    """
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(CACHE_KEY_INDUSTRY_LIST, IndustryListResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, CACHE_KEY_INDUSTRY_LIST, latency, TTL_INDUSTRY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from Snowflake
    industry_rows = repo.get_all()
    industries = [row_to_response(ind) for ind in industry_rows]

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, CACHE_KEY_INDUSTRY_LIST, latency, TTL_INDUSTRY)

    response = IndustryListResponse(
        items=industries,
        total=len(industries),
        cache=cache_info,
    )

    # 3. Store in cache
    if cache:
        try:
            cache.set(CACHE_KEY_INDUSTRY_LIST, response, TTL_INDUSTRY)
        except Exception:
            pass

    return response


@router.get(
    "/industries/{id}",
    response_model=IndustryResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Industry not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INDUSTRY_NOT_FOUND",
                        "message": "Industry not found",
                        "details": None,
                        "timestamp": "2026-01-28T01:19:36.803Z",
                    }
                }
            },
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "ID must be a valid UUID format",
                        "details": {"field": "id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T01:19:36.805Z",
                    }
                }
            },
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
                        "timestamp": "2026-01-28T01:19:36.806Z",
                    }
                }
            },
        },
    },
    summary="Get industry by ID",
    description="Retrieves a single industry by UUID. Cached for 1 hour.",
)
async def get_industry(
    id: UUID,
    repo: IndustryRepository = Depends(get_industry_repository),
) -> IndustryResponse:
    """
    Get industry by ID with Redis caching.

    Cache Strategy:
    - Key: "industry:{id}"
    - TTL: 1 hour (3600 seconds)
    """
    cache_key = get_industry_cache_key(id)
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(cache_key, IndustryResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, cache_key, latency, TTL_INDUSTRY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from Snowflake
    industry = repo.get_by_id(id)
    if not industry:
        raise_industry_not_found()

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, cache_key, latency, TTL_INDUSTRY)
    response = row_to_response(industry, cache_info)

    # 3. Store in cache
    if cache:
        try:
            cache.set(cache_key, response, TTL_INDUSTRY)
        except Exception:
            pass

    return response