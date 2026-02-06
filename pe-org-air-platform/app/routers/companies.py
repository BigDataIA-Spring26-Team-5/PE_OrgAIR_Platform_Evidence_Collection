"""
Company Router - PE Org-AI-R Platform
app/routers/companies.py

Handles company CRUD operations with Redis caching.
"""

import time
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, root_validator

from app.core.dependencies import get_company_repository, get_industry_repository
from app.repositories.company_repository import CompanyRepository
from app.repositories.industry_repository import IndustryRepository
from app.services.cache import get_cache, TTL_COMPANY

router = APIRouter(prefix="/api/v1", tags=["companies"])



#  Validation Error Messages


FIELD_MESSAGES = {
    "name": {
        "missing": "Company name is required",
        "string_too_short": "Company name cannot be empty",
        "string_too_long": "Company name must not exceed 255 characters",
        "string_type": "Company name must be a string",
    },
    "ticker": {
        "string_too_long": "Ticker symbol must not exceed 10 characters",
        "string_pattern_mismatch": "Ticker symbol must contain only uppercase letters (A-Z)",
        "string_type": "Ticker symbol must be a string",
    },
    "industry_id": {
        "missing": "Industry ID is required",
        "uuid_parsing": "Industry ID must be a valid UUID format",
        "uuid_type": "Industry ID must be a valid UUID",
    },
    "position_factor": {
        "less_than_equal": "Position factor must be between -1.0 and 1.0",
        "greater_than_equal": "Position factor must be between -1.0 and 1.0",
        "float_type": "Position factor must be a number",
        "float_parsing": "Position factor must be a valid number",
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
    "float_type": "Field '{field}' must be a number",
    "float_parsing": "Field '{field}' must be a valid number",
    "int_type": "Field '{field}' must be an integer",
    "int_parsing": "Field '{field}' must be a valid integer",
    "json_invalid": "Malformed JSON request body",
    "extra_forbidden": "Unknown field '{field}' is not allowed",
}


def get_validation_message(field: str, error_type: str) -> str:
    if field in FIELD_MESSAGES:
        for key in FIELD_MESSAGES[field]:
            if key in error_type:
                return FIELD_MESSAGES[field][key]
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



#  Schemas


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CacheInfo(BaseModel):
    """Cache metadata for debugging - shows if Redis is working."""
    hit: bool
    source: str
    key: str
    latency_ms: float
    ttl_seconds: int
    message: str


class CompanyBase(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=10)
    industry_id: Optional[UUID] = None
    position_factor: float = Field(default=0.0, ge=-1.0, le=1.0)

    @root_validator(pre=True)
    @classmethod
    def uppercase_ticker(cls, values):
        if 'ticker' in values and values['ticker']:
            values['ticker'] = values['ticker'].upper()
        return values


class CompanyCreate(CompanyBase):
    name: str = Field(..., min_length=1, max_length=255)
    industry_id: UUID


class CompanyUpdate(CompanyBase):
    pass


class CompanyResponse(BaseModel):
    id: UUID
    name: str
    ticker: Optional[str] = None
    industry_id: UUID
    position_factor: float
    created_at: datetime
    updated_at: datetime
    cache: Optional[CacheInfo] = None

    class Config:
        from_attributes = True


class CompanyListResponse(BaseModel):
    """Response for get all companies (no pagination)."""
    items: list[CompanyResponse]
    total: int
    cache: Optional[CacheInfo] = None


class PaginatedCompanyResponse(BaseModel):
    items: list[CompanyResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
    cache: Optional[CacheInfo] = None



#  Exception Helpers


def raise_error(status_code: int, error_code: str, message: str):
    raise HTTPException(
        status_code=status_code,
        detail=ErrorResponse(error_code=error_code, message=message).model_dump(mode="json"),
    )

def raise_company_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "COMPANY_NOT_FOUND", "Company not found")

def raise_industry_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "INDUSTRY_NOT_FOUND", "Industry does not exist")

def raise_company_deleted():
    raise_error(status.HTTP_410_GONE, "COMPANY_DELETED", "Company has been deleted")

def raise_duplicate_company():
    raise_error(status.HTTP_409_CONFLICT, "DUPLICATE_COMPANY", "Company already exists in this industry")

def raise_validation_error(msg: str):
    raise_error(status.HTTP_422_UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", msg)



#  Cache Helpers


CACHE_KEY_COMPANY_PREFIX = "company:"
CACHE_KEY_COMPANIES_LIST_PREFIX = "companies:list:"
CACHE_KEY_COMPANIES_ALL = "companies:all"
CACHE_KEY_COMPANIES_BY_INDUSTRY = "companies:industry:"


def get_company_cache_key(company_id: UUID) -> str:
    return f"{CACHE_KEY_COMPANY_PREFIX}{company_id}"


def get_companies_list_cache_key(page: int, page_size: int, industry_id: Optional[UUID]) -> str:
    return f"{CACHE_KEY_COMPANIES_LIST_PREFIX}page:{page}:size:{page_size}:industry:{industry_id}"


def get_companies_by_industry_cache_key(industry_id: UUID) -> str:
    return f"{CACHE_KEY_COMPANIES_BY_INDUSTRY}{industry_id}"


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


def invalidate_company_cache(company_id: Optional[UUID] = None) -> None:
    """Invalidate company cache entries in Redis."""
    cache = get_cache()
    if cache:
        try:
            if company_id:
                cache.delete(get_company_cache_key(company_id))
            cache.delete_pattern(f"{CACHE_KEY_COMPANIES_LIST_PREFIX}*")
            cache.delete(CACHE_KEY_COMPANIES_ALL)
            cache.delete_pattern(f"{CACHE_KEY_COMPANIES_BY_INDUSTRY}*")
        except Exception:
            pass



#  Helper Functions


def row_to_response(row: dict, cache_info: Optional[CacheInfo] = None) -> CompanyResponse:
    return CompanyResponse(
        id=UUID(row["id"]),
        name=row["name"],
        ticker=row["ticker"],
        industry_id=UUID(row["industry_id"]),
        position_factor=float(row["position_factor"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        cache=cache_info,
    )



#  Routes


@router.post(
    "/companies",
    response_model=CompanyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new company",
    description="Creates a new company. Validates schema, checks industry existence, and enforces uniqueness.",
)
async def create_company(
    company: CompanyCreate,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyResponse:
    if not industry_repo.exists(company.industry_id):
        raise_industry_not_found()

    if company_repo.check_duplicate(company.name, company.industry_id):
        raise_duplicate_company()

    company_data = company_repo.create(
        name=company.name,
        industry_id=company.industry_id,
        ticker=company.ticker,
        position_factor=company.position_factor,
    )

    invalidate_company_cache()

    return row_to_response(company_data)


@router.get(
    "/companies/all",
    response_model=CompanyListResponse,
    summary="Get all companies",
    description="Returns all companies without pagination. Cached for 5 minutes.",
)
async def get_all_companies(
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> CompanyListResponse:
    cache_key = CACHE_KEY_COMPANIES_ALL
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(cache_key, CompanyListResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, cache_key, latency, TTL_COMPANY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from database
    companies = company_repo.get_all()

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, cache_key, latency, TTL_COMPANY)

    response = CompanyListResponse(
        items=[row_to_response(c) for c in companies],
        total=len(companies),
        cache=cache_info,
    )

    # 3. Store in cache
    if cache:
        try:
            cache.set(cache_key, response, TTL_COMPANY)
        except Exception:
            pass

    return response


@router.get(
    "/companies/industry/{industry_id}",
    response_model=CompanyListResponse,
    summary="Get companies by industry",
    description="Returns all companies for a specific industry. Cached for 5 minutes.",
)
async def get_companies_by_industry(
    industry_id: UUID,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyListResponse:
    # Check industry exists
    if not industry_repo.exists(industry_id):
        raise_industry_not_found()

    cache_key = get_companies_by_industry_cache_key(industry_id)
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(cache_key, CompanyListResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, cache_key, latency, TTL_COMPANY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from database
    companies = company_repo.get_by_industry(industry_id)

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, cache_key, latency, TTL_COMPANY)

    response = CompanyListResponse(
        items=[row_to_response(c) for c in companies],
        total=len(companies),
        cache=cache_info,
    )

    # 3. Store in cache
    if cache:
        try:
            cache.set(cache_key, response, TTL_COMPANY)
        except Exception:
            pass

    return response


@router.get(
    "/companies",
    response_model=PaginatedCompanyResponse,
    summary="List companies (paginated)",
    description="Returns a paginated list of companies. Cached for 5 minutes.",
)
async def list_companies(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    industry_id: Optional[UUID] = Query(default=None),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> PaginatedCompanyResponse:
    cache_key = get_companies_list_cache_key(page, page_size, industry_id)
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(cache_key, PaginatedCompanyResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, cache_key, latency, TTL_COMPANY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from database
    # Get all companies and apply pagination in memory
    if industry_id:
        all_companies = company_repo.get_by_industry(industry_id)
    else:
        all_companies = company_repo.get_all()

    total = len(all_companies)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    # Apply pagination
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    companies = all_companies[start_idx:end_idx]

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, cache_key, latency, TTL_COMPANY)

    response = PaginatedCompanyResponse(
        items=[row_to_response(c) for c in companies],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        cache=cache_info,
    )

    # 3. Store in cache
    if cache:
        try:
            cache.set(cache_key, response, TTL_COMPANY)
        except Exception:
            pass

    return response


@router.get(
    "/companies/{id}",
    response_model=CompanyResponse,
    summary="Get company by ID",
    description="Retrieves a company by UUID. Cached for 5 minutes.",
)
async def get_company(
    id: UUID,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> CompanyResponse:
    cache_key = get_company_cache_key(id)
    cache = get_cache()
    start_time = time.time()

    # 1. Try cache first
    if cache:
        try:
            cached = cache.get(cache_key, CompanyResponse)
            if cached:
                latency = (time.time() - start_time) * 1000
                cached.cache = create_cache_info(True, cache_key, latency, TTL_COMPANY)
                return cached
        except Exception:
            pass

    # 2. Cache miss - fetch from Snowflake
    if company_repo.is_deleted(id):
        raise_company_deleted()

    company = company_repo.get_by_id(id)
    if not company:
        raise_company_not_found()

    latency = (time.time() - start_time) * 1000
    cache_info = create_cache_info(False, cache_key, latency, TTL_COMPANY)
    response = row_to_response(company, cache_info)

    # 3. Store in cache
    if cache:
        try:
            cache.set(cache_key, response, TTL_COMPANY)
        except Exception:
            pass

    return response


@router.put(
    "/companies/{id}",
    response_model=CompanyResponse,
    summary="Update company",
    description="Updates company data and invalidates cache.",
)
async def update_company(
    id: UUID,
    company: CompanyUpdate,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyResponse:
    if company_repo.is_deleted(id):
        raise_company_deleted()

    existing = company_repo.get_by_id(id)
    if not existing:
        raise_company_not_found()

    update_data = company.model_dump(exclude_unset=True)

    if not update_data:
        return row_to_response(existing)

    if "industry_id" in update_data and str(update_data["industry_id"]) != existing["industry_id"]:
        if not industry_repo.exists(update_data["industry_id"]):
            raise_industry_not_found()

    new_name = update_data.get("name", existing["name"])
    new_industry_id = update_data.get("industry_id", UUID(existing["industry_id"]))

    if new_name != existing["name"] or str(new_industry_id) != existing["industry_id"]:
        if company_repo.check_duplicate(new_name, new_industry_id, exclude_id=id):
            raise_duplicate_company()

    updated = company_repo.update(
        company_id=id,
        name=update_data.get("name"),
        ticker=update_data.get("ticker"),
        industry_id=update_data.get("industry_id"),
        position_factor=update_data.get("position_factor"),
    )

    invalidate_company_cache(id)

    return row_to_response(updated)


@router.delete(
    "/companies/{id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Soft delete company",
    description="Marks a company as deleted and invalidates cache.",
)
async def delete_company(
    id: UUID,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> None:
    if company_repo.is_deleted(id):
        raise_company_deleted()

    if not company_repo.exists(id):
        raise_company_not_found()

    company_repo.soft_delete(id)
    invalidate_company_cache(id)