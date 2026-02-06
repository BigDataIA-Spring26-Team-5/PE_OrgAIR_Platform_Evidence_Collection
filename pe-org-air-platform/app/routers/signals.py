"""
Signals API Router
app/routers/signals.py

Endpoints:
- DELETE /api/v1/signals/reset              - Delete all signals for all companies
- DELETE /api/v1/signals/reset/{ticker}     - Delete all signals for a company
- DELETE /api/v1/signals/reset/{ticker}/{category} - Delete signals by category
- POST  /api/v1/signals/collect             - Trigger signal collection for a company
- GET   /api/v1/signals/tasks/{task_id}     - Get task status
- GET   /api/v1/signals                     - List signals (filterable)
- GET   /api/v1/companies/{id}/signals      - Get signal summary for company
- GET   /api/v1/companies/{id}/signals/{category} - Get signals by category
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone
from enum import Enum
import logging
import boto3
import os

from app.services.leadership_service import get_leadership_service
from app.services.job_signal_service import get_job_signal_service
from app.services.tech_signal_service import get_tech_signal_service
from app.services.patent_signal_service import get_patent_signal_service
from app.repositories.signal_repository import get_signal_repository
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_scores_repository import SignalScoresRepository
from app.services.s3_storage import get_s3_service

logger = logging.getLogger(__name__)

# In-memory task status store (in production, use Redis or database)
_task_store: Dict[str, Dict[str, Any]] = {}

# S3 config (reuse from your existing env)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "pe-orgair-platform-group5")



# S3 Helper


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def delete_s3_prefix(prefix: str) -> int:
    """Delete all S3 objects under a given prefix. Returns count of deleted objects."""
    s3 = get_s3_client()
    deleted_count = 0
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects = page.get('Contents', [])
            if not objects:
                continue
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': delete_keys})
            deleted_count += len(delete_keys)
            logger.info(f"Deleted {len(delete_keys)} S3 objects under prefix '{prefix}'")
    except Exception as e:
        logger.error(f"Error deleting S3 prefix '{prefix}': {e}")
    return deleted_count



# Enums and Models


class SignalCategory(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class CollectionRequest(BaseModel):
    company_id: str = Field(..., description="Company ID or ticker symbol")
    categories: List[str] = Field(
        default=["technology_hiring", "innovation_activity", "digital_presence", "leadership_signals"],
        description="Signal categories to collect"
    )
    years_back: int = Field(default=5, ge=1, le=10, description="Years back for patent search")
    force_refresh: bool = Field(default=False, description="Force refresh cached data")


class CollectionResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SignalSummary(BaseModel):
    company_id: str
    company_name: Optional[str] = None
    ticker: Optional[str] = None
    technology_hiring_score: Optional[float] = None
    innovation_activity_score: Optional[float] = None
    digital_presence_score: Optional[float] = None
    leadership_signals_score: Optional[float] = None
    composite_score: Optional[float] = None
    signal_count: int = 0
    last_updated: Optional[str] = None


class SignalDetail(BaseModel):
    signal_id: str
    company_id: str
    category: str
    source: Optional[str] = None
    normalized_score: Optional[float] = None
    confidence: Optional[float] = None
    evidence_count: int = 0
    signal_date: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None



# Router


router = APIRouter(prefix="/api/v1", tags=["Signals"])



# DELETE /api/v1/signals/reset - Delete ALL signals for ALL companies


@router.delete(
    "/signals/reset",
    summary="Delete all signals for all companies",
    description="Deletes all signal records from Snowflake and all signal-related files from S3 for every company.",
)
async def reset_all_signals():
    """Delete all signals for all companies from Snowflake and S3."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    # --- Snowflake: delete all signals ---
    snowflake_deleted = 0
    try:
        # Try bulk delete first (if your repo supports it)
        if hasattr(repo, 'delete_all'):
            snowflake_deleted = repo.delete_all() or 0
        else:
            # Fallback: delete per company
            companies = company_repo.get_all()
            for company in companies:
                cid = str(company.get('id'))
                if hasattr(repo, 'delete_by_company'):
                    snowflake_deleted += repo.delete_by_company(cid) or 0
                else:
                    # Manual SQL fallback
                    try:
                        repo.execute(f"DELETE FROM signals WHERE company_id = '{cid}'")
                        snowflake_deleted += 1
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"Error deleting signals from Snowflake: {e}")

    # --- Snowflake: delete all signal summaries ---
    summary_deleted = 0
    try:
        if hasattr(repo, 'delete_all_summaries'):
            summary_deleted = repo.delete_all_summaries() or 0
        else:
            try:
                repo.execute("DELETE FROM signal_summaries")
                summary_deleted = 1
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error deleting signal summaries: {e}")

    # --- S3: delete all signal-related files ---
    s3_deleted = 0
    s3_prefixes = ["signals/", "sec/signals/"]
    for prefix in s3_prefixes:
        s3_deleted += delete_s3_prefix(prefix)

    logger.info(f"Reset all signals: snowflake={snowflake_deleted}, summaries={summary_deleted}, s3={s3_deleted}")

    return {
        "message": "All signal data deleted for all companies",
        "snowflake_signals_deleted": snowflake_deleted,
        "snowflake_summaries_deleted": summary_deleted,
        "s3_objects_deleted": s3_deleted,
    }



# DELETE /api/v1/signals/reset/{ticker} - Delete all signals for one company


@router.delete(
    "/signals/reset/{ticker}",
    summary="Delete all signals for a company",
    description="Deletes all signal records from Snowflake and signal files from S3 for the given ticker.",
)
async def reset_signals_by_ticker(ticker: str):
    """Delete all signals for a specific company."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    company = company_repo.get_by_ticker(ticker.upper())
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")

    company_id = str(company['id'])
    company_name = company.get('name', ticker)

    # --- Snowflake: delete signals ---
    snowflake_deleted = 0
    try:
        if hasattr(repo, 'delete_by_company'):
            snowflake_deleted = repo.delete_by_company(company_id) or 0
        else:
            try:
                repo.execute(f"DELETE FROM signals WHERE company_id = '{company_id}'")
                snowflake_deleted = 1
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error deleting signals for {ticker}: {e}")

    # --- Snowflake: delete signal summary ---
    summary_deleted = 0
    try:
        if hasattr(repo, 'delete_summary_by_company'):
            summary_deleted = repo.delete_summary_by_company(company_id) or 0
        elif hasattr(repo, 'delete_summary_by_ticker'):
            summary_deleted = repo.delete_summary_by_ticker(ticker.upper()) or 0
        else:
            try:
                repo.execute(f"DELETE FROM signal_summaries WHERE company_id = '{company_id}'")
                summary_deleted = 1
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error deleting summary for {ticker}: {e}")

    # --- S3: delete signal files for this ticker ---
    s3_deleted = 0
    s3_prefixes = [
        f"signals/{ticker.upper()}/",
        f"sec/signals/{ticker.upper()}/",
    ]
    for prefix in s3_prefixes:
        s3_deleted += delete_s3_prefix(prefix)

    logger.info(f"Reset signals for {ticker}: snowflake={snowflake_deleted}, summaries={summary_deleted}, s3={s3_deleted}")

    return {
        "ticker": ticker.upper(),
        "company_name": company_name,
        "message": f"All signal data deleted for {ticker.upper()}",
        "snowflake_signals_deleted": snowflake_deleted,
        "snowflake_summaries_deleted": summary_deleted,
        "s3_objects_deleted": s3_deleted,
    }



# DELETE /api/v1/signals/reset/{ticker}/{category} - Delete by category


@router.delete(
    "/signals/reset/{ticker}/{category}",
    summary="Delete signals by category for a company",
    description="Deletes signal records for a specific category from Snowflake and S3.",
)
async def reset_signals_by_category(ticker: str, category: str):
    """Delete signals for a company filtered by category."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    valid_categories = ["technology_hiring", "innovation_activity", "digital_presence", "leadership_signals"]
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category: {category}. Valid: {valid_categories}"
        )

    company = company_repo.get_by_ticker(ticker.upper())
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")

    company_id = str(company['id'])
    company_name = company.get('name', ticker)

    # --- Snowflake: delete signals by category ---
    snowflake_deleted = 0
    try:
        if hasattr(repo, 'delete_by_category'):
            snowflake_deleted = repo.delete_by_category(company_id, category) or 0
        else:
            try:
                repo.execute(
                    f"DELETE FROM signals WHERE company_id = '{company_id}' AND category = '{category}'"
                )
                snowflake_deleted = 1
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error deleting {category} signals for {ticker}: {e}")

    # --- S3: delete category-specific files ---
    s3_deleted = 0
    s3_prefixes = [
        f"signals/{ticker.upper()}/{category}/",
        f"sec/signals/{ticker.upper()}/{category}/",
    ]
    for prefix in s3_prefixes:
        s3_deleted += delete_s3_prefix(prefix)

    logger.info(f"Reset {category} signals for {ticker}: snowflake={snowflake_deleted}, s3={s3_deleted}")

    return {
        "ticker": ticker.upper(),
        "company_name": company_name,
        "category": category,
        "message": f"{category} signals deleted for {ticker.upper()}",
        "snowflake_signals_deleted": snowflake_deleted,
        "s3_objects_deleted": s3_deleted,
    }



# POST /api/v1/signals/collect - Trigger signal collection


@router.post(
    "/signals/collect",
    response_model=CollectionResponse,
    summary="Trigger signal collection for a company",
    description="""
    Trigger signal collection for a company. Runs asynchronously in the background.

    **Categories:**
    - `technology_hiring` - Job posting analysis (LinkedIn, Indeed, etc.)
    - `innovation_activity` - Patent analysis (PatentsView API)
    - `digital_presence` - Tech stack analysis (from job descriptions)
    - `leadership_signals` - Leadership analysis (DEF 14A SEC filings)

    Returns a task_id to check status via GET /api/v1/signals/tasks/{task_id}
    """
)
async def collect_signals(
    request: CollectionRequest,
    background_tasks: BackgroundTasks
):
    """Trigger signal collection for a company."""
    task_id = str(uuid4())

    _task_store[task_id] = {
        "task_id": task_id,
        "status": "queued",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "progress": {
            "total_categories": len(request.categories),
            "completed_categories": 0,
            "current_category": None
        },
        "result": None,
        "error": None
    }

    background_tasks.add_task(
        run_signal_collection,
        task_id=task_id,
        company_id=request.company_id,
        categories=request.categories,
        years_back=request.years_back,
        force_refresh=request.force_refresh
    )

    logger.info(f"Signal collection queued: task_id={task_id}, company={request.company_id}")

    return CollectionResponse(
        task_id=task_id,
        status="queued",
        message=f"Signal collection started for company {request.company_id}"
    )


@router.get(
    "/signals/tasks/{task_id}",
    response_model=TaskStatusResponse,
    summary="Get task status",
    description="Check the status of a signal collection task."
)
async def get_task_status(task_id: str):
    """Get the status of a signal collection task."""
    if task_id not in _task_store:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")
    return TaskStatusResponse(**_task_store[task_id])



# GET /api/v1/signals - List signals (filterable)


@router.get(
    "/signals",
    summary="List signals (filterable)",
    description="""
    List all signals with optional filters.

    **Filters:**
    - `category` - Filter by signal category
    - `ticker` - Filter by company ticker
    - `min_score` - Minimum normalized score
    - `limit` - Maximum results to return
    """
)
async def list_signals(
    category: Optional[str] = Query(None, description="Filter by category"),
    ticker: Optional[str] = Query(None, description="Filter by company ticker"),
    min_score: Optional[float] = Query(None, ge=0, le=100, description="Minimum score"),
    limit: int = Query(100, ge=1, le=1000, description="Max results")
):
    """List signals with optional filters."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    results = []

    if ticker:
        company = company_repo.get_by_ticker(ticker.upper())
        if not company:
            raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")
        company_id = str(company['id'])
        if category:
            signals = repo.get_signals_by_category(company_id, category)
        else:
            signals = repo.get_signals_by_company(company_id)
        results = signals
    else:
        companies = company_repo.get_all()
        for company in companies:
            company_id = str(company.get('id'))
            if category:
                signals = repo.get_signals_by_category(company_id, category)
            else:
                signals = repo.get_signals_by_company(company_id)
            results.extend(signals)

    if min_score is not None:
        results = [s for s in results if (s.get('normalized_score') or 0) >= min_score]

    results = results[:limit]

    return {
        "total": len(results),
        "filters": {"category": category, "ticker": ticker, "min_score": min_score},
        "signals": results
    }



# GET /api/v1/companies/{id}/signals - Get signal summary for company


@router.get(
    "/companies/{company_id}/signals",
    summary="Get signal summary for company",
    description="""
    Get aggregated signal summary for a company.

    The company_id can be either:
    - Company UUID/ID
    - Ticker symbol (e.g., "AAPL", "MSFT")
    """
)
async def get_company_signals(company_id: str):
    """Get signal summary for a company."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get('id')) == company_id), None)

    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {company_id}")

    ticker = company.get('ticker')
    db_company_id = str(company.get('id'))

    summary = repo.get_summary_by_ticker(ticker) if ticker else None
    signals = repo.get_signals_by_company(db_company_id)

    signals_by_category = {}
    for signal in signals:
        cat = signal.get('category', 'unknown')
        if cat not in signals_by_category:
            signals_by_category[cat] = []
        signals_by_category[cat].append(signal)

    return {
        "company_id": db_company_id,
        "company_name": company.get('name'),
        "ticker": ticker,
        "summary": {
            "technology_hiring_score": summary.get("technology_hiring_score") if summary else None,
            "innovation_activity_score": summary.get("innovation_activity_score") if summary else None,
            "digital_presence_score": summary.get("digital_presence_score") if summary else None,
            "leadership_signals_score": summary.get("leadership_signals_score") if summary else None,
            "composite_score": summary.get("composite_score") if summary else None,
            "signal_count": len(signals),
            "last_updated": summary.get("updated_at") if summary else None
        },
        "categories": {
            cat: {
                "count": len(sigs),
                "latest_score": sigs[0].get('normalized_score') if sigs else None
            }
            for cat, sigs in signals_by_category.items()
        }
    }



# GET /api/v1/companies/{id}/signals/{category} - Get signals by category


@router.get(
    "/companies/{company_id}/signals/{category}",
    summary="Get signals by category",
    description="""
    Get detailed signals for a company filtered by category.

    **Categories:**
    - `technology_hiring` - Job posting/hiring signals
    - `innovation_activity` - Patent/innovation signals
    - `digital_presence` - Tech stack signals
    - `leadership_signals` - Leadership/executive signals
    """
)
async def get_company_signals_by_category(company_id: str, category: str):
    """Get signals for a company filtered by category."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    valid_categories = ["technology_hiring", "innovation_activity", "digital_presence", "leadership_signals"]
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category: {category}. Valid categories: {valid_categories}"
        )

    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get('id')) == company_id), None)

    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {company_id}")

    ticker = company.get('ticker')
    db_company_id = str(company.get('id'))

    signals = repo.get_signals_by_category(db_company_id, category)

    scores = [s.get('normalized_score') for s in signals if s.get('normalized_score') is not None]
    avg_score = sum(scores) / len(scores) if scores else None

    return {
        "company_id": db_company_id,
        "company_name": company.get('name'),
        "ticker": ticker,
        "category": category,
        "signal_count": len(signals),
        "average_score": round(avg_score, 2) if avg_score else None,
        "signals": signals
    }



# Background Task Implementation


async def run_signal_collection(
    task_id: str,
    company_id: str,
    categories: List[str],
    years_back: int,
    force_refresh: bool
):
    """Background task for signal collection."""
    logger.info(f"Starting signal collection: task_id={task_id}, company={company_id}")

    _task_store[task_id]["status"] = "running"

    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get('id')) == company_id), None)

    if not company:
        _task_store[task_id]["status"] = "failed"
        _task_store[task_id]["error"] = f"Company not found: {company_id}"
        _task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        return

    ticker = company.get('ticker')
    result = {
        "company_id": str(company.get('id')),
        "company_name": company.get('name'),
        "ticker": ticker,
        "signals": {},
        "errors": []
    }

    for i, category in enumerate(categories):
        _task_store[task_id]["progress"]["current_category"] = category

        try:
            if category == "technology_hiring":
                service = get_job_signal_service()
                signal_result = await service.analyze_company(ticker, force_refresh=force_refresh)
                result["signals"]["technology_hiring"] = {
                    "status": "success",
                    "score": signal_result.get("normalized_score"),
                    "details": signal_result
                }

            elif category == "innovation_activity":
                service = get_patent_signal_service()
                signal_result = await service.analyze_company(ticker, years_back=years_back)
                result["signals"]["innovation_activity"] = {
                    "status": "success",
                    "score": signal_result.get("normalized_score"),
                    "details": signal_result
                }

            elif category == "digital_presence":
                service = get_tech_signal_service()
                signal_result = await service.analyze_company(ticker, force_refresh=force_refresh)
                result["signals"]["digital_presence"] = {
                    "status": "success",
                    "score": signal_result.get("normalized_score"),
                    "details": signal_result
                }

            elif category == "leadership_signals":
                service = get_leadership_service()
                signal_result = await service.analyze_company(ticker)
                result["signals"]["leadership_signals"] = {
                    "status": "success",
                    "score": signal_result.get("normalized_score"),
                    "details": signal_result
                }

        except Exception as e:
            logger.error(f"Error collecting {category} signals: {e}")
            result["signals"][category] = {"status": "failed", "error": str(e)}
            result["errors"].append(f"{category}: {str(e)}")

        _task_store[task_id]["progress"]["completed_categories"] = i + 1

    _task_store[task_id]["status"] = "completed" if not result["errors"] else "completed_with_errors"
    _task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _task_store[task_id]["result"] = result
    _task_store[task_id]["progress"]["current_category"] = None

    logger.info(f"Signal collection completed: task_id={task_id}")
    logger.info(f"Signal collection completed: task_id={task_id}")



# RESET (DEMO)


@router.delete(
    "/signals/reset/{ticker}",
    summary="Reset all signals for a company",
    description="""
    Deletes all signal data for a company across Snowflake and S3.

    **Snowflake tables cleared:**
    - `EXTERNAL_SIGNALS` — individual signal rows
    - `COMPANY_SIGNAL_SUMMARIES` — aggregated summary
    - `SIGNAL_SCORES` — detailed scores

    **S3 prefixes cleared:**
    - `signals/jobs/{TICKER}/`
    - `signals/patents/{TICKER}/`
    - `signals/techstack/{TICKER}/`
    """,
    tags=["Reset (Demo)"],
)
async def reset_signals(ticker: str):
    """Delete all signals for a company from Snowflake and S3."""
    ticker = ticker.upper()

    # Look up company
    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found for ticker: {ticker}")

    company_id = str(company["id"])
    result = {"ticker": ticker, "snowflake": {}, "s3": {"deleted_keys": [], "errors": []}}

    # --- Snowflake ---
    signal_repo = get_signal_repository()
    scores_repo = SignalScoresRepository()

    signals_deleted = signal_repo.delete_signals_by_company(company_id)
    result["snowflake"]["external_signals_deleted"] = signals_deleted

    summary_deleted = signal_repo.delete_summary(company_id)
    result["snowflake"]["signal_summary_deleted"] = summary_deleted

    scores_deleted = scores_repo.delete_by_ticker(ticker)
    result["snowflake"]["signal_scores_deleted"] = scores_deleted

    # --- S3 ---
    s3_prefixes = [
        f"signals/jobs/{ticker}/",
        f"signals/patents/{ticker}/",
        f"signals/techstack/{ticker}/",
    ]

    try:
        s3 = get_s3_service()
        for prefix in s3_prefixes:
            keys = s3.list_files(prefix)
            for key in keys:
                if s3.delete_file(key):
                    result["s3"]["deleted_keys"].append(key)
                else:
                    result["s3"]["errors"].append(key)
    except Exception as e:
        logger.error(f"S3 cleanup error for {ticker}: {e}")
        result["s3"]["errors"].append(str(e))

    total_s3 = len(result["s3"]["deleted_keys"])
    logger.info(
        f"Signal reset for {ticker}: "
        f"snowflake(signals={signals_deleted}, summary={summary_deleted}, scores={scores_deleted}) "
        f"s3({total_s3} files deleted)"
    )

    return result
