"""
Signals API Router
app/routers/signals.py

Endpoints:
- POST /api/v1/signals/collect         - Trigger signal collection for a company
- GET  /api/v1/signals                 - List signals (filterable)
- GET  /api/v1/companies/{id}/signals  - Get signal summary for company
- GET  /api/v1/companies/{id}/signals/{category} - Get signals by category
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone
from enum import Enum
import logging

from app.services.leadership_service import get_leadership_service
from app.services.job_signal_service import get_job_signal_service
from app.services.tech_signal_service import get_tech_signal_service
from app.services.patent_signal_service import get_patent_signal_service
from app.repositories.signal_repository import get_signal_repository
from app.repositories.company_repository import CompanyRepository

logger = logging.getLogger(__name__)

# In-memory task status store (in production, use Redis or database)
_task_store: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Enums and Models
# =============================================================================

class SignalCategory(str, Enum):
    """Signal category types."""
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class CollectionRequest(BaseModel):
    """Request model for signal collection."""
    company_id: str = Field(..., description="Company ID or ticker symbol")
    categories: List[str] = Field(
        default=["technology_hiring", "innovation_activity", "digital_presence", "leadership_signals"],
        description="Signal categories to collect"
    )
    years_back: int = Field(default=5, ge=1, le=10, description="Years back for patent search")
    force_refresh: bool = Field(default=False, description="Force refresh cached data")


class CollectionResponse(BaseModel):
    """Response model for signal collection."""
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    """Response model for task status."""
    task_id: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SignalSummary(BaseModel):
    """Signal summary for a company."""
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
    """Detailed signal record."""
    signal_id: str
    company_id: str
    category: str
    source: Optional[str] = None
    normalized_score: Optional[float] = None
    confidence: Optional[float] = None
    evidence_count: int = 0
    signal_date: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# =============================================================================
# Routers
# =============================================================================

# Main signals router
router = APIRouter(prefix="/api/v1", tags=["Signals"])


# =============================================================================
# POST /api/v1/signals/collect - Trigger signal collection
# =============================================================================

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
    # Generate task ID
    task_id = str(uuid4())

    # Initialize task status
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

    # Add to background tasks
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


# =============================================================================
# GET /api/v1/signals - List signals (filterable)
# =============================================================================

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
        # Get signals for specific company
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
        # Get signals for all companies
        companies = company_repo.get_all()
        for company in companies:
            company_id = str(company.get('id'))
            if category:
                signals = repo.get_signals_by_category(company_id, category)
            else:
                signals = repo.get_signals_by_company(company_id)
            results.extend(signals)

    # Apply min_score filter
    if min_score is not None:
        results = [s for s in results if (s.get('normalized_score') or 0) >= min_score]

    # Apply limit
    results = results[:limit]

    return {
        "total": len(results),
        "filters": {
            "category": category,
            "ticker": ticker,
            "min_score": min_score
        },
        "signals": results
    }


# =============================================================================
# GET /api/v1/companies/{id}/signals - Get signal summary for company
# =============================================================================

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

    # Try to find company by ticker first, then by ID
    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        # Try as company ID
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get('id')) == company_id), None)

    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {company_id}")

    ticker = company.get('ticker')
    db_company_id = str(company.get('id'))

    # Get summary
    summary = repo.get_summary_by_ticker(ticker) if ticker else None

    # Get all signals for this company
    signals = repo.get_signals_by_company(db_company_id)

    # Group signals by category
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


# =============================================================================
# GET /api/v1/companies/{id}/signals/{category} - Get signals by category
# =============================================================================

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

    # Validate category
    valid_categories = ["technology_hiring", "innovation_activity", "digital_presence", "leadership_signals"]
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category: {category}. Valid categories: {valid_categories}"
        )

    # Try to find company by ticker first, then by ID
    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get('id')) == company_id), None)

    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {company_id}")

    ticker = company.get('ticker')
    db_company_id = str(company.get('id'))

    # Get signals for this category
    signals = repo.get_signals_by_category(db_company_id, category)

    # Calculate aggregate stats
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


# =============================================================================
# Background Task Implementation
# =============================================================================

async def run_signal_collection(
    task_id: str,
    company_id: str,
    categories: List[str],
    years_back: int,
    force_refresh: bool
):
    """Background task for signal collection."""
    logger.info(f"Starting signal collection: task_id={task_id}, company={company_id}")

    # Update status to running
    _task_store[task_id]["status"] = "running"

    # Get company info
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

    # Process each category
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

    # Update task status
    _task_store[task_id]["status"] = "completed" if not result["errors"] else "completed_with_errors"
    _task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _task_store[task_id]["result"] = result
    _task_store[task_id]["progress"]["current_category"] = None

    logger.info(f"Signal collection completed: task_id={task_id}")
