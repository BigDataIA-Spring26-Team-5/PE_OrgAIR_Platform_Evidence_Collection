"""
Signals Router - Job Postings, Patents, and Tech Stack Data
app/routers/signals.py

Provides API endpoints for collecting and retrieving AI-related signals for companies.
- POST /collect: Triggers data collection and stores results locally
- GET endpoints: Retrieve previously collected data from local storage
"""

from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from app.models.signal_responses import (
    JobPostingsResponse,
    PatentsResponse,
    TechStacksResponse,
    AllSignalsResponse,
    JobPostingResponse,
    PatentResponse,
    TechStackResponse,
    SignalCollectRequest,
    SignalCollectResponse,
    StoredSignalSummary,
)
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.keywords import TOP_AI_TOOLS
from app.pipelines.utils import Company
from app.services.signals_storage import SignalsStorage

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


def _create_state(company_name: str, output_dir: str) -> Pipeline2State:
    """Create a pipeline state for a single company."""
    return Pipeline2State(
        companies=[Company.from_name(company_name, 0).to_dict()],
        output_dir=output_dir
    )


async def _collect_signals_task(
    company_name: str,
    collect_jobs: bool,
    collect_patents: bool,
    patents_years_back: int,
) -> None:
    """
    Background task to collect all signals for a company and store locally.
    """
    storage = SignalsStorage()

    job_postings = []
    patents = []
    job_market_score = None
    patent_score = None
    techstack_score = None
    techstack_keywords = []

    # Collect job postings and tech stack
    if collect_jobs:
        try:
            job_state = _create_state(company_name, "data/signals/jobs")
            job_state = await run_job_signals(job_state, use_cloud_storage=False)

            job_postings = job_state.job_postings
            job_market_score = job_state.job_market_scores.get("company-0")
            techstack_score = job_state.techstack_scores.get("company-0")
            techstack_keywords = job_state.company_techstacks.get("company-0", [])
        except Exception as e:
            print(f"[signals] Job collection failed for {company_name}: {e}")

    # Collect patents
    if collect_patents:
        try:
            patent_state = _create_state(company_name, "data/signals/patents")
            patent_state = await run_patent_signals(patent_state, years_back=patents_years_back)

            patents = patent_state.patents
            patent_score = patent_state.patent_scores.get("company-0")
        except Exception as e:
            print(f"[signals] Patent collection failed for {company_name}: {e}")

    # Save all collected data
    storage.save_signals(
        company_name=company_name,
        job_postings=job_postings,
        patents=patents,
        job_market_score=job_market_score,
        patent_score=patent_score,
        techstack_score=techstack_score,
        techstack_keywords=techstack_keywords,
    )

    print(f"[signals] Collection complete for {company_name}: {len(job_postings)} jobs, {len(patents)} patents")


# ============================================
# POST /collect - Trigger signal collection
# ============================================

@router.post("/collect", response_model=SignalCollectResponse)
async def collect_signals(
    req: SignalCollectRequest,
    background_tasks: BackgroundTasks,
):
    """
    Trigger signal collection (job postings, patents, tech stack) for a company.

    This endpoint queues a background task to:
    1. Scrape job postings from LinkedIn, Indeed, Glassdoor
    2. Fetch patents from PatentsView API
    3. Extract tech stack keywords from job descriptions
    4. Store all data locally for later retrieval

    Use GET endpoints to retrieve the collected data.
    """
    if not req.company_name or not req.company_name.strip():
        raise HTTPException(status_code=400, detail="company_name is required")

    company_name = req.company_name.strip()

    background_tasks.add_task(
        _collect_signals_task,
        company_name=company_name,
        collect_jobs=req.collect_jobs,
        collect_patents=req.collect_patents,
        patents_years_back=req.patents_years_back,
    )

    return SignalCollectResponse(
        status="queued",
        message=f"Signal collection queued for {company_name}",
        company_name=company_name,
    )


# ============================================
# GET /companies - List companies with signals
# ============================================

@router.get("/companies", response_model=List[StoredSignalSummary])
async def list_signal_companies():
    """
    List all companies that have collected signals data.

    Returns summaries including scores and counts for each company.
    """
    storage = SignalsStorage()
    companies = storage.list_companies()

    return [StoredSignalSummary(**c) for c in companies]


# ============================================
# GET /job_postings - Retrieve stored job data
# ============================================

@router.get("/job_postings", response_model=JobPostingsResponse)
async def get_job_postings(
    company_name: str = Query(..., description="Company name to retrieve job postings for"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of job postings to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Retrieve stored job postings for a company.

    Returns previously collected job data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_job_postings(company_name, limit=limit, offset=offset)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No job data found for '{company_name}'. Use POST /collect to fetch data first."
        )

    # Convert to response models
    job_responses = []
    for job in data.get("job_postings", []):
        try:
            job_responses.append(JobPostingResponse(**job))
        except Exception:
            continue

    return JobPostingsResponse(
        company_id="company-0",
        company_name=data.get("company_name"),
        total_count=data.get("total_count", 0),
        ai_count=data.get("ai_count", 0),
        job_market_score=data.get("job_market_score"),
        job_postings=job_responses,
        errors=[],
    )


# ============================================
# GET /patents - Retrieve stored patent data
# ============================================

@router.get("/patents", response_model=PatentsResponse)
async def get_patents(
    company_name: str = Query(..., description="Company name to retrieve patents for"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of patents to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Retrieve stored patents for a company.

    Returns previously collected patent data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_patents(company_name, limit=limit, offset=offset)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No patent data found for '{company_name}'. Use POST /collect to fetch data first."
        )

    # Convert to response models
    patent_responses = []
    for patent in data.get("patents", []):
        try:
            patent_responses.append(PatentResponse(**patent))
        except Exception:
            continue

    return PatentsResponse(
        company_id="company-0",
        company_name=data.get("company_name"),
        total_count=data.get("total_count", 0),
        ai_count=data.get("ai_count", 0),
        patent_portfolio_score=data.get("patent_portfolio_score"),
        patents=patent_responses,
    )


# ============================================
# GET /tech_stacks - Retrieve stored tech data
# ============================================

@router.get("/tech_stacks", response_model=TechStacksResponse)
async def get_tech_stacks(
    company_name: str = Query(..., description="Company name to retrieve tech stack for"),
):
    """
    Retrieve stored tech stack data for a company.

    Returns previously collected tech stack data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_techstack(company_name)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No tech stack data found for '{company_name}'. Use POST /collect to fetch data first."
        )

    keywords = data.get("techstack_keywords", [])
    ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
    techstack_score = data.get("techstack_score", 0)

    techstack = TechStackResponse(
        company_id="company-0",
        company_name=data.get("company_name", company_name),
        techstack_keywords=keywords,
        ai_tools_found=ai_tools,
        techstack_score=techstack_score,
        total_keywords=len(keywords),
        total_ai_tools=len(ai_tools),
    )

    return TechStacksResponse(
        company_id="company-0",
        company_name=data.get("company_name", company_name),
        techstack_score=techstack_score,
        techstacks=[techstack],
    )


# ============================================
# GET /all - Retrieve all stored signals
# ============================================

@router.get("/all", response_model=AllSignalsResponse)
async def get_all_signals(
    company_name: str = Query(..., description="Company name to retrieve all signals for"),
    jobs_limit: int = Query(50, ge=1, le=200, description="Maximum number of job postings to return"),
    patents_limit: int = Query(50, ge=1, le=200, description="Maximum number of patents to return"),
):
    """
    Retrieve all stored signal data for a company.

    Returns previously collected job postings, patents, and tech stack data.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()

    # Check if company has any data
    summary = storage.get_summary(company_name)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No signal data found for '{company_name}'. Use POST /collect to fetch data first."
        )

    # Get job postings
    job_data = storage.get_job_postings(company_name, limit=jobs_limit, offset=0)
    job_responses = []
    if job_data:
        for job in job_data.get("job_postings", []):
            try:
                job_responses.append(JobPostingResponse(**job))
            except Exception:
                continue

    # Get patents
    patent_data = storage.get_patents(company_name, limit=patents_limit, offset=0)
    patent_responses = []
    if patent_data:
        for patent in patent_data.get("patents", []):
            try:
                patent_responses.append(PatentResponse(**patent))
            except Exception:
                continue

    # Get tech stack
    techstack_data = storage.get_techstack(company_name)
    techstacks = []
    if techstack_data:
        keywords = techstack_data.get("techstack_keywords", [])
        ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
        techstack_score = techstack_data.get("techstack_score", 0)

        techstacks.append(TechStackResponse(
            company_id="company-0",
            company_name=company_name,
            techstack_keywords=keywords,
            ai_tools_found=ai_tools,
            techstack_score=techstack_score,
            total_keywords=len(keywords),
            total_ai_tools=len(ai_tools),
        ))

    return AllSignalsResponse(
        company_id="company-0",
        company_name=company_name,
        job_market_score=summary.get("job_market_score"),
        patent_portfolio_score=summary.get("patent_portfolio_score"),
        techstack_score=summary.get("techstack_score"),
        total_jobs=summary.get("total_jobs", 0),
        ai_jobs=summary.get("ai_jobs", 0),
        total_patents=summary.get("total_patents", 0),
        ai_patents=summary.get("ai_patents", 0),
        job_postings=job_responses,
        patents=patent_responses,
        techstacks=techstacks,
    )
