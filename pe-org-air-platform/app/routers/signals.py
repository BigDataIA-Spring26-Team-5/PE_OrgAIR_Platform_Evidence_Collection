"""
Signals Router - Job Postings, Patents, and Tech Stack Data
app/routers/signals.py

Provides API endpoints for collecting and retrieving AI-related signals for companies.
- POST /collect: Triggers data collection for a company (by company_id from Snowflake)
- GET endpoints: Retrieve previously collected data from local storage (by ticker)

Storage structure:
    data/signals/jobs/<ticker>/     - Job postings and tech stack
    data/signals/patents/<ticker>/  - Patent data
"""

from typing import List, Optional
from uuid import UUID

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
    SignalScoresResponse,
)
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.keywords import TOP_AI_TOOLS
from app.pipelines.utils import Company
from app.services.signals_storage import SignalsStorage
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_scores_repository import (
    SignalScoresRepository,
    calculate_composite_score,
)

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


def _create_state(company_name: str, output_dir: str) -> Pipeline2State:
    """Create a pipeline state for a single company."""
    return Pipeline2State(
        companies=[Company.from_name(company_name, 0).to_dict()],
        output_dir=output_dir
    )


async def _collect_signals_task(
    company_id: str,
    company_name: str,
    ticker: str,
    collect_jobs: bool,
    collect_patents: bool,
    patents_years_back: int,
) -> None:
    """
    Background task to collect all signals for a company.

    Storage strategy (Option 1: Local + S3 parallel):
    1. Save to local filesystem
    2. Upload to S3 in parallel
    3. Upsert scores to Snowflake (replace if ticker exists)

    Args:
        company_id: UUID of the company from Snowflake
        company_name: Company name for search/scraping
        ticker: Company ticker symbol for storage directory naming
    """
    storage = SignalsStorage(enable_s3=True)

    # Initialize score tracking
    job_market_score = None
    techstack_score = None
    patent_score = None
    total_jobs = 0
    ai_jobs = 0
    total_patents = 0
    ai_patents = 0
    techstack_keywords = []
    s3_jobs_key = None
    s3_patents_key = None

    # Collect job postings and tech stack
    if collect_jobs:
        job_postings = []

        try:
            job_state = _create_state(company_name, f"data/signals/jobs/{ticker.upper()}")
            job_state = await run_job_signals(job_state, use_cloud_storage=False)

            job_postings = job_state.job_postings
            job_market_score = job_state.job_market_scores.get("company-0")
            techstack_score = job_state.techstack_scores.get("company-0")
            techstack_keywords = job_state.company_techstacks.get("company-0", [])
        except Exception as e:
            print(f"[signals] Job collection failed for {company_name} ({ticker}): {e}")

        # Save job signals (local + S3 parallel)
        local_path, s3_jobs_key = storage.save_job_signals(
            company_id=company_id,
            company_name=company_name,
            ticker=ticker,
            job_postings=job_postings,
            job_market_score=job_market_score,
            techstack_score=techstack_score,
            techstack_keywords=techstack_keywords,
        )

        total_jobs = len(job_postings)
        ai_jobs = sum(1 for j in job_postings if j.get("is_ai_role", False))
        print(f"[signals] Jobs saved for {company_name} ({ticker}): {total_jobs} jobs")

    # Collect patents
    if collect_patents:
        patents = []

        try:
            patent_state = _create_state(company_name, f"data/signals/patents/{ticker.upper()}")
            patent_state = await run_patent_signals(patent_state, years_back=patents_years_back)

            patents = patent_state.patents
            patent_score = patent_state.patent_scores.get("company-0")
        except Exception as e:
            print(f"[signals] Patent collection failed for {company_name} ({ticker}): {e}")

        # Save patent signals (local + S3 parallel)
        local_path, s3_patents_key = storage.save_patent_signals(
            company_id=company_id,
            company_name=company_name,
            ticker=ticker,
            patents=patents,
            patent_score=patent_score,
        )

        total_patents = len(patents)
        ai_patents = sum(1 for p in patents if p.get("is_ai_patent", False))
        print(f"[signals] Patents saved for {company_name} ({ticker}): {total_patents} patents")

    # Calculate composite score (Leadership is None for now)
    composite_score = calculate_composite_score(
        hiring_score=job_market_score,
        innovation_score=patent_score,
        tech_stack_score=techstack_score,
        leadership_score=None,  # Blank for now
    )

    # Upsert to Snowflake (replace if ticker exists)
    try:
        scores_repo = SignalScoresRepository()
        scores_repo.upsert_signal_scores(
            company_id=company_id,
            company_name=company_name,
            ticker=ticker,
            hiring_score=job_market_score,
            innovation_score=patent_score,
            tech_stack_score=techstack_score,
            leadership_score=None,  # Blank for now
            composite_score=composite_score,
            total_jobs=total_jobs,
            ai_jobs=ai_jobs,
            total_patents=total_patents,
            ai_patents=ai_patents,
            techstack_keywords=techstack_keywords,
            s3_jobs_key=s3_jobs_key,
            s3_patents_key=s3_patents_key,
        )
        scores_repo.close()
        print(f"[signals] Snowflake upsert complete for {ticker}: composite_score={composite_score}")
    except Exception as e:
        print(f"[signals] Snowflake upsert failed for {company_name} ({ticker}): {e}")

    print(f"[signals] Collection complete for {company_name} ({ticker})")


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

    Requires a valid company_id from the Snowflake database.
    The company must have a ticker symbol for storage organization.

    This endpoint queues a background task to:
    1. Scrape job postings from LinkedIn, Indeed, Glassdoor
    2. Fetch patents from PatentsView API
    3. Extract tech stack keywords from job descriptions
    4. Store data locally organized by ticker:
       - data/signals/jobs/<TICKER>/
       - data/signals/patents/<TICKER>/

    Use GET endpoints with the ticker to retrieve the collected data.
    """
    # Look up company from Snowflake
    company_repo = CompanyRepository()
    company = company_repo.get_by_id(req.company_id)

    if company is None:
        raise HTTPException(
            status_code=404,
            detail=f"Company with ID '{req.company_id}' not found in database"
        )

    company_id = str(req.company_id)
    company_name = company["name"]
    ticker = company.get("ticker")

    if not ticker:
        raise HTTPException(
            status_code=400,
            detail=f"Company '{company_name}' does not have a ticker symbol. Ticker is required for signal storage."
        )

    background_tasks.add_task(
        _collect_signals_task,
        company_id=company_id,
        company_name=company_name,
        ticker=ticker,
        collect_jobs=req.collect_jobs,
        collect_patents=req.collect_patents,
        patents_years_back=req.patents_years_back,
    )

    return SignalCollectResponse(
        status="queued",
        message=f"Signal collection queued for {company_name} ({ticker})",
        company_id=req.company_id,
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
    companies = storage.list_all_companies()

    results = []
    for c in companies:
        try:
            # Use jobs_collected_at or patents_collected_at as collected_at
            collected_at = c.get("jobs_collected_at") or c.get("patents_collected_at") or ""
            results.append(StoredSignalSummary(
                company_id=c.get("company_id", ""),
                company_name=c.get("company_name", ""),
                ticker=c.get("ticker", ""),
                collected_at=collected_at,
                total_jobs=c.get("total_jobs", 0),
                ai_jobs=c.get("ai_jobs", 0),
                job_market_score=c.get("job_market_score"),
                total_patents=c.get("total_patents", 0),
                ai_patents=c.get("ai_patents", 0),
                patent_portfolio_score=c.get("patent_portfolio_score"),
                techstack_score=c.get("techstack_score"),
                techstack_keywords=c.get("techstack_keywords", []),
            ))
        except Exception:
            continue

    return results


# ============================================
# GET /job_postings - Retrieve stored job data
# ============================================

@router.get("/job_postings", response_model=JobPostingsResponse)
async def get_job_postings(
    ticker: str = Query(..., description="Company ticker symbol to retrieve job postings for"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of job postings to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Retrieve stored job postings for a company by ticker.

    Returns previously collected job data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_job_postings(ticker, limit=limit, offset=offset)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No job data found for ticker '{ticker.upper()}'. Use POST /collect to fetch data first."
        )

    # Convert to response models
    job_responses = []
    for job in data.get("job_postings", []):
        try:
            job_responses.append(JobPostingResponse(**job))
        except Exception:
            continue

    return JobPostingsResponse(
        company_id=data.get("company_id", ""),
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
    ticker: str = Query(..., description="Company ticker symbol to retrieve patents for"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of patents to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Retrieve stored patents for a company by ticker.

    Returns previously collected patent data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_patents(ticker, limit=limit, offset=offset)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No patent data found for ticker '{ticker.upper()}'. Use POST /collect to fetch data first."
        )

    # Convert to response models
    patent_responses = []
    for patent in data.get("patents", []):
        try:
            patent_responses.append(PatentResponse(**patent))
        except Exception:
            continue

    return PatentsResponse(
        company_id=data.get("company_id", ""),
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
    ticker: str = Query(..., description="Company ticker symbol to retrieve tech stack for"),
):
    """
    Retrieve stored tech stack data for a company by ticker.

    Returns previously collected tech stack data from local storage.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()
    data = storage.get_techstack(ticker)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No tech stack data found for ticker '{ticker.upper()}'. Use POST /collect to fetch data first."
        )

    keywords = data.get("techstack_keywords", [])
    ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
    techstack_score = data.get("techstack_score", 0)
    company_name = data.get("company_name", "")
    company_id = data.get("company_id", "")

    techstack = TechStackResponse(
        company_id=company_id,
        company_name=company_name,
        techstack_keywords=keywords,
        ai_tools_found=ai_tools,
        techstack_score=techstack_score,
        total_keywords=len(keywords),
        total_ai_tools=len(ai_tools),
    )

    return TechStacksResponse(
        company_id=company_id,
        company_name=company_name,
        techstack_score=techstack_score,
        techstacks=[techstack],
    )


# ============================================
# GET /all - Retrieve all stored signals
# ============================================

@router.get("/all", response_model=AllSignalsResponse)
async def get_all_signals(
    ticker: str = Query(..., description="Company ticker symbol to retrieve all signals for"),
    jobs_limit: int = Query(50, ge=1, le=200, description="Maximum number of job postings to return"),
    patents_limit: int = Query(50, ge=1, le=200, description="Maximum number of patents to return"),
):
    """
    Retrieve all stored signal data for a company by ticker.

    Returns previously collected job postings, patents, and tech stack data.
    Use POST /collect to trigger fresh data collection.
    """
    storage = SignalsStorage()

    # Check if company has any data
    summary = storage.get_combined_summary(ticker)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No signal data found for ticker '{ticker.upper()}'. Use POST /collect to fetch data first."
        )

    company_id = summary.get("company_id", "")
    company_name = summary.get("company_name", "")

    # Get job postings
    job_data = storage.get_job_postings(ticker, limit=jobs_limit, offset=0)
    job_responses = []
    if job_data:
        for job in job_data.get("job_postings", []):
            try:
                job_responses.append(JobPostingResponse(**job))
            except Exception:
                continue

    # Get patents
    patent_data = storage.get_patents(ticker, limit=patents_limit, offset=0)
    patent_responses = []
    if patent_data:
        for patent in patent_data.get("patents", []):
            try:
                patent_responses.append(PatentResponse(**patent))
            except Exception:
                continue

    # Get tech stack
    techstack_data = storage.get_techstack(ticker)
    techstacks = []
    if techstack_data:
        keywords = techstack_data.get("techstack_keywords", [])
        ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
        techstack_score = techstack_data.get("techstack_score", 0)

        techstacks.append(TechStackResponse(
            company_id=company_id,
            company_name=company_name,
            techstack_keywords=keywords,
            ai_tools_found=ai_tools,
            techstack_score=techstack_score,
            total_keywords=len(keywords),
            total_ai_tools=len(ai_tools),
        ))

    return AllSignalsResponse(
        company_id=company_id,
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


# ============================================
# GET /scores - Retrieve signal scores from Snowflake
# ============================================

@router.get("/scores", response_model=SignalScoresResponse)
async def get_signal_scores(
    ticker: str = Query(..., description="Company ticker symbol to retrieve scores for"),
):
    """
    Retrieve signal scores for a company from Snowflake.

    Scores include:
    - hiring_score: Job market/hiring signal (0-100)
    - innovation_score: Patent/innovation signal (0-100)
    - tech_stack_score: Tech stack signal (0-100)
    - leadership_score: Leadership signal (0-100) - blank for now
    - composite_score: Average of available scores

    Use POST /collect to trigger fresh data collection and score calculation.
    """
    try:
        scores_repo = SignalScoresRepository()
        scores = scores_repo.get_by_ticker(ticker)
        scores_repo.close()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve scores from Snowflake: {e}"
        )

    if scores is None:
        raise HTTPException(
            status_code=404,
            detail=f"No signal scores found for ticker '{ticker.upper()}'. Use POST /collect to fetch data first."
        )

    return SignalScoresResponse(**scores)


@router.get("/scores/all", response_model=List[SignalScoresResponse])
async def get_all_signal_scores():
    """
    Retrieve all signal scores from Snowflake.

    Returns a list of all companies with their signal scores.
    """
    try:
        scores_repo = SignalScoresRepository()
        all_scores = scores_repo.get_all()
        scores_repo.close()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve scores from Snowflake: {e}"
        )

    return [SignalScoresResponse(**s) for s in all_scores]
