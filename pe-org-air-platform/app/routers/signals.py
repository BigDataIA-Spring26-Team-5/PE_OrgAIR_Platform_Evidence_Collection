"""
Signals Router - Job Postings, Patents, and Tech Stack Data
app/routers/signals.py

Provides API endpoints for collecting and returning AI-related signals for companies.
Each GET endpoint triggers the relevant pipeline to collect fresh data.
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.signal_responses import (
    JobPostingsResponse,
    PatentsResponse,
    TechStacksResponse,
    AllSignalsResponse,
    JobPostingResponse,
    PatentResponse,
    TechStackResponse
)
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.keywords import TOP_AI_TOOLS
from app.pipelines.utils import Company

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


def _create_state(company_name: str, output_dir: str) -> Pipeline2State:
    """Create a pipeline state for a single company."""
    return Pipeline2State(
        companies=[Company.from_name(company_name, 0).to_dict()],
        output_dir=output_dir
    )


@router.get("/job_postings", response_model=JobPostingsResponse)
async def get_job_postings(
    company_name: str = Query(..., description="Company name to collect job postings for"),
    limit: int = Query(100, description="Maximum number of job postings to return"),
    offset: int = Query(0, description="Offset for pagination")
):
    """
    Collect and return job postings for a company.

    Triggers the job scraping pipeline to collect fresh data from job sites,
    classifies AI-related roles, and returns the results.
    """
    try:
        state = _create_state(company_name, "data/signals/jobs")
        state = await run_job_signals(state, use_cloud_storage=False)

        # Convert to response models
        job_responses = []
        for job in state.job_postings[offset:offset + limit]:
            try:
                job_responses.append(JobPostingResponse(**job))
            except Exception:
                continue

        total_count = len(state.job_postings)
        ai_count = sum(1 for job in state.job_postings if job.get("is_ai_role", False))
        job_market_score = state.job_market_scores.get("company-0")

        return JobPostingsResponse(
            company_id="company-0",
            company_name=company_name,
            total_count=total_count,
            ai_count=ai_count,
            job_market_score=job_market_score,
            job_postings=job_responses
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job collection failed: {e}")


@router.get("/patents", response_model=PatentsResponse)
async def get_patents(
    company_name: str = Query(..., description="Company name to collect patents for"),
    limit: int = Query(100, description="Maximum number of patents to return"),
    offset: int = Query(0, description="Offset for pagination"),
    years_back: int = Query(5, description="Years back to search for patents")
):
    """
    Collect and return patents for a company.

    Triggers the patent collection pipeline to fetch data from PatentsView API,
    classifies AI-related patents, and returns the results.
    """
    try:
        state = _create_state(company_name, "data/signals/patents")
        state = await run_patent_signals(state, years_back=years_back)

        # Convert to response models
        patent_responses = []
        for patent in state.patents[offset:offset + limit]:
            try:
                patent_responses.append(PatentResponse(**patent))
            except Exception:
                continue

        total_count = len(state.patents)
        ai_count = sum(1 for p in state.patents if p.get("is_ai_patent", False))
        patent_score = state.patent_scores.get("company-0")

        return PatentsResponse(
            company_id="company-0",
            company_name=company_name,
            total_count=total_count,
            ai_count=ai_count,
            patent_portfolio_score=patent_score,
            patents=patent_responses
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Patent collection failed: {e}")


@router.get("/tech_stacks", response_model=TechStacksResponse)
async def get_tech_stacks(
    company_name: str = Query(..., description="Company name to collect tech stack for")
):
    """
    Collect and return tech stack data for a company.

    Triggers the job scraping pipeline to collect job postings, then extracts
    technology keywords and AI tools mentioned in the job descriptions.
    """
    try:
        state = _create_state(company_name, "data/signals/jobs")
        state = await run_job_signals(state, use_cloud_storage=False)

        # Build techstack response
        keywords = state.company_techstacks.get("company-0", [])
        ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
        techstack_score = state.techstack_scores.get("company-0", 0)

        techstack = TechStackResponse(
            company_id="company-0",
            company_name=company_name,
            techstack_keywords=keywords,
            ai_tools_found=ai_tools,
            techstack_score=techstack_score,
            total_keywords=len(keywords),
            total_ai_tools=len(ai_tools)
        )

        return TechStacksResponse(
            company_id="company-0",
            company_name=company_name,
            techstack_score=techstack_score,
            techstacks=[techstack]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tech stack collection failed: {e}")


@router.get("/all", response_model=AllSignalsResponse)
async def get_all_signals(
    company_name: str = Query(..., description="Company name to collect all signals for"),
    jobs_limit: int = Query(50, description="Maximum number of job postings to return"),
    patents_limit: int = Query(50, description="Maximum number of patents to return"),
    patents_years_back: int = Query(5, description="Years back to search for patents")
):
    """
    Collect and return all signal data for a company.

    Triggers both job scraping and patent collection pipelines, then returns
    combined results including job postings, patents, and tech stack data.
    """
    try:
        # Collect jobs and tech stack
        job_state = _create_state(company_name, "data/signals/jobs")
        job_state = await run_job_signals(job_state, use_cloud_storage=False)

        # Collect patents
        patent_state = _create_state(company_name, "data/signals/patents")
        patent_state = await run_patent_signals(patent_state, years_back=patents_years_back)

        # Convert job postings
        job_responses = []
        for job in job_state.job_postings[:jobs_limit]:
            try:
                job_responses.append(JobPostingResponse(**job))
            except Exception:
                continue

        # Convert patents
        patent_responses = []
        for patent in patent_state.patents[:patents_limit]:
            try:
                patent_responses.append(PatentResponse(**patent))
            except Exception:
                continue

        # Build techstack
        keywords = job_state.company_techstacks.get("company-0", [])
        ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
        techstack_score = job_state.techstack_scores.get("company-0", 0)

        techstack = TechStackResponse(
            company_id="company-0",
            company_name=company_name,
            techstack_keywords=keywords,
            ai_tools_found=ai_tools,
            techstack_score=techstack_score,
            total_keywords=len(keywords),
            total_ai_tools=len(ai_tools)
        )

        return AllSignalsResponse(
            company_id="company-0",
            company_name=company_name,
            job_market_score=job_state.job_market_scores.get("company-0"),
            patent_portfolio_score=patent_state.patent_scores.get("company-0"),
            techstack_score=techstack_score,
            total_jobs=len(job_state.job_postings),
            ai_jobs=sum(1 for j in job_state.job_postings if j.get("is_ai_role", False)),
            total_patents=len(patent_state.patents),
            ai_patents=sum(1 for p in patent_state.patents if p.get("is_ai_patent", False)),
            job_postings=job_responses,
            patents=patent_responses,
            techstacks=[techstack]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Signal collection failed: {e}")
