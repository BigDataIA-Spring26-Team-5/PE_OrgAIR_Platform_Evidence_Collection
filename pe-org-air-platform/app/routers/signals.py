"""
Signals Router - Job Postings, Patents, and Tech Stack Data
app/routers/signals.py

Provides API endpoints for accessing job postings, patents, and tech stack data
collected by Pipeline 2 (pipeline2_runner.py).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse

from app.models.signal_responses import (
    JobPostingsResponse,
    PatentsResponse,
    TechStacksResponse,
    AllSignalsResponse,
    SignalCollectionRequest,
    JobPostingResponse,
    PatentResponse,
    TechStackResponse
)
from app.pipelines.pipeline2_runner import run_pipeline2
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.keywords import TOP_AI_TOOLS
from app.pipelines.utils import Company, safe_filename

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])

# Cache with TTL support
CACHE_TTL_MINUTES = 60
_pipeline_cache: Dict[str, Dict[str, Any]] = {}


def _cleanup_expired_cache():
    """Remove expired entries from cache."""
    now = datetime.now()
    expired = [
        job_id for job_id, entry in _pipeline_cache.items()
        if datetime.fromisoformat(entry["timestamp"]) < now - timedelta(minutes=CACHE_TTL_MINUTES)
    ]
    for job_id in expired:
        del _pipeline_cache[job_id]


def _cache_result(job_id: str, result_type: str, state: Optional[Pipeline2State] = None,
                  error: Optional[str] = None):
    """Cache a pipeline result with timestamp."""
    _cleanup_expired_cache()
    entry = {"type": result_type, "timestamp": datetime.now().isoformat()}
    if error:
        entry["error"] = error
        entry["status"] = "failed"
    else:
        entry["state"] = state
    _pipeline_cache[job_id] = entry


# =============================================================================
# File Loading Utilities
# =============================================================================

def _load_json_file(file_path: Path) -> Any:
    """Load JSON data from file."""
    try:
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
    return []


def _find_latest_json_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the latest JSON file matching pattern in directory."""
    try:
        if not directory.exists():
            return None
        json_files = list(directory.glob(pattern))
        if not json_files:
            return None
        json_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        return json_files[0]
    except Exception:
        return None


# =============================================================================
# Data Loading Functions
# =============================================================================

def _load_job_data(company_name: Optional[str] = None) -> Dict[str, Any]:
    """Load job data from files."""
    jobs_dir = Path("data/signals/jobs")
    result = {
        "job_postings": [],
        "job_market_scores": {},
        "techstack_scores": {},
        "company_techstacks": {}
    }

    if not jobs_dir.exists():
        return result

    # Try company-specific file first
    if company_name:
        safe_name = safe_filename(company_name)
        company_file = _find_latest_json_file(jobs_dir, f"{safe_name}_*.json")
        if company_file:
            company_data = _load_json_file(company_file)
            if company_data and isinstance(company_data, dict):
                result["job_postings"] = company_data.get("jobs", [])
                result["job_market_scores"][company_name] = company_data.get("job_market_score", 0)
                result["techstack_scores"][company_name] = company_data.get("techstack_score", 0)
                result["company_techstacks"][company_name] = company_data.get("techstack_keywords", [])
                return result

    # Load all jobs
    all_jobs_file = _find_latest_json_file(jobs_dir, "all_jobs_*.json")
    if all_jobs_file:
        result["job_postings"] = _load_json_file(all_jobs_file)

    # Load summary for scores
    summary_file = _find_latest_json_file(jobs_dir, "summary_*.json")
    if summary_file:
        summary_data = _load_json_file(summary_file)
        if summary_data and isinstance(summary_data, dict):
            result["job_market_scores"] = summary_data.get("job_market_scores", {})
            result["techstack_scores"] = summary_data.get("techstack_scores", {})
            result["company_techstacks"] = summary_data.get("company_techstacks", {})

    return result


def _load_patent_data(company_name: Optional[str] = None) -> Dict[str, Any]:
    """Load patent data from files."""
    patents_dir = Path("data/signals/patents")
    result = {"patents": [], "patent_scores": {}}

    if not patents_dir.exists():
        return result

    # Try company-specific file first
    if company_name:
        safe_name = safe_filename(company_name)
        company_file = _find_latest_json_file(patents_dir, f"{safe_name}_patents_*.json")
        if company_file:
            company_data = _load_json_file(company_file)
            if company_data and isinstance(company_data, dict):
                result["patents"] = company_data.get("patents", [])
                result["patent_scores"][company_name] = company_data.get("patent_portfolio_score", 0)
                return result

    # Load all patents
    all_patents_file = _find_latest_json_file(patents_dir, "all_patents_*.json")
    if all_patents_file:
        result["patents"] = _load_json_file(all_patents_file)

    # Load summary for scores
    summary_file = _find_latest_json_file(patents_dir, "patent_summary_*.json")
    if summary_file:
        summary_data = _load_json_file(summary_file)
        if summary_data and isinstance(summary_data, dict):
            result["patent_scores"] = summary_data.get("patent_scores", {})

    return result


def _build_techstacks(job_data: Dict[str, Any]) -> List[TechStackResponse]:
    """Build techstack responses from job data."""
    techstacks = []
    for company_id, keywords in job_data.get("company_techstacks", {}).items():
        ai_tools = [k for k in keywords if k in TOP_AI_TOOLS]
        techstacks.append(TechStackResponse(
            company_id=company_id,
            company_name=company_id,
            techstack_keywords=keywords,
            ai_tools_found=ai_tools,
            techstack_score=job_data.get("techstack_scores", {}).get(company_id, 0),
            total_keywords=len(keywords),
            total_ai_tools=len(ai_tools)
        ))
    return techstacks


def _filter_by_company(items: List[Dict], company_name: str) -> List[Dict]:
    """Filter items by company name."""
    return [
        item for item in items
        if item.get("company_name", "").lower() == company_name.lower()
        or item.get("company_id", "").lower() == company_name.lower()
    ]


def _average_scores(scores: Dict[str, float]) -> Optional[float]:
    """Calculate average of scores."""
    if not scores:
        return None
    values = list(scores.values())
    return sum(values) / len(values) if values else None


# =============================================================================
# GET Endpoints - Data Retrieval
# =============================================================================

@router.get("/job_postings", response_model=JobPostingsResponse)
async def get_job_postings(
    company_name: Optional[str] = Query(None, description="Filter by company name"),
    limit: int = Query(100, description="Maximum number of job postings to return"),
    offset: int = Query(0, description="Offset for pagination")
):
    """Get job postings collected by Pipeline 2."""
    job_data = _load_job_data(company_name)

    postings = job_data["job_postings"]
    if company_name and not job_data.get("company_techstacks", {}).get(company_name):
        postings = _filter_by_company(postings, company_name)

    # Convert to response models
    job_responses = []
    for job in postings[offset:offset + limit]:
        try:
            job_responses.append(JobPostingResponse(**job))
        except Exception:
            continue

    total_count = len(postings)
    ai_count = sum(1 for job in postings if job.get("is_ai_role", False))

    job_market_score = (
        job_data["job_market_scores"].get(company_name) if company_name
        else _average_scores(job_data["job_market_scores"])
    )

    return JobPostingsResponse(
        company_id=company_name,
        company_name=job_responses[0].company_name if job_responses and company_name else None,
        total_count=total_count,
        ai_count=ai_count,
        job_market_score=job_market_score,
        job_postings=job_responses
    )


@router.get("/patents", response_model=PatentsResponse)
async def get_patents(
    company_name: Optional[str] = Query(None, description="Filter by company name"),
    limit: int = Query(100, description="Maximum number of patents to return"),
    offset: int = Query(0, description="Offset for pagination")
):
    """Get patents collected by Pipeline 2."""
    patent_data = _load_patent_data(company_name)

    patents = patent_data["patents"]
    if company_name and not patent_data.get("patent_scores", {}).get(company_name):
        patents = _filter_by_company(patents, company_name)

    # Convert to response models
    patent_responses = []
    for patent in patents[offset:offset + limit]:
        try:
            patent_responses.append(PatentResponse(**patent))
        except Exception:
            continue

    total_count = len(patents)
    ai_count = sum(1 for p in patents if p.get("is_ai_patent", False))

    patent_score = (
        patent_data["patent_scores"].get(company_name) if company_name
        else _average_scores(patent_data["patent_scores"])
    )

    return PatentsResponse(
        company_id=company_name,
        company_name=patent_responses[0].company_name if patent_responses and company_name else None,
        total_count=total_count,
        ai_count=ai_count,
        patent_portfolio_score=patent_score,
        patents=patent_responses
    )


@router.get("/tech_stacks", response_model=TechStacksResponse)
async def get_tech_stacks(
    company_name: Optional[str] = Query(None, description="Filter by company name")
):
    """Get tech stack data collected by Pipeline 2."""
    job_data = _load_job_data(company_name)
    techstacks = _build_techstacks(job_data)

    if company_name:
        techstacks = [
            ts for ts in techstacks
            if ts.company_name.lower() == company_name.lower()
            or ts.company_id.lower() == company_name.lower()
        ]

    techstack_score = (
        job_data["techstack_scores"].get(company_name) if company_name
        else _average_scores(job_data["techstack_scores"])
    )

    return TechStacksResponse(
        company_id=company_name,
        company_name=company_name,
        techstack_score=techstack_score,
        techstacks=techstacks
    )


@router.get("/all", response_model=AllSignalsResponse)
async def get_all_signals(
    company_name: Optional[str] = Query(None, description="Filter by company name"),
    jobs_limit: int = Query(50, description="Maximum number of job postings to return"),
    patents_limit: int = Query(50, description="Maximum number of patents to return")
):
    """Get all signal data (job postings, patents, and tech stacks) for a company."""
    job_data = _load_job_data(company_name)
    patent_data = _load_patent_data(company_name)

    postings = job_data["job_postings"]
    patents = patent_data["patents"]

    if company_name:
        postings = _filter_by_company(postings, company_name)
        patents = _filter_by_company(patents, company_name)

    # Convert to response models
    job_responses = [JobPostingResponse(**j) for j in postings[:jobs_limit] if _safe_convert(j, JobPostingResponse)]
    patent_responses = [PatentResponse(**p) for p in patents[:patents_limit] if _safe_convert(p, PatentResponse)]

    techstacks = _build_techstacks(job_data)
    if company_name:
        techstacks = [ts for ts in techstacks if ts.company_name.lower() == company_name.lower()]

    return AllSignalsResponse(
        company_id=company_name,
        company_name=company_name,
        job_market_score=job_data["job_market_scores"].get(company_name) if company_name else _average_scores(job_data["job_market_scores"]),
        patent_portfolio_score=patent_data["patent_scores"].get(company_name) if company_name else _average_scores(patent_data["patent_scores"]),
        techstack_score=job_data["techstack_scores"].get(company_name) if company_name else _average_scores(job_data["techstack_scores"]),
        total_jobs=len(postings),
        ai_jobs=sum(1 for j in postings if j.get("is_ai_role", False)),
        total_patents=len(patents),
        ai_patents=sum(1 for p in patents if p.get("is_ai_patent", False)),
        job_postings=job_responses,
        patents=patent_responses,
        techstacks=techstacks
    )


def _safe_convert(data: Dict, model_class) -> bool:
    """Check if data can be converted to model."""
    try:
        model_class(**data)
        return True
    except Exception:
        return False


@router.get("/available_companies")
async def get_available_companies():
    """Get list of companies with available signal data."""
    job_data = _load_job_data()
    patent_data = _load_patent_data()
    techstacks = _build_techstacks(job_data)

    companies = set()
    for job in job_data["job_postings"]:
        if name := job.get("company_name"):
            companies.add(name)
    for patent in patent_data["patents"]:
        if name := patent.get("company_name"):
            companies.add(name)
    for ts in techstacks:
        if ts.company_name:
            companies.add(ts.company_name)

    return {
        "available_companies": sorted(companies),
        "total_companies": len(companies),
        "job_postings_count": len(job_data["job_postings"]),
        "patents_count": len(patent_data["patents"]),
        "techstacks_count": len(techstacks)
    }


@router.get("/collection_status/{job_id}")
async def get_collection_status(job_id: str):
    """Get the status of a signal collection job."""
    status_file = Path("data/signals") / f"{job_id}_status.json"

    if not status_file.exists():
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    try:
        with open(status_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading status file: {e}")


@router.get("/recent/{job_id}")
async def get_recent_collection(job_id: str):
    """Get results from a recent pipeline collection job."""
    _cleanup_expired_cache()

    if job_id not in _pipeline_cache:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found in cache")

    entry = _pipeline_cache[job_id]

    if "error" in entry:
        return {"job_id": job_id, "status": "failed", "error": entry["error"], "timestamp": entry["timestamp"]}

    state = entry["state"]
    collection_type = entry["type"]

    base_response = {"job_id": job_id, "type": collection_type, "status": "completed", "timestamp": entry["timestamp"]}

    if collection_type == "jobs":
        return {
            **base_response,
            "job_postings_collected": len(state.job_postings),
            "companies_processed": len(state.companies),
            "job_market_scores": state.job_market_scores,
            "techstack_scores": state.techstack_scores,
            "summary": state.summary
        }
    elif collection_type == "patents":
        return {
            **base_response,
            "patents_collected": len(state.patents),
            "companies_processed": len(state.companies),
            "patent_scores": state.patent_scores,
            "summary": state.summary
        }

    raise HTTPException(status_code=500, detail=f"Unknown collection type for job {job_id}")


# =============================================================================
# POST Endpoints - Data Collection
# =============================================================================

@router.post("/collect")
async def collect_signals(request: SignalCollectionRequest, background_tasks: BackgroundTasks):
    """Trigger Pipeline 2 to collect job postings, patents, and tech stack data."""
    job_id = f"signal_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    async def run_pipeline_background():
        try:
            state = await run_pipeline2(
                companies=request.companies,
                mode=request.mode,
                jobs_output_dir="data/signals/jobs",
                patents_output_dir="data/signals/patents",
                jobs_results_per_company=request.jobs_results_per_company,
                patents_results_per_company=request.patents_results_per_company,
                patents_years_back=request.years_back,
                use_cloud_storage=request.use_cloud_storage,
            )
            _save_collection_status(job_id, state, request)
        except Exception as e:
            _save_collection_error(job_id, str(e), request)

    background_tasks.add_task(run_pipeline_background)

    return JSONResponse(status_code=202, content={
        "job_id": job_id,
        "status": "accepted",
        "message": "Signal collection started in background",
        "companies": request.companies,
        "mode": request.mode
    })


def _save_collection_status(job_id: str, state: Pipeline2State, request: SignalCollectionRequest):
    """Save successful collection status to file."""
    status_file = Path("data/signals") / f"{job_id}_status.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)

    status_data = {
        "job_id": job_id,
        "status": "completed",
        "completed_at": datetime.now().isoformat(),
        "summary": state.summary,
        "companies_processed": len(state.companies),
        "job_postings_collected": len(state.job_postings),
        "patents_collected": len(state.patents),
        "job_market_scores": state.job_market_scores,
        "patent_scores": state.patent_scores,
        "techstack_scores": state.techstack_scores,
        "errors": state.summary.get("errors", [])
    }

    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status_data, f, indent=2, default=str)


def _save_collection_error(job_id: str, error: str, request: SignalCollectionRequest):
    """Save collection error status to file."""
    status_file = Path("data/signals") / f"{job_id}_status.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)

    error_data = {
        "job_id": job_id,
        "status": "failed",
        "failed_at": datetime.now().isoformat(),
        "error": error,
        "companies": request.companies,
        "mode": request.mode
    }

    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(error_data, f, indent=2)


@router.post("/collect/jobs")
async def collect_jobs(
    background_tasks: BackgroundTasks,
    company_names: List[str] = Query(..., description="List of company names"),
    results_per_company: int = Query(50, description="Maximum job postings per company"),
    use_cloud_storage: bool = Query(True, description="Store in S3 + Snowflake")
):
    """Collect job postings for specified companies."""
    job_id = f"job_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    async def run_job_collection():
        try:
            state = Pipeline2State(
                companies=[Company.from_name(name, i).to_dict() for i, name in enumerate(company_names)],
                output_dir="data/signals/jobs"
            )
            state = await run_job_signals(state, use_cloud_storage=use_cloud_storage)
            _cache_result(job_id, "jobs", state=state)
        except Exception as e:
            _cache_result(job_id, "jobs", error=str(e))

    background_tasks.add_task(run_job_collection)

    return JSONResponse(status_code=202, content={
        "job_id": job_id,
        "status": "accepted",
        "message": "Job collection started in background",
        "companies": company_names
    })


@router.post("/collect/patents")
async def collect_patents(
    background_tasks: BackgroundTasks,
    company_names: List[str] = Query(..., description="List of company names"),
    years_back: int = Query(5, description="Years back to search for patents"),
    results_per_company: int = Query(100, description="Maximum patents per company"),
    api_key: Optional[str] = Query(None, description="PatentsView API key")
):
    """Collect patents for specified companies."""
    job_id = f"patent_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    async def run_patent_collection():
        try:
            state = Pipeline2State(
                companies=[Company.from_name(name, i).to_dict() for i, name in enumerate(company_names)],
                output_dir="data/signals/patents"
            )
            state = await run_patent_signals(state, years_back=years_back,
                                             results_per_company=results_per_company, api_key=api_key)
            _cache_result(job_id, "patents", state=state)
        except Exception as e:
            _cache_result(job_id, "patents", error=str(e))

    background_tasks.add_task(run_patent_collection)

    return JSONResponse(status_code=202, content={
        "job_id": job_id,
        "status": "accepted",
        "message": "Patent collection started in background",
        "companies": company_names
    })


@router.post("/collect/realtime")
async def collect_realtime(
    company_name: str = Query(..., description="Company name"),
    collect_jobs: bool = Query(True, description="Collect job postings"),
    collect_patents: bool = Query(True, description="Collect patents"),
    jobs_results: int = Query(50, description="Max job postings"),
    patents_results: int = Query(100, description="Max patents"),
    patents_years_back: int = Query(5, description="Years back for patent search")
):
    """Collect real-time data for a single company (synchronous)."""
    job_id = f"realtime_{company_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        results = {
            "job_id": job_id,
            "company_name": company_name,
            "status": "processing",
            "timestamp": datetime.now().isoformat()
        }

        company_dict = Company.from_name(company_name, 0).to_dict()

        if collect_jobs:
            job_state = Pipeline2State(companies=[company_dict], output_dir="data/signals/jobs")
            job_state = await run_job_signals(job_state, use_cloud_storage=False)
            results["job_postings"] = {
                "collected": len(job_state.job_postings),
                "ai_jobs": sum(1 for j in job_state.job_postings if j.get("is_ai_role")),
                "job_market_score": job_state.job_market_scores.get("company-0", 0),
                "techstack_score": job_state.techstack_scores.get("company-0", 0),
                "sample_jobs": job_state.job_postings[:5]
            }

        if collect_patents:
            patent_state = Pipeline2State(companies=[company_dict], output_dir="data/signals/patents")
            patent_state = await run_patent_signals(patent_state, years_back=patents_years_back,
                                                    results_per_company=patents_results)
            results["patents"] = {
                "collected": len(patent_state.patents),
                "ai_patents": sum(1 for p in patent_state.patents if p.get("is_ai_patent")),
                "patent_portfolio_score": patent_state.patent_scores.get("company-0", 0),
                "sample_patents": patent_state.patents[:5]
            }

        results["status"] = "completed"
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Real-time collection failed: {e}")
