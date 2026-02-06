"""
Evidence API Router
app/routers/evidence.py

Endpoints:
- GET  /api/v1/companies/{ticker}/evidence      - Get all collected evidence for a company
- POST /api/v1/evidence/backfill                 - Trigger full backfill for all 10 companies
- GET  /api/v1/evidence/backfill/tasks/{task_id} - Check backfill progress
"""

import asyncio
from fastapi import APIRouter, BackgroundTasks, HTTPException
from uuid import uuid4
from datetime import datetime, timezone
from typing import Dict, Any
import logging

from app.repositories.company_repository import CompanyRepository
from app.repositories.document_repository import get_document_repository
from app.repositories.signal_repository import get_signal_repository
from app.services.document_collector import get_document_collector_service
from app.services.job_signal_service import get_job_signal_service
from app.services.patent_signal_service import get_patent_signal_service
from app.services.tech_signal_service import get_tech_signal_service
from app.services.leadership_service import get_leadership_service
from app.models.document import DocumentCollectionRequest
from app.models.evidence import (
    CompanyEvidenceResponse,
    DocumentEvidence,
    SignalEvidence,
    SignalSummary,
    BackfillResponse,
    BackfillTaskStatus,
    BackfillProgress,
    BackfillStatus,
    CompanyBackfillResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Evidence"])

# Target tickers for backfill
TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]

# In-memory task store (in production, use Redis or database)
_backfill_task_store: Dict[str, Dict[str, Any]] = {}


@router.get(
    "/companies/{ticker}/evidence",
    response_model=CompanyEvidenceResponse,
    summary="Get all collected evidence for a company",
    description="Returns existing SEC filing documents and external signals stored in Snowflake for the given company ticker.",
)
async def get_company_evidence(ticker: str):
    """Retrieve all collected evidence (documents + signals) for a company by ticker."""
    ticker = ticker.upper()

    # Look up company
    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found for ticker: {ticker}")

    company_id = str(company["id"])

    # Fetch documents and signals from Snowflake
    doc_repo = get_document_repository()
    signal_repo = get_signal_repository()

    documents = doc_repo.get_by_ticker(ticker)
    signals = signal_repo.get_signals_by_ticker(ticker)
    summary = signal_repo.get_summary_by_ticker(ticker)

    # Build response
    doc_evidence = [
        DocumentEvidence(
            id=doc["id"],
            filing_type=doc.get("filing_type", ""),
            filing_date=doc.get("filing_date"),
            source_url=doc.get("source_url"),
            s3_key=doc.get("s3_key"),
            word_count=doc.get("word_count"),
            chunk_count=doc.get("chunk_count"),
            status=doc.get("status", "unknown"),
            created_at=doc.get("created_at"),
            processed_at=doc.get("processed_at"),
        )
        for doc in documents
    ]

    signal_evidence = [
        SignalEvidence(
            id=sig["id"],
            category=sig.get("category", ""),
            source=sig.get("source", ""),
            signal_date=sig.get("signal_date"),
            raw_value=sig.get("raw_value"),
            normalized_score=sig.get("normalized_score"),
            confidence=sig.get("confidence"),
            metadata=sig.get("metadata"),
            created_at=sig.get("created_at"),
        )
        for sig in signals
    ]

    signal_summary = None
    if summary:
        signal_summary = SignalSummary(
            technology_hiring_score=summary.get("technology_hiring_score"),
            innovation_activity_score=summary.get("innovation_activity_score"),
            digital_presence_score=summary.get("digital_presence_score"),
            leadership_signals_score=summary.get("leadership_signals_score"),
            composite_score=summary.get("composite_score"),
            signal_count=summary.get("signal_count", 0),
            last_updated=summary.get("last_updated"),
        )

    return CompanyEvidenceResponse(
        company_id=company_id,
        company_name=company.get("name", ""),
        ticker=ticker,
        documents=doc_evidence,
        document_count=len(doc_evidence),
        signals=signal_evidence,
        signal_count=len(signal_evidence),
        signal_summary=signal_summary,
    )


# =============================================================================
# POST /api/v1/evidence/backfill - Trigger full backfill
# =============================================================================

@router.post(
    "/evidence/backfill",
    response_model=BackfillResponse,
    summary="Trigger full evidence backfill for all companies",
    description="""
    Re-triggers the full evidence collection pipeline (SEC documents + external signals)
    for all 10 target companies. Companies are processed sequentially to respect rate limits,
    but SEC and signal pipelines run in parallel within each company.

    Returns a task_id immediately. Use GET /api/v1/evidence/backfill/tasks/{task_id} to check progress.
    """,
)
async def trigger_backfill(background_tasks: BackgroundTasks):
    """Trigger full evidence backfill for all 10 target companies."""
    task_id = str(uuid4())

    _backfill_task_store[task_id] = {
        "task_id": task_id,
        "status": BackfillStatus.QUEUED,
        "progress": {
            "companies_completed": 0,
            "total_companies": len(TARGET_TICKERS),
            "current_company": None,
        },
        "company_results": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
    }

    background_tasks.add_task(run_backfill, task_id)

    logger.info(f"Backfill queued: task_id={task_id}, companies={len(TARGET_TICKERS)}")

    return BackfillResponse(
        task_id=task_id,
        status=BackfillStatus.QUEUED,
        message=f"Backfill started for {len(TARGET_TICKERS)} companies. Poll /api/v1/evidence/backfill/tasks/{task_id} for progress.",
    )


# =============================================================================
# GET /api/v1/evidence/backfill/tasks/{task_id} - Check backfill progress
# =============================================================================

@router.get(
    "/evidence/backfill/tasks/{task_id}",
    response_model=BackfillTaskStatus,
    summary="Check backfill task progress",
    description="Returns per-company status, the current company being processed, and overall progress.",
)
async def get_backfill_status(task_id: str):
    """Check progress of a backfill task."""
    if task_id not in _backfill_task_store:
        raise HTTPException(status_code=404, detail=f"Backfill task not found: {task_id}")

    task = _backfill_task_store[task_id]
    return BackfillTaskStatus(
        task_id=task["task_id"],
        status=task["status"],
        progress=BackfillProgress(**task["progress"]),
        company_results=[CompanyBackfillResult(**r) for r in task["company_results"]],
        started_at=task["started_at"],
        completed_at=task["completed_at"],
    )


# =============================================================================
# Background Task: run_backfill
# =============================================================================

async def _collect_signals_for_company(ticker: str) -> Dict[str, Any]:
    """Run all 4 signal categories for a company. Returns a summary dict."""
    signal_results = {}
    errors = []

    categories = [
        ("technology_hiring", lambda: get_job_signal_service().analyze_company(ticker, force_refresh=True)),
        ("innovation_activity", lambda: get_patent_signal_service().analyze_company(ticker, years_back=5)),
        ("digital_presence", lambda: get_tech_signal_service().analyze_company(ticker, force_refresh=True)),
        ("leadership_signals", lambda: get_leadership_service().analyze_company(ticker)),
    ]

    for category, service_call in categories:
        try:
            result = await service_call()
            signal_results[category] = {
                "status": "success",
                "score": result.get("normalized_score") if isinstance(result, dict) else None,
            }
        except Exception as e:
            logger.error(f"Signal error for {ticker}/{category}: {e}")
            signal_results[category] = {"status": "failed", "error": str(e)}
            errors.append(f"{category}: {str(e)}")

    return {"signals": signal_results, "errors": errors}


async def _collect_sec_for_company(ticker: str) -> Dict[str, Any]:
    """Run SEC document collection for a company (sync → wrapped in to_thread)."""
    service = get_document_collector_service()
    request = DocumentCollectionRequest(ticker=ticker)
    result = await asyncio.to_thread(service.collect_for_company, request)
    return {
        "documents_found": result.documents_found,
        "documents_uploaded": result.documents_uploaded,
        "documents_skipped": result.documents_skipped,
        "documents_failed": result.documents_failed,
        "summary": result.summary,
    }


async def run_backfill(task_id: str):
    """Background task: process all companies sequentially, SEC + signals in parallel per company."""
    logger.info(f"Backfill started: task_id={task_id}")
    _backfill_task_store[task_id]["status"] = BackfillStatus.RUNNING

    has_errors = False

    for i, ticker in enumerate(TARGET_TICKERS):
        _backfill_task_store[task_id]["progress"]["current_company"] = ticker
        logger.info(f"Backfill [{i+1}/{len(TARGET_TICKERS)}]: Processing {ticker}")

        company_result = {"ticker": ticker, "status": "success", "sec_result": None, "signal_result": None, "error": None}

        try:
            # Run SEC and signals in parallel for this company
            sec_task = asyncio.create_task(_collect_sec_for_company(ticker))
            signal_task = asyncio.create_task(_collect_signals_for_company(ticker))

            sec_result, signal_result = await asyncio.gather(sec_task, signal_task, return_exceptions=True)

            # Handle SEC result
            if isinstance(sec_result, Exception):
                logger.error(f"SEC collection failed for {ticker}: {sec_result}")
                company_result["sec_result"] = {"status": "failed", "error": str(sec_result)}
                has_errors = True
            else:
                company_result["sec_result"] = sec_result

            # Handle signal result
            if isinstance(signal_result, Exception):
                logger.error(f"Signal collection failed for {ticker}: {signal_result}")
                company_result["signal_result"] = {"status": "failed", "error": str(signal_result)}
                has_errors = True
            else:
                company_result["signal_result"] = signal_result
                if signal_result.get("errors"):
                    has_errors = True

        except Exception as e:
            logger.error(f"Backfill failed for {ticker}: {e}")
            company_result["status"] = "failed"
            company_result["error"] = str(e)
            has_errors = True

        _backfill_task_store[task_id]["company_results"].append(company_result)
        _backfill_task_store[task_id]["progress"]["companies_completed"] = i + 1

    # Finalize
    _backfill_task_store[task_id]["progress"]["current_company"] = None
    _backfill_task_store[task_id]["status"] = (
        BackfillStatus.COMPLETED_WITH_ERRORS if has_errors else BackfillStatus.COMPLETED
    )
    _backfill_task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"Backfill finished: task_id={task_id}, status={_backfill_task_store[task_id]['status']}")
