# """
# Evidence API Router
# app/routers/evidence.py

# Endpoints:
# - GET  /api/v1/companies/{ticker}/evidence      - Get all collected evidence for a company
# - GET  /api/v1/evidence/stats                    - Get evidence collection statistics
# - POST /api/v1/evidence/backfill                 - Trigger full backfill for all 10 companies
# - GET  /api/v1/evidence/backfill/tasks/{task_id} - Check backfill progress
# """

# import asyncio
# import json
# from fastapi import APIRouter, BackgroundTasks, HTTPException
# from uuid import uuid4
# from datetime import datetime, timezone
# from typing import Dict, Any
# import logging

# from app.repositories.company_repository import CompanyRepository
# from app.repositories.document_repository import get_document_repository
# from app.repositories.signal_repository import get_signal_repository
# from app.repositories.signal_scores_repository import SignalScoresRepository
# from app.services.document_collector import get_document_collector_service
# from app.services.job_signal_service import get_job_signal_service
# from app.services.patent_signal_service import get_patent_signal_service
# from app.services.tech_signal_service import get_tech_signal_service
# from app.services.leadership_service import get_leadership_service
# from app.models.document import DocumentCollectionRequest
# from app.models.evidence import (
#     CompanyEvidenceResponse,
#     DocumentEvidence,
#     SignalEvidence,
#     SignalSummary,
#     BackfillResponse,
#     BackfillTaskStatus,
#     BackfillProgress,
#     BackfillStatus,
#     CompanyBackfillResult,
#     EvidenceStatsResponse,
#     CompanyDocumentStat,
#     CompanySignalStat,
#     SignalCategoryBreakdown,
# )

# logger = logging.getLogger(__name__)

# router = APIRouter(prefix="/api/v1", tags=["Evidence"])

# # Target tickers for backfill
# TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]

# # In-memory task store (in production, use Redis or database)
# _backfill_task_store: Dict[str, Dict[str, Any]] = {}


# @router.get(
#     "/companies/{ticker}/evidence",
#     response_model=CompanyEvidenceResponse,
#     summary="Get all collected evidence for a company",
#     description="Returns existing SEC filing documents and external signals stored in Snowflake for the given company ticker.",
# )
# async def get_company_evidence(ticker: str):
#     """Retrieve all collected evidence (documents + signals) for a company by ticker."""
#     ticker = ticker.upper()

#     # Look up company
#     company_repo = CompanyRepository()
#     company = company_repo.get_by_ticker(ticker)
#     if not company:
#         raise HTTPException(status_code=404, detail=f"Company not found for ticker: {ticker}")

#     company_id = str(company["id"])

#     # Fetch documents and signals from Snowflake
#     doc_repo = get_document_repository()
#     signal_repo = get_signal_repository()

#     documents = doc_repo.get_by_ticker(ticker)
#     signals = signal_repo.get_signals_by_ticker(ticker)
#     summary = signal_repo.get_summary_by_ticker(ticker)

#     # Build response
#     doc_evidence = [
#         DocumentEvidence(
#             id=doc["id"],
#             filing_type=doc.get("filing_type", ""),
#             filing_date=doc.get("filing_date"),
#             source_url=doc.get("source_url"),
#             s3_key=doc.get("s3_key"),
#             word_count=doc.get("word_count"),
#             chunk_count=doc.get("chunk_count"),
#             status=doc.get("status", "unknown"),
#             created_at=doc.get("created_at"),
#             processed_at=doc.get("processed_at"),
#         )
#         for doc in documents
#     ]

#     signal_evidence = [
#         SignalEvidence(
#             id=sig["id"],
#             category=sig.get("category", ""),
#             source=sig.get("source", ""),
#             signal_date=sig.get("signal_date"),
#             raw_value=sig.get("raw_value"),
#             normalized_score=sig.get("normalized_score"),
#             confidence=sig.get("confidence"),
#             metadata=sig.get("metadata"),
#             created_at=sig.get("created_at"),
#         )
#         for sig in signals
#     ]

#     signal_summary = None
#     if summary:
#         signal_summary = SignalSummary(
#             technology_hiring_score=summary.get("technology_hiring_score"),
#             innovation_activity_score=summary.get("innovation_activity_score"),
#             digital_presence_score=summary.get("digital_presence_score"),
#             leadership_signals_score=summary.get("leadership_signals_score"),
#             composite_score=summary.get("composite_score"),
#             signal_count=summary.get("signal_count", 0),
#             last_updated=summary.get("last_updated"),
#         )

#     return CompanyEvidenceResponse(
#         company_id=company_id,
#         company_name=company.get("name", ""),
#         ticker=ticker,
#         documents=doc_evidence,
#         document_count=len(doc_evidence),
#         signals=signal_evidence,
#         signal_count=len(signal_evidence),
#         signal_summary=signal_summary,
#     )


# 
# # GET /api/v1/evidence/stats - Evidence collection statistics
# 

# @router.get(
#     "/evidence/stats",
#     response_model=EvidenceStatsResponse,
#     summary="Get evidence collection statistics",
#     description="""
#     Returns comprehensive statistics across all companies including:
#     - Document counts by filing type and processing status
#     - Document freshness (last collected / last processed per company)
#     - Signal scores with detailed metrics (jobs, patents, tech keywords)
#     - Signal breakdown by category with average scores and confidence
#     """,
# )
# async def get_evidence_stats():
#     """Get evidence collection statistics."""
#     doc_repo = get_document_repository()
#     signal_repo = get_signal_repository()
#     scores_repo = SignalScoresRepository()

#     # Document stats
#     doc_summary = doc_repo.get_summary_statistics()
#     status_breakdown = doc_repo.get_status_breakdown()
#     company_doc_stats = doc_repo.get_all_company_stats()
#     freshness = {r["ticker"]: r for r in doc_repo.get_freshness_by_ticker()}

#     # Signal stats
#     total_signals = signal_repo.get_total_signal_count()
#     category_breakdown = signal_repo.get_category_breakdown()

#     # Detailed signal scores (jobs, patents, keywords)
#     all_scores = scores_repo.get_all()

#     # Build per-company document stats with freshness
#     doc_stats = []
#     for s in company_doc_stats:
#         ticker = s["ticker"]
#         fresh = freshness.get(ticker, {})
#         last_collected = fresh.get("last_collected")
#         last_processed = fresh.get("last_processed")
#         doc_stats.append(CompanyDocumentStat(
#             **s,
#             last_collected=last_collected.isoformat() if last_collected else None,
#             last_processed=last_processed.isoformat() if last_processed else None,
#         ))

#     # Build per-company signal stats from SIGNAL_SCORES table
#     signal_stats = []
#     for sc in all_scores:
#         total_jobs = sc.get("total_jobs", 0)
#         ai_jobs = sc.get("ai_jobs", 0)
#         total_patents = sc.get("total_patents", 0)
#         ai_patents = sc.get("ai_patents", 0)
#         updated_at = sc.get("updated_at")
#         signal_stats.append(CompanySignalStat(
#             ticker=sc.get("ticker", ""),
#             hiring_score=sc.get("hiring_score"),
#             innovation_score=sc.get("innovation_score"),
#             tech_stack_score=sc.get("tech_stack_score"),
#             leadership_score=sc.get("leadership_score"),
#             composite_score=sc.get("composite_score"),
#             signal_count=signal_repo._get_signal_count(sc.get("company_id", "")),
#             total_jobs=total_jobs,
#             ai_jobs=ai_jobs,
#             ai_job_ratio=round(ai_jobs / total_jobs * 100, 1) if total_jobs > 0 else None,
#             total_patents=total_patents,
#             ai_patents=ai_patents,
#             ai_patent_ratio=round(ai_patents / total_patents * 100, 1) if total_patents > 0 else None,
#             techstack_keywords=json.loads(sc["techstack_keywords"]) if isinstance(sc.get("techstack_keywords"), str) else (sc.get("techstack_keywords") or []),
#             last_updated=updated_at.isoformat() if updated_at else None,
#         ))

#     return EvidenceStatsResponse(
#         companies_tracked=doc_summary["companies_processed"],
#         total_documents=doc_summary["total_documents"],
#         total_chunks=doc_summary["total_chunks"],
#         total_words=doc_summary["total_words"],
#         total_signals=total_signals,
#         documents_by_status=status_breakdown,
#         signals_by_category=[
#             SignalCategoryBreakdown(
#                 category=c.get("category", ""),
#                 count=c.get("count", 0),
#                 avg_score=round(c["avg_score"], 2) if c.get("avg_score") is not None else None,
#                 avg_confidence=round(c["avg_confidence"], 3) if c.get("avg_confidence") is not None else None,
#             )
#             for c in category_breakdown
#         ],
#         documents_by_company=doc_stats,
#         signals_by_company=signal_stats,
#     )


# 
# # POST /api/v1/evidence/backfill - Trigger full backfill
# 

# @router.post(
#     "/evidence/backfill",
#     response_model=BackfillResponse,
#     summary="Trigger full evidence backfill for all companies",
#     description="""
#     Re-triggers the full evidence collection pipeline (SEC documents + external signals)
#     for all 10 target companies. Companies are processed sequentially to respect rate limits,
#     but SEC and signal pipelines run in parallel within each company.

#     Returns a task_id immediately. Use GET /api/v1/evidence/backfill/tasks/{task_id} to check progress.
#     """,
# )
# async def trigger_backfill(background_tasks: BackgroundTasks):
#     """Trigger full evidence backfill for all 10 target companies."""
#     task_id = str(uuid4())

#     _backfill_task_store[task_id] = {
#         "task_id": task_id,
#         "status": BackfillStatus.QUEUED,
#         "progress": {
#             "companies_completed": 0,
#             "total_companies": len(TARGET_TICKERS),
#             "current_company": None,
#         },
#         "company_results": [],
#         "started_at": datetime.now(timezone.utc).isoformat(),
#         "completed_at": None,
#     }

#     background_tasks.add_task(run_backfill, task_id)

#     logger.info(f"Backfill queued: task_id={task_id}, companies={len(TARGET_TICKERS)}")

#     return BackfillResponse(
#         task_id=task_id,
#         status=BackfillStatus.QUEUED,
#         message=f"Backfill started for {len(TARGET_TICKERS)} companies. Poll /api/v1/evidence/backfill/tasks/{task_id} for progress.",
#     )


# 
# # GET /api/v1/evidence/backfill/tasks/{task_id} - Check backfill progress
# 

# @router.get(
#     "/evidence/backfill/tasks/{task_id}",
#     response_model=BackfillTaskStatus,
#     summary="Check backfill task progress",
#     description="Returns per-company status, the current company being processed, and overall progress.",
# )
# async def get_backfill_status(task_id: str):
#     """Check progress of a backfill task."""
#     if task_id not in _backfill_task_store:
#         raise HTTPException(status_code=404, detail=f"Backfill task not found: {task_id}")

#     task = _backfill_task_store[task_id]
#     return BackfillTaskStatus(
#         task_id=task["task_id"],
#         status=task["status"],
#         progress=BackfillProgress(**task["progress"]),
#         company_results=[CompanyBackfillResult(**r) for r in task["company_results"]],
#         started_at=task["started_at"],
#         completed_at=task["completed_at"],
#     )


# 
# # Background Task: run_backfill
# 

# async def _collect_signals_for_company(ticker: str) -> Dict[str, Any]:
#     """Run all 4 signal categories for a company. Returns a summary dict."""
#     signal_results = {}
#     errors = []

#     categories = [
#         ("technology_hiring", lambda: get_job_signal_service().analyze_company(ticker, force_refresh=True)),
#         ("innovation_activity", lambda: get_patent_signal_service().analyze_company(ticker, years_back=5)),
#         ("digital_presence", lambda: get_tech_signal_service().analyze_company(ticker, force_refresh=True)),
#         ("leadership_signals", lambda: get_leadership_service().analyze_company(ticker)),
#     ]

#     for category, service_call in categories:
#         try:
#             result = await service_call()
#             signal_results[category] = {
#                 "status": "success",
#                 "score": result.get("normalized_score") if isinstance(result, dict) else None,
#             }
#         except Exception as e:
#             logger.error(f"Signal error for {ticker}/{category}: {e}")
#             signal_results[category] = {"status": "failed", "error": str(e)}
#             errors.append(f"{category}: {str(e)}")

#     return {"signals": signal_results, "errors": errors}


# async def _collect_sec_for_company(ticker: str) -> Dict[str, Any]:
#     """Run SEC document collection for a company (sync → wrapped in to_thread)."""
#     service = get_document_collector_service()
#     request = DocumentCollectionRequest(ticker=ticker)
#     result = await asyncio.to_thread(service.collect_for_company, request)
#     return {
#         "documents_found": result.documents_found,
#         "documents_uploaded": result.documents_uploaded,
#         "documents_skipped": result.documents_skipped,
#         "documents_failed": result.documents_failed,
#         "summary": result.summary,
#     }


# async def run_backfill(task_id: str):
#     """Background task: process all companies sequentially, SEC + signals in parallel per company."""
#     logger.info(f"Backfill started: task_id={task_id}")
#     _backfill_task_store[task_id]["status"] = BackfillStatus.RUNNING

#     has_errors = False

#     for i, ticker in enumerate(TARGET_TICKERS):
#         _backfill_task_store[task_id]["progress"]["current_company"] = ticker
#         logger.info(f"Backfill [{i+1}/{len(TARGET_TICKERS)}]: Processing {ticker}")

#         company_result = {"ticker": ticker, "status": "success", "sec_result": None, "signal_result": None, "error": None}

#         try:
#             # Run SEC and signals in parallel for this company
#             sec_task = asyncio.create_task(_collect_sec_for_company(ticker))
#             signal_task = asyncio.create_task(_collect_signals_for_company(ticker))

#             sec_result, signal_result = await asyncio.gather(sec_task, signal_task, return_exceptions=True)

#             # Handle SEC result
#             if isinstance(sec_result, Exception):
#                 logger.error(f"SEC collection failed for {ticker}: {sec_result}")
#                 company_result["sec_result"] = {"status": "failed", "error": str(sec_result)}
#                 has_errors = True
#             else:
#                 company_result["sec_result"] = sec_result

#             # Handle signal result
#             if isinstance(signal_result, Exception):
#                 logger.error(f"Signal collection failed for {ticker}: {signal_result}")
#                 company_result["signal_result"] = {"status": "failed", "error": str(signal_result)}
#                 has_errors = True
#             else:
#                 company_result["signal_result"] = signal_result
#                 if signal_result.get("errors"):
#                     has_errors = True

#         except Exception as e:
#             logger.error(f"Backfill failed for {ticker}: {e}")
#             company_result["status"] = "failed"
#             company_result["error"] = str(e)
#             has_errors = True

#         _backfill_task_store[task_id]["company_results"].append(company_result)
#         _backfill_task_store[task_id]["progress"]["companies_completed"] = i + 1

#     # Finalize
#     _backfill_task_store[task_id]["progress"]["current_company"] = None
#     _backfill_task_store[task_id]["status"] = (
#         BackfillStatus.COMPLETED_WITH_ERRORS if has_errors else BackfillStatus.COMPLETED
#     )
#     _backfill_task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

#     logger.info(f"Backfill finished: task_id={task_id}, status={_backfill_task_store[task_id]['status']}")
"""
Evidence API Router
app/routers/evidence.py

Endpoints:
- GET  /api/v1/companies/{ticker}/evidence      - Get summary evidence for a company
- GET  /api/v1/evidence/stats                    - Get evidence collection statistics
- POST /api/v1/evidence/backfill                 - Trigger full backfill for all 10 companies
- GET  /api/v1/evidence/backfill/tasks/{task_id} - Check backfill progress
- POST /api/v1/evidence/backfill/tasks/{task_id}/cancel - Cancel a running backfill
"""

import asyncio
import json
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
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
from app.shutdown import is_shutting_down
from app.models.evidence import (
    DocumentSummary,
    CompanyEvidenceResponse,
    SignalEvidence,
    SignalSummary,
    BackfillResponse,
    BackfillTaskStatus,
    BackfillProgress,
    BackfillStatus,
    CompanyBackfillResult,
    EvidenceStatsResponse,
    CompanyDocumentStat,
    CompanySignalStat,
    SignalCategoryBreakdown,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["Evidence"])

TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]

# Default skip threshold: skip companies collected within this many hours
DEFAULT_SKIP_HOURS = 24

# In-memory task store (in production, use Redis or database)
_backfill_task_store: Dict[str, Dict[str, Any]] = {}



# GET /api/v1/companies/{ticker}/evidence


@router.get(
    "/companies/{ticker}/evidence",
    response_model=CompanyEvidenceResponse,
    summary="Get summary evidence for a company",
    description=(
        "Returns aggregated SEC filing statistics (counts by type, status, "
        "word/chunk totals, filing date range, freshness) and external signals "
        "for the given ticker. No individual document rows are returned."
    ),
)
async def get_company_evidence(ticker: str):
    """Retrieve summary-level evidence (doc stats + signals) for a company."""
    ticker = ticker.upper()

    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found for ticker: {ticker}")

    company_id = str(company["id"])

    doc_repo = get_document_repository()
    signal_repo = get_signal_repository()

    # --- document summary (aggregated) ---
    documents = doc_repo.get_by_ticker(ticker)

    by_status: Dict[str, int] = {}
    by_filing_type: Dict[str, int] = {}
    total_chunks = 0
    total_words = 0
    filing_dates = []
    collected_dates = []
    processed_dates = []

    for doc in documents:
        st = doc.get("status", "unknown")
        by_status[st] = by_status.get(st, 0) + 1

        ft = doc.get("filing_type", "unknown")
        by_filing_type[ft] = by_filing_type.get(ft, 0) + 1

        total_chunks += doc.get("chunk_count") or 0
        total_words += doc.get("word_count") or 0

        if doc.get("filing_date"):
            filing_dates.append(doc["filing_date"])
        if doc.get("created_at"):
            collected_dates.append(doc["created_at"])
        if doc.get("processed_at"):
            processed_dates.append(doc["processed_at"])

    doc_summary = DocumentSummary(
        total_documents=len(documents),
        by_status=by_status,
        by_filing_type=by_filing_type,
        total_chunks=total_chunks,
        total_words=total_words,
        earliest_filing=str(min(filing_dates)) if filing_dates else None,
        latest_filing=str(max(filing_dates)) if filing_dates else None,
        last_collected=str(max(collected_dates)) if collected_dates else None,
        last_processed=str(max(processed_dates)) if processed_dates else None,
    )

    # --- signals ---
    signals = signal_repo.get_signals_by_ticker(ticker)
    summary = signal_repo.get_summary_by_ticker(ticker)

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
        document_summary=doc_summary,
        signals=signal_evidence,
        signal_count=len(signal_evidence),
        signal_summary=signal_summary,
    )



# GET /api/v1/evidence/stats


def _build_signal_stat(ticker: str, summary: Optional[Dict]) -> CompanySignalStat:
    """Build a CompanySignalStat from a company_signal_summaries row."""
    if not summary:
        return CompanySignalStat(ticker=ticker)

    last_updated = summary.get("last_updated")
    return CompanySignalStat(
        ticker=ticker,
        technology_hiring_score=summary.get("technology_hiring_score"),
        innovation_activity_score=summary.get("innovation_activity_score"),
        digital_presence_score=summary.get("digital_presence_score"),
        leadership_signals_score=summary.get("leadership_signals_score"),
        composite_score=summary.get("composite_score"),
        signal_count=summary.get("signal_count") or 0,
        last_updated=last_updated.isoformat() if hasattr(last_updated, "isoformat") else (str(last_updated) if last_updated else None),
    )


@router.get(
    "/evidence/stats",
    response_model=EvidenceStatsResponse,
    summary="Get evidence collection statistics",
    description=(
        "Returns comprehensive statistics across all companies including "
        "document counts, processing status, signal scores, and category breakdowns. "
        "All 10 target companies appear in signals_by_company even if no scores exist yet."
    ),
)
async def get_evidence_stats():
    """Get evidence collection statistics."""
    doc_repo = get_document_repository()
    signal_repo = get_signal_repository()

    # --- document stats ---
    doc_summary = doc_repo.get_summary_statistics()
    status_breakdown = doc_repo.get_status_breakdown()
    company_doc_stats = doc_repo.get_all_company_stats()
    freshness = {r["ticker"]: r for r in doc_repo.get_freshness_by_ticker()}

    # --- signal stats ---
    total_signals = signal_repo.get_total_signal_count()
    category_breakdown = signal_repo.get_category_breakdown()

    # --- signal summaries from company_signal_summaries table ---
    all_summaries = signal_repo.get_all_summaries()
    summaries_by_ticker = {s["ticker"]: s for s in all_summaries}

    # --- per-company document stats ---
    doc_stats = []
    for s in company_doc_stats:
        tk = s["ticker"]
        fresh = freshness.get(tk, {})
        lc = fresh.get("last_collected")
        lp = fresh.get("last_processed")
        doc_stats.append(CompanyDocumentStat(
            **s,
            last_collected=lc.isoformat() if lc else None,
            last_processed=lp.isoformat() if lp else None,
        ))

    # --- per-company signal stats — ALL 10 tickers guaranteed ---
    signal_stats = [
        _build_signal_stat(ticker, summaries_by_ticker.get(ticker))
        for ticker in TARGET_TICKERS
    ]

    return EvidenceStatsResponse(
        companies_tracked=doc_summary["companies_processed"],
        total_documents=doc_summary["total_documents"],
        total_chunks=doc_summary["total_chunks"],
        total_words=doc_summary["total_words"],
        total_signals=total_signals,
        documents_by_status=status_breakdown,
        signals_by_category=[
            SignalCategoryBreakdown(
                category=c.get("category", ""),
                count=c.get("count", 0),
                avg_score=round(c["avg_score"], 2) if c.get("avg_score") is not None else None,
                avg_confidence=round(c["avg_confidence"], 3) if c.get("avg_confidence") is not None else None,
            )
            for c in category_breakdown
        ],
        documents_by_company=doc_stats,
        signals_by_company=signal_stats,
    )



# POST /api/v1/evidence/backfill


@router.post(
    "/evidence/backfill",
    response_model=BackfillResponse,
    summary="Trigger full evidence backfill for all companies",
    description=(
        "Triggers the full evidence collection pipeline (SEC documents + external signals) "
        "for all 10 target companies. Returns a task_id immediately.\n\n"
        "**Options:**\n"
        "- `skip_recent_hours` (query param, default 24): Skip companies whose signals "
        "were last updated within this many hours. Set to 0 to force re-collection for all.\n"
        "- `force` (query param, default false): If true, ignores skip_recent_hours and "
        "re-collects everything.\n\n"
        "Use GET /api/v1/evidence/backfill/tasks/{task_id} to check progress.\n"
        "Use POST /api/v1/evidence/backfill/tasks/{task_id}/cancel to cancel."
    ),
)
async def trigger_backfill(
    background_tasks: BackgroundTasks,
    skip_recent_hours: int = Query(default=DEFAULT_SKIP_HOURS, ge=0, description="Skip companies updated within this many hours. 0 = skip none."),
    force: bool = Query(default=False, description="Force re-collection for all companies, ignoring skip_recent_hours."),
):
    """Trigger full evidence backfill for all 10 target companies."""

    # Determine which companies to skip
    skipped_tickers = []
    tickers_to_process = []

    if force:
        tickers_to_process = list(TARGET_TICKERS)
    else:
        signal_repo = get_signal_repository()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=skip_recent_hours)

        for ticker in TARGET_TICKERS:
            summary = signal_repo.get_summary_by_ticker(ticker)
            last_updated = summary.get("last_updated") if summary else None

            # Make last_updated timezone-aware for comparison
            if last_updated and not last_updated.tzinfo:
                last_updated = last_updated.replace(tzinfo=timezone.utc)

            if skip_recent_hours > 0 and last_updated and last_updated > cutoff:
                skipped_tickers.append(ticker)
                logger.info(f"Skipping {ticker}: last updated {last_updated.isoformat()}, within {skip_recent_hours}h window")
            else:
                tickers_to_process.append(ticker)

    task_id = str(uuid4())

    _backfill_task_store[task_id] = {
        "task_id": task_id,
        "status": BackfillStatus.QUEUED,
        "cancelled": False,
        "progress": {
            "companies_completed": 0,
            "total_companies": len(tickers_to_process),
            "current_company": None,
            "skipped_companies": skipped_tickers,
        },
        "company_results": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
    }

    # Add skipped companies as pre-filled results
    for ticker in skipped_tickers:
        _backfill_task_store[task_id]["company_results"].append({
            "ticker": ticker,
            "status": "skipped",
            "sec_result": None,
            "signal_result": None,
            "error": f"Skipped: last updated within {skip_recent_hours}h window",
        })

    if not tickers_to_process:
        _backfill_task_store[task_id]["status"] = BackfillStatus.COMPLETED
        _backfill_task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        return BackfillResponse(
            task_id=task_id,
            status=BackfillStatus.COMPLETED,
            message=f"All {len(TARGET_TICKERS)} companies were recently collected (within {skip_recent_hours}h).Use force=true to override and check above evidence stats to get full evidence for all companies",
        )

    background_tasks.add_task(run_backfill, task_id, tickers_to_process)

    skip_msg = f" Skipped {len(skipped_tickers)} recently collected: {', '.join(skipped_tickers)}." if skipped_tickers else ""
    logger.info(f"Backfill queued: task_id={task_id}, processing={len(tickers_to_process)}, skipped={len(skipped_tickers)}")

    return BackfillResponse(
        task_id=task_id,
        status=BackfillStatus.QUEUED,
        message=(
            f"Backfill started for {len(tickers_to_process)} companies.{skip_msg} "
            f"Poll /api/v1/evidence/backfill/tasks/{task_id} for progress."
        ),
    )



# GET /api/v1/evidence/backfill/tasks/{task_id}


@router.get(
    "/evidence/backfill/tasks/{task_id}",
    response_model=BackfillTaskStatus,
    summary="Check backfill task progress",
    description="Returns per-company status, the current company being processed, skipped companies, and overall progress.",
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



# POST /api/v1/evidence/backfill/tasks/{task_id}/cancel


@router.post(
    "/evidence/backfill/tasks/{task_id}/cancel",
    summary="Cancel a running backfill task",
    description=(
        "Signals a running backfill to stop after the current company finishes. "
        "Companies already completed are kept. The task status changes to 'cancelled'."
    ),
)
async def cancel_backfill(task_id: str):
    """Cancel a running backfill task."""
    if task_id not in _backfill_task_store:
        raise HTTPException(status_code=404, detail=f"Backfill task not found: {task_id}")

    task = _backfill_task_store[task_id]

    if task["status"] in (BackfillStatus.COMPLETED, BackfillStatus.COMPLETED_WITH_ERRORS, BackfillStatus.FAILED):
        raise HTTPException(status_code=400, detail=f"Task already finished with status: {task['status']}")

    if task.get("cancelled"):
        return {
            "task_id": task_id,
            "status": "cancelling",
            "message": "Cancel already requested. Task will stop after current company finishes.",
        }

    task["cancelled"] = True
    logger.info(f"Backfill cancel requested: task_id={task_id}")

    return {
        "task_id": task_id,
        "status": "cancelling",
        "message": "Cancel requested. Task will stop after the current company finishes processing.",
        "current_company": task["progress"].get("current_company"),
        "companies_completed": task["progress"]["companies_completed"],
    }



# Background Task: run_backfill


async def _collect_signals_for_company(ticker: str) -> Dict[str, Any]:
    """Run all 4 signal categories for a company."""
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
    """Run SEC document collection for a company."""
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


async def run_backfill(task_id: str, tickers: list[str]):
    """Background task: process companies sequentially, SEC + signals in parallel per company."""
    logger.info(f"Backfill started: task_id={task_id}, companies={tickers}")
    _backfill_task_store[task_id]["status"] = BackfillStatus.RUNNING

    has_errors = False
    start_time = datetime.now(timezone.utc)

    for i, ticker in enumerate(tickers):

        # --- Check for cancellation OR app shutdown before starting next company ---
        if _backfill_task_store[task_id].get("cancelled") or is_shutting_down():
            cancel_reason = "App shutdown (Ctrl+C)" if is_shutting_down() else "Backfill cancelled by user"
            logger.info(f"Backfill stopped at company {i+1}/{len(tickers)}: {ticker} — reason: {cancel_reason}")

            # Mark remaining companies as cancelled
            for remaining_ticker in tickers[i:]:
                _backfill_task_store[task_id]["company_results"].append({
                    "ticker": remaining_ticker,
                    "status": "cancelled",
                    "sec_result": None,
                    "signal_result": None,
                    "error": cancel_reason,
                })

            _backfill_task_store[task_id]["status"] = BackfillStatus.CANCELLED
            _backfill_task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.info(f"Backfill cancelled: task_id={task_id}, completed={i}/{len(tickers)}, elapsed={elapsed:.1f}s")
            return

        _backfill_task_store[task_id]["progress"]["current_company"] = ticker
        company_start = datetime.now(timezone.utc)
        logger.info(f"Backfill [{i+1}/{len(tickers)}]: Processing {ticker}")

        company_result = {
            "ticker": ticker, "status": "success",
            "sec_result": None, "signal_result": None, "error": None,
            "duration_seconds": None,
        }

        try:
            sec_task = asyncio.create_task(_collect_sec_for_company(ticker))
            signal_task = asyncio.create_task(_collect_signals_for_company(ticker))
            sec_result, signal_result = await asyncio.gather(sec_task, signal_task, return_exceptions=True)

            if isinstance(sec_result, Exception):
                logger.error(f"SEC collection failed for {ticker}: {sec_result}")
                company_result["sec_result"] = {"status": "failed", "error": str(sec_result)}
                has_errors = True
            else:
                company_result["sec_result"] = sec_result

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

        company_result["duration_seconds"] = round((datetime.now(timezone.utc) - company_start).total_seconds(), 1)
        _backfill_task_store[task_id]["company_results"].append(company_result)
        _backfill_task_store[task_id]["progress"]["companies_completed"] = i + 1

    # --- Finalize ---
    _backfill_task_store[task_id]["progress"]["current_company"] = None
    _backfill_task_store[task_id]["status"] = (
        BackfillStatus.COMPLETED_WITH_ERRORS if has_errors else BackfillStatus.COMPLETED
    )
    _backfill_task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(
        f"Backfill finished: task_id={task_id}, "
        f"status={_backfill_task_store[task_id]['status']}, "
        f"companies={len(tickers)}, elapsed={elapsed:.1f}s"
    )