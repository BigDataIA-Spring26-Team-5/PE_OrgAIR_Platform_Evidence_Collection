from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone
import logging
from app.services.leadership_service import get_leadership_service
from app.services.job_signal_service import get_job_signal_service
from app.services.tech_signal_service import get_tech_signal_service
from app.services.patent_signal_service import get_patent_signal_service
from app.repositories.signal_repository import get_signal_repository
from app.repositories.company_repository import CompanyRepository
import json

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/signals",
    # tags=["Signals"],
)


# ============================================================
# MAIN PIPELINE ENDPOINT - Run Full Signals Pipeline
# ============================================================

@router.post(
    "/pipeline/run",
    tags=["0. Signals Pipeline"],
    summary="Run full signals pipeline for all companies",
    description="""
    **Single endpoint to trigger the entire signals pipeline.**

    Fetches all companies from Snowflake and runs all 4 signal analyses:

    1. **Job Postings** (technology_hiring) - From JobSpy (LinkedIn, Indeed, etc.)
    2. **Tech Stack** (digital_presence) - Extracted from job descriptions
    3. **Patents** (innovation_activity) - From PatentsView API
    4. **Leadership** (leadership_signals) - From DEF 14A SEC filings

    **Storage:**
    - Raw data → S3
    - Signals → Snowflake `external_signals` table
    - Summaries → Snowflake `company_signal_summaries` table

    **Note:** This may take several minutes depending on the number of companies.
    """
)
async def run_signals_pipeline(
    force_refresh: bool = Query(default=False, description="Force refresh of cached job data"),
    years_back: int = Query(default=5, ge=1, le=10, description="Years back for patent search")
):
    """
    Run the complete signals pipeline for all companies in Snowflake.

    Steps:
    1. Fetch all companies from Snowflake
    2. For each company, run all 4 signal analyses
    3. Store raw data in S3, signals in Snowflake
    """
    logger.info("=" * 60)
    logger.info("STARTING FULL SIGNALS PIPELINE")
    logger.info("=" * 60)

    start_time = datetime.now(timezone.utc)

    # Get all companies from Snowflake
    company_repo = CompanyRepository()
    companies = company_repo.get_all()

    logger.info(f"Found {len(companies)} companies to process")

    # Initialize services
    job_service = get_job_signal_service()
    tech_service = get_tech_signal_service()
    patent_service = get_patent_signal_service()
    leadership_service = get_leadership_service()

    # Results tracking
    results = {
        "status": "success",
        "pipeline_started_at": start_time.isoformat(),
        "total_companies": len(companies),
        "companies_processed": 0,
        "summary": {
            "job_signals": {"success": 0, "failed": 0},
            "tech_signals": {"success": 0, "failed": 0},
            "patent_signals": {"success": 0, "failed": 0},
            "leadership_signals": {"success": 0, "failed": 0}
        },
        "companies": [],
        "errors": []
    }

    # Process each company
    for company in companies:
        ticker = company.get('ticker')
        if not ticker:
            continue

        company_name = company.get('name', ticker)
        logger.info(f"\n{'='*40}")
        logger.info(f"Processing: {ticker} - {company_name}")
        logger.info(f"{'='*40}")

        company_result = {
            "ticker": ticker,
            "company_name": company_name,
            "signals": {
                "job_postings": None,
                "tech_stack": None,
                "patents": None,
                "leadership": None
            },
            "errors": []
        }

        # 1. Job Posting Signals (technology_hiring)
        try:
            logger.info(f"  [1/4] Analyzing job postings...")
            job_result = await job_service.analyze_company(ticker, force_refresh=force_refresh)
            company_result["signals"]["job_postings"] = {
                "status": "success",
                "score": job_result.get("normalized_score"),
                "total_jobs": job_result.get("total_jobs", 0),
                "ai_jobs": job_result.get("ai_jobs", 0)
            }
            results["summary"]["job_signals"]["success"] += 1
            logger.info(f"  ✅ Job signals: score={job_result.get('normalized_score')}")
        except Exception as e:
            error_msg = f"Job signals failed: {str(e)}"
            company_result["errors"].append(error_msg)
            company_result["signals"]["job_postings"] = {"status": "failed", "error": str(e)}
            results["summary"]["job_signals"]["failed"] += 1
            logger.error(f"  ❌ {error_msg}")

        # 2. Tech Stack Signals (digital_presence)
        try:
            logger.info(f"  [2/4] Analyzing tech stack...")
            tech_result = await tech_service.analyze_company(ticker, force_refresh=force_refresh)
            company_result["signals"]["tech_stack"] = {
                "status": "success",
                "score": tech_result.get("normalized_score"),
                "keywords_found": tech_result.get("keywords_count", 0)
            }
            results["summary"]["tech_signals"]["success"] += 1
            logger.info(f"  ✅ Tech signals: score={tech_result.get('normalized_score')}")
        except Exception as e:
            error_msg = f"Tech signals failed: {str(e)}"
            company_result["errors"].append(error_msg)
            company_result["signals"]["tech_stack"] = {"status": "failed", "error": str(e)}
            results["summary"]["tech_signals"]["failed"] += 1
            logger.error(f"  ❌ {error_msg}")

        # 3. Patent Signals (innovation_activity)
        try:
            logger.info(f"  [3/4] Analyzing patents...")
            patent_result = await patent_service.analyze_company(ticker, years_back=years_back)
            company_result["signals"]["patents"] = {
                "status": "success",
                "score": patent_result.get("normalized_score"),
                "total_patents": patent_result.get("total_patents", 0),
                "ai_patents": patent_result.get("ai_patents", 0)
            }
            results["summary"]["patent_signals"]["success"] += 1
            logger.info(f"  ✅ Patent signals: score={patent_result.get('normalized_score')}")
        except Exception as e:
            error_msg = f"Patent signals failed: {str(e)}"
            company_result["errors"].append(error_msg)
            company_result["signals"]["patents"] = {"status": "failed", "error": str(e)}
            results["summary"]["patent_signals"]["failed"] += 1
            logger.error(f"  ❌ {error_msg}")

        # 4. Leadership Signals (leadership_signals)
        try:
            logger.info(f"  [4/4] Analyzing leadership...")
            leadership_result = await leadership_service.analyze_company(ticker)
            company_result["signals"]["leadership"] = {
                "status": "success",
                "score": leadership_result.get("normalized_score"),
                "filings_analyzed": leadership_result.get("filings_analyzed", 0)
            }
            results["summary"]["leadership_signals"]["success"] += 1
            logger.info(f"  ✅ Leadership signals: score={leadership_result.get('normalized_score')}")
        except Exception as e:
            error_msg = f"Leadership signals failed: {str(e)}"
            company_result["errors"].append(error_msg)
            company_result["signals"]["leadership"] = {"status": "failed", "error": str(e)}
            results["summary"]["leadership_signals"]["failed"] += 1
            logger.error(f"  ❌ {error_msg}")

        results["companies"].append(company_result)
        results["companies_processed"] += 1

        if company_result["errors"]:
            results["errors"].extend([f"{ticker}: {e}" for e in company_result["errors"]])

    # Finalize
    end_time = datetime.now(timezone.utc)
    results["pipeline_completed_at"] = end_time.isoformat()
    results["duration_seconds"] = (end_time - start_time).total_seconds()

    if results["errors"]:
        results["status"] = "completed_with_errors"

    logger.info("\n" + "=" * 60)
    logger.info("SIGNALS PIPELINE COMPLETE")
    logger.info(f"Duration: {results['duration_seconds']:.1f} seconds")
    logger.info(f"Companies processed: {results['companies_processed']}")
    logger.info(f"Errors: {len(results['errors'])}")
    logger.info("=" * 60)

    return results


@router.post(
    "/pipeline/run/{ticker}",
    tags=["0. Signals Pipeline"],
    summary="Run full signals pipeline for a single company",
    description="Run all 4 signal analyses for a specific company."
)
async def run_signals_pipeline_for_company(
    ticker: str,
    force_refresh: bool = Query(default=False, description="Force refresh of cached job data"),
    years_back: int = Query(default=5, ge=1, le=10, description="Years back for patent search")
):
    """Run the complete signals pipeline for a single company."""
    ticker = ticker.upper()
    logger.info(f"Running signals pipeline for: {ticker}")

    start_time = datetime.now(timezone.utc)

    # Verify company exists
    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")

    # Initialize services
    job_service = get_job_signal_service()
    tech_service = get_tech_signal_service()
    patent_service = get_patent_signal_service()
    leadership_service = get_leadership_service()

    result = {
        "status": "success",
        "ticker": ticker,
        "company_name": company.get('name'),
        "pipeline_started_at": start_time.isoformat(),
        "signals": {},
        "errors": []
    }

    # 1. Job Posting Signals
    try:
        job_result = await job_service.analyze_company(ticker, force_refresh=force_refresh)
        result["signals"]["job_postings"] = {
            "status": "success",
            "category": "technology_hiring",
            "score": job_result.get("normalized_score"),
            "confidence": job_result.get("confidence"),
            "details": job_result
        }
    except Exception as e:
        result["signals"]["job_postings"] = {"status": "failed", "error": str(e)}
        result["errors"].append(f"Job signals: {str(e)}")

    # 2. Tech Stack Signals
    try:
        tech_result = await tech_service.analyze_company(ticker, force_refresh=force_refresh)
        result["signals"]["tech_stack"] = {
            "status": "success",
            "category": "digital_presence",
            "score": tech_result.get("normalized_score"),
            "confidence": tech_result.get("confidence"),
            "details": tech_result
        }
    except Exception as e:
        result["signals"]["tech_stack"] = {"status": "failed", "error": str(e)}
        result["errors"].append(f"Tech signals: {str(e)}")

    # 3. Patent Signals
    try:
        patent_result = await patent_service.analyze_company(ticker, years_back=years_back)
        result["signals"]["patents"] = {
            "status": "success",
            "category": "innovation_activity",
            "score": patent_result.get("normalized_score"),
            "confidence": patent_result.get("confidence"),
            "details": patent_result
        }
    except Exception as e:
        result["signals"]["patents"] = {"status": "failed", "error": str(e)}
        result["errors"].append(f"Patent signals: {str(e)}")

    # 4. Leadership Signals
    try:
        leadership_result = await leadership_service.analyze_company(ticker)
        result["signals"]["leadership"] = {
            "status": "success",
            "category": "leadership_signals",
            "score": leadership_result.get("normalized_score"),
            "confidence": leadership_result.get("confidence"),
            "details": leadership_result
        }
    except Exception as e:
        result["signals"]["leadership"] = {"status": "failed", "error": str(e)}
        result["errors"].append(f"Leadership signals: {str(e)}")

    end_time = datetime.now(timezone.utc)
    result["pipeline_completed_at"] = end_time.isoformat()
    result["duration_seconds"] = (end_time - start_time).total_seconds()

    if result["errors"]:
        result["status"] = "completed_with_errors"

    return result


# ============================================================
# SECTION 1: LEADERSHIP SIGNALS (From DEF 14A)
# ============================================================

@router.post(
    "/leadership/{ticker}",
    tags=["1. Leadership Signals"],
    summary="Analyze leadership signals for a company",
    description="""
    Extract leadership signals from DEF 14A (proxy statement) filings.
    
    **Analyzes:**
    - Tech Executive Presence (CTO, CDO, Chief AI Officer, etc.)
    - AI/Tech Keywords in Compensation Discussion
    - Tech-Linked Performance Metrics
    - Board Tech Expertise
    
    **Scoring (0-100):**
    - Tech Exec Presence: 30 pts max
    - AI/Tech Keywords: 30 pts max
    - Performance Metrics: 25 pts max
    - Board Expertise: 15 pts max
    
    Results are stored in `external_signals` and `company_signal_summaries` tables.
    """
)
async def analyze_leadership_signals(ticker: str):
    """Analyze DEF 14A filings for leadership signals."""
    logger.info(f"🎯 Leadership analysis request for: {ticker}")
    
    try:
        service = get_leadership_service()
        result = service.analyze_company(ticker)
        return {
            "status": "success",
            "ticker": ticker,
            "result": result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Leadership analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/leadership",
    tags=["1. Leadership Signals"],
    summary="Analyze leadership signals for all companies"
)
async def analyze_all_leadership_signals():
    """Analyze DEF 14A filings for all 10 target companies."""
    logger.info("🎯 Leadership analysis request for ALL companies")
    
    try:
        service = get_leadership_service()
        result = service.analyze_all_companies()
        return result
    except Exception as e:
        logger.error(f"Batch leadership analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SECTION 2: SIGNAL QUERIES
# ============================================================

@router.get(
    "/all",
    tags=["2. Signal Queries"],
    summary="Get all signals for all companies",
    description="Fetch signals for all companies stored in Snowflake in one request."
)
async def get_all_company_signals(
    category: Optional[str] = Query(None, description="Filter by category (technology_hiring, innovation_activity, digital_presence, leadership_signals)")
):
    """Get signals for all companies in the database."""
    logger.info("Fetching signals for ALL companies")

    repo = get_signal_repository()
    company_repo = CompanyRepository()

    # Get all companies from Snowflake
    companies = company_repo.get_all()

    results = []
    for company in companies:
        ticker = company.get('ticker')
        if not ticker:
            continue

        company_id = str(company.get('id'))

        if category:
            signals = repo.get_signals_by_category(company_id, category)
        else:
            signals = repo.get_signals_by_company(company_id)

        results.append({
            "ticker": ticker,
            "company_name": company.get('name'),
            "company_id": company_id,
            "signal_count": len(signals),
            "signals": signals
        })

    return {
        "total_companies": len(results),
        "category_filter": category,
        "companies": results
    }


@router.get(
    "/summary/all",
    tags=["2. Signal Queries"],
    summary="Get signal summaries for all companies",
    description="Fetch aggregated signal summaries for all companies."
)
async def get_all_signal_summaries():
    """Get signal summaries for all companies."""
    logger.info("Fetching signal summaries for ALL companies")

    repo = get_signal_repository()
    company_repo = CompanyRepository()

    companies = company_repo.get_all()

    results = []
    for company in companies:
        ticker = company.get('ticker')
        if not ticker:
            continue

        summary = repo.get_summary_by_ticker(ticker)
        results.append({
            "ticker": ticker,
            "company_name": company.get('name'),
            "summary": summary
        })

    return {
        "total_companies": len(results),
        "summaries": results
    }


@router.get(
    "/{ticker}",
    tags=["2. Signal Queries"],
    summary="Get all signals for a company"
)
async def get_company_signals(
    ticker: str,
    category: Optional[str] = Query(None, description="Filter by category")
):
    """Get all signals for a company, optionally filtered by category."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()
    
    company = company_repo.get_by_ticker(ticker.upper())
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")
    
    if category:
        signals = repo.get_signals_by_category(str(company['id']), category)
    else:
        signals = repo.get_signals_by_company(str(company['id']))
    
    return {
        "ticker": ticker.upper(),
        "signal_count": len(signals),
        "signals": signals
    }


@router.get(
    "/summary/{ticker}",
    tags=["2. Signal Queries"],
    summary="Get signal summary for a company"
)
async def get_company_signal_summary(ticker: str):
    """Get the aggregated signal summary for a company."""
    repo = get_signal_repository()
    
    summary = repo.get_summary_by_ticker(ticker.upper())
    
    if not summary:
        raise HTTPException(status_code=404, detail=f"No signal summary found for: {ticker}")
    
    return summary


# ============================================================
# SECTION 3: SIGNAL REPORTS
# ============================================================

# @router.get(
#     "/report/summary",
#     tags=["3. Signal Reports"],
#     summary="Get signal summary table for all companies",
#     description="Returns the Signal Scores by Company table with leadership details"
# )
# async def get_signal_summary_report():
#     """Get signal summary for all companies in table format."""
#     repo = get_signal_repository()
    
#     summaries = repo.get_all_summaries()
#     target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
    
#     # Build table format
#     table = {
#         "headers": ["Ticker", "Hiring", "Innovation", "Tech", "Leadership", "Composite"],
#         "rows": []
#     }
    
#     # Build detailed company signals
#     company_signals = []
    
#     for ticker in target_tickers:
#         # Get summary for this ticker
#         summary = next((s for s in summaries if s["ticker"] == ticker), None)
        
#         # Add to table
#         if summary:
#             table["rows"].append([
#                 ticker,
#                 summary.get("technology_hiring_score") or "-",
#                 summary.get("innovation_activity_score") or "-",
#                 summary.get("digital_presence_score") or "-",
#                 summary.get("leadership_signals_score") or "-",
#                 round(summary["composite_score"], 1) if summary.get("composite_score") else "-"
#             ])
#         else:
#             table["rows"].append([ticker, "-", "-", "-", "-", "-"])
        
#         # Get detailed leadership signals for this company
#         signals = repo.get_signals_by_ticker(ticker)
#         leadership_signals = [s for s in signals if s.get('category') == 'leadership_signals']
        
#         if leadership_signals:
#             # Aggregate metadata across all filings
#             all_tech_execs = []
#             all_keyword_counts = {}
#             all_filing_dates = []
#             total_score = 0
            
#             for sig in leadership_signals:
#                 metadata = sig.get('metadata', {})
#                 if isinstance(metadata, str):
#                     try:
#                         metadata = json.loads(metadata)
#                     except:
#                         metadata = {}
                
#                 # Collect tech execs
#                 all_tech_execs.extend(metadata.get('tech_execs_found', []))
                
#                 # Aggregate keyword counts
#                 for kw, count in metadata.get('keyword_counts', {}).items():
#                     all_keyword_counts[kw] = all_keyword_counts.get(kw, 0) + count
                
#                 # Collect filing dates
#                 if metadata.get('filing_date'):
#                     all_filing_dates.append(metadata['filing_date'])
                
#                 total_score += sig.get('normalized_score', 0)
            
#             avg_score = total_score / len(leadership_signals) if leadership_signals else 0
#             avg_confidence = sum(s.get('confidence', 0) for s in leadership_signals) / len(leadership_signals)
            
#             company_signals.append({
#                 "ticker": ticker,
#                 "company_id": leadership_signals[0].get('company_id'),
#                 "category": "leadership_signals",
#                 "source": "sec_filing",
#                 "normalized_score": round(avg_score, 2),
#                 "confidence": round(avg_confidence, 3),
#                 "signal_count": len(leadership_signals),
#                 "metadata": {
#                     "tech_execs_found": list(set(all_tech_execs)),
#                     "keyword_counts": all_keyword_counts,
#                     "filing_dates": all_filing_dates
#                 }
#             })
#         else:
#             company_signals.append({
#                 "ticker": ticker,
#                 "company_id": None,
#                 "category": "leadership_signals",
#                 "source": None,
#                 "normalized_score": None,
#                 "confidence": None,
#                 "signal_count": 0,
#                 "metadata": None
#             })
    
#     return {
#         "report_generated_at": datetime.now(timezone.utc).isoformat(),
#         "companies_count": len(summaries),
#         "table": table,
#         "company_signals": company_signals
#     }


@router.get(
    "/report/summary/{ticker}",
    tags=["3. Signal Reports"],
    summary="Get signal summary for a specific company",
    description="Returns detailed signal information for one company"
)
async def get_company_signal_summary_report(ticker: str):
    """Get detailed signal summary for a specific company."""
    ticker = ticker.upper()
    repo = get_signal_repository()
    company_repo = CompanyRepository()
    
    # Get company
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")
    
    company_id = str(company['id'])
    
    # Get summary
    summary = repo.get_summary_by_ticker(ticker)
    
    # Get all signals for this company
    signals = repo.get_signals_by_ticker(ticker)
    leadership_signals = [s for s in signals if s.get('category') == 'leadership_signals']
    
    # Build response
    leadership_detail = None
    if leadership_signals:
        all_tech_execs = []
        all_keyword_counts = {}
        all_filing_dates = []
        all_board_indicators = []
        total_tech_exec_score = 0
        total_keyword_score = 0
        total_perf_score = 0
        total_board_score = 0
        
        for sig in leadership_signals:
            metadata = sig.get('metadata', {})
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            all_tech_execs.extend(metadata.get('tech_execs_found', []))
            all_board_indicators.extend(metadata.get('board_indicators', []))
            
            for kw, count in metadata.get('keyword_counts', {}).items():
                all_keyword_counts[kw] = all_keyword_counts.get(kw, 0) + count
            
            if metadata.get('filing_date'):
                all_filing_dates.append(metadata['filing_date'])
            
            total_tech_exec_score += metadata.get('tech_exec_score', 0)
            total_keyword_score += metadata.get('keyword_score', 0)
            total_perf_score += metadata.get('performance_metric_score', 0)
            total_board_score += metadata.get('board_tech_score', 0)
        
        n = len(leadership_signals)
        avg_score = sum(s.get('normalized_score', 0) for s in leadership_signals) / n
        avg_confidence = sum(s.get('confidence', 0) for s in leadership_signals) / n
        
        leadership_detail = {
            "company_id": company_id,
            "category": "leadership_signals",
            "source": "sec_filing",
            "normalized_score": round(avg_score, 2),
            "confidence": round(avg_confidence, 3),
            "signal_count": n,
            "score_breakdown": {
                "tech_exec_score": round(total_tech_exec_score / n, 1),
                "keyword_score": round(total_keyword_score / n, 1),
                "performance_metric_score": round(total_perf_score / n, 1),
                "board_tech_score": round(total_board_score / n, 1)
            },
            "metadata": {
                "tech_execs_found": list(set(all_tech_execs)),
                "keyword_counts": all_keyword_counts,
                "board_indicators": list(set(all_board_indicators)),
                "filing_dates": all_filing_dates
            }
        }
    
    return {
        "ticker": ticker,
        "company_id": company_id,
        "company_name": company.get('name'),
        "summary": {
            "technology_hiring_score": summary.get("technology_hiring_score") if summary else None,
            "innovation_activity_score": summary.get("innovation_activity_score") if summary else None,
            "digital_presence_score": summary.get("digital_presence_score") if summary else None,
            "leadership_signals_score": summary.get("leadership_signals_score") if summary else None,
            "composite_score": summary.get("composite_score") if summary else None,
            "signal_count": summary.get("signal_count") if summary else 0
        },
        "leadership_signals": leadership_detail,
        "hiring_signals": None,  # Placeholder for future
        "innovation_signals": None,  # Placeholder for future
        "tech_signals": None  # Placeholder for future
    }


# @router.get(
#     "/report/leadership",
#     tags=["3. Signal Reports"],
#     summary="Get detailed leadership signals report"
# )
# async def get_leadership_report():
#     """Get detailed leadership signal breakdown for all companies."""
#     repo = get_signal_repository()
    
#     # Get all leadership signals
#     all_signals = []
#     target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
    
#     for ticker in target_tickers:
#         signals = repo.get_signals_by_ticker(ticker)
#         leadership_signals = [s for s in signals if s.get('category') == 'leadership_signals']
        
#         if leadership_signals:
#             # Get the most recent signal for summary
#             latest = leadership_signals[0]
#             all_signals.append({
#                 "ticker": ticker,
#                 "score": latest.get("normalized_score"),
#                 "confidence": latest.get("confidence"),
#                 "signal_count": len(leadership_signals),
#                 "latest_filing": str(latest.get("signal_date"))
#             })
#         else:
#             all_signals.append({
#                 "ticker": ticker,
#                 "score": None,
#                 "confidence": None,
#                 "signal_count": 0,
#                 "latest_filing": None
#             })
    
#     # Build table
#     table = {
#         "headers": ["Ticker", "Leadership Score", "Confidence", "Filings Analyzed", "Latest Filing"],
#         "rows": [[
#             s["ticker"],
#             s["score"] or "-",
#             f"{s['confidence']:.2f}" if s["confidence"] else "-",
#             s["signal_count"],
#             s["latest_filing"] or "-"
#         ] for s in all_signals]
#     }
    
#     return {
#         "report_generated_at": datetime.now(timezone.utc).isoformat(),
#         "table": table,
#         "details": all_signals
#     }


# ============================================================
# SECTION 4: JOB POSTING SIGNALS (From JobSpy)
# ============================================================

@router.post(
    "/hiring/{ticker}",
    tags=["4. Job Posting Signals"],
    summary="Analyze hiring signals for a company",
    description="""
    Extract hiring signals from job postings using JobSpy.

    **Analyzes:**
    - Total job postings
    - AI/ML related job postings
    - Technology hiring trends

    **Data Sources:** LinkedIn, Indeed, Glassdoor, ZipRecruiter

    Results are stored in `external_signals` and `company_signal_summaries` tables.
    Raw data is stored in S3.
    """
)
async def analyze_hiring_signals(ticker: str, force_refresh: bool = False):
    """Analyze job postings for hiring signals."""
    logger.info(f"Hiring analysis request for: {ticker}")

    try:
        service = get_job_signal_service()
        result = await service.analyze_company(ticker, force_refresh=force_refresh)
        return {
            "status": "success",
            "ticker": ticker,
            "result": result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Hiring analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/hiring",
    tags=["4. Job Posting Signals"],
    summary="Analyze hiring signals for all companies"
)
async def analyze_all_hiring_signals(force_refresh: bool = False):
    """Analyze job postings for all target companies."""
    logger.info("Hiring analysis request for ALL companies")

    try:
        service = get_job_signal_service()
        result = await service.analyze_all_companies(force_refresh=force_refresh)
        return result
    except Exception as e:
        logger.error(f"Batch hiring analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SECTION 5: PATENT SIGNALS (From PatentsView)
# ============================================================

@router.post(
    "/innovation/{ticker}",
    tags=["5. Patent Signals"],
    summary="Analyze innovation signals for a company",
    description="""
    Extract innovation signals from patent data using PatentsView API.

    **Analyzes:**
    - Total patents filed
    - AI/ML related patents
    - Patent filing trends
    - CPC classification codes

    Results are stored in `external_signals` and `company_signal_summaries` tables.
    """
)
async def analyze_innovation_signals(ticker: str, years_back: int = 5):
    """Analyze patents for innovation signals."""
    logger.info(f"Innovation analysis request for: {ticker}")

    try:
        service = get_patent_signal_service()
        result = await service.analyze_company(ticker, years_back=years_back)
        return {
            "status": "success",
            "ticker": ticker,
            "result": result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Innovation analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/innovation",
    tags=["5. Patent Signals"],
    summary="Analyze innovation signals for all companies"
)
async def analyze_all_innovation_signals(years_back: int = 5):
    """Analyze patents for all target companies."""
    logger.info("Innovation analysis request for ALL companies")

    try:
        service = get_patent_signal_service()
        result = await service.analyze_all_companies(years_back=years_back)
        return result
    except Exception as e:
        logger.error(f"Batch innovation analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SECTION 6: TECH STACK SIGNALS (From Job Descriptions)
# ============================================================

@router.post(
    "/tech/{ticker}",
    tags=["6. Tech Stack Signals"],
    summary="Analyze tech stack signals for a company",
    description="""
    Extract tech stack signals from job posting descriptions.

    **Analyzes:**
    - Technology keywords in job descriptions
    - AI/ML tools and frameworks mentioned
    - Cloud platforms (AWS, GCP, Azure)
    - Programming languages and frameworks

    **Note:** Uses same job data as hiring signals (no additional API calls if data is cached).

    Results are stored in `external_signals` and `company_signal_summaries` tables.
    """
)
async def analyze_tech_signals(ticker: str, force_refresh: bool = False):
    """Analyze job descriptions for tech stack signals."""
    logger.info(f"Tech stack analysis request for: {ticker}")

    try:
        service = get_tech_signal_service()
        result = await service.analyze_company(ticker, force_refresh=force_refresh)
        return {
            "status": "success",
            "ticker": ticker,
            "result": result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Tech stack analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/tech",
    tags=["6. Tech Stack Signals"],
    summary="Analyze tech stack signals for all companies"
)
async def analyze_all_tech_signals(force_refresh: bool = False):
    """Analyze tech stack for all target companies."""
    logger.info("Tech stack analysis request for ALL companies")

    try:
        service = get_tech_signal_service()
        result = await service.analyze_all_companies(force_refresh=force_refresh)
        return result
    except Exception as e:
        logger.error(f"Batch tech stack analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SECTION 7: COMBINED ANALYSIS
# ============================================================

@router.post(
    "/analyze/{ticker}",
    tags=["7. Combined Analysis"],
    summary="Run all signal analyses for a company",
    description="Run hiring, innovation, tech stack, and leadership analysis for a single company."
)
async def analyze_all_signals_for_company(ticker: str, force_refresh: bool = False):
    """Run all signal analyses for a company."""
    logger.info(f"Full signal analysis request for: {ticker}")

    results = {
        "ticker": ticker,
        "status": "success",
        "hiring": None,
        "innovation": None,
        "tech_stack": None,
        "leadership": None,
        "errors": []
    }

    # Hiring signals
    try:
        service = get_job_signal_service()
        results["hiring"] = await service.analyze_company(ticker, force_refresh=force_refresh)
    except Exception as e:
        results["errors"].append({"signal": "hiring", "error": str(e)})

    # Innovation signals
    try:
        service = get_patent_signal_service()
        results["innovation"] = await service.analyze_company(ticker)
    except Exception as e:
        results["errors"].append({"signal": "innovation", "error": str(e)})

    # Tech stack signals
    try:
        service = get_tech_signal_service()
        results["tech_stack"] = await service.analyze_company(ticker, force_refresh=force_refresh)
    except Exception as e:
        results["errors"].append({"signal": "tech_stack", "error": str(e)})

    # Leadership signals
    try:
        service = get_leadership_service()
        results["leadership"] = await service.analyze_company(ticker)
    except Exception as e:
        results["errors"].append({"signal": "leadership", "error": str(e)})

    if results["errors"]:
        results["status"] = "partial_success"

    return results


@router.post(
    "/analyze",
    tags=["7. Combined Analysis"],
    summary="Run all signal analyses for all companies",
    description="Run hiring, innovation, tech stack, and leadership analysis for all companies."
)
async def analyze_all_signals_for_all_companies(force_refresh: bool = False):
    """Run all signal analyses for all companies."""
    logger.info("Full signal analysis request for ALL companies")

    company_repo = CompanyRepository()
    companies = company_repo.get_all()

    results = {
        "status": "success",
        "total_companies": len(companies),
        "companies": [],
        "summary": {
            "hiring_success": 0,
            "innovation_success": 0,
            "tech_stack_success": 0,
            "leadership_success": 0,
            "total_errors": 0
        }
    }

    for company in companies:
        ticker = company.get('ticker')
        if not ticker:
            continue

        company_result = {
            "ticker": ticker,
            "company_name": company.get('name'),
            "hiring": None,
            "innovation": None,
            "tech_stack": None,
            "leadership": None,
            "errors": []
        }

        # Hiring signals
        try:
            service = get_job_signal_service()
            company_result["hiring"] = await service.analyze_company(ticker, force_refresh=force_refresh)
            results["summary"]["hiring_success"] += 1
        except Exception as e:
            company_result["errors"].append({"signal": "hiring", "error": str(e)})
            results["summary"]["total_errors"] += 1

        # Innovation signals
        try:
            service = get_patent_signal_service()
            company_result["innovation"] = await service.analyze_company(ticker)
            results["summary"]["innovation_success"] += 1
        except Exception as e:
            company_result["errors"].append({"signal": "innovation", "error": str(e)})
            results["summary"]["total_errors"] += 1

        # Tech stack signals
        try:
            service = get_tech_signal_service()
            company_result["tech_stack"] = await service.analyze_company(ticker, force_refresh=force_refresh)
            results["summary"]["tech_stack_success"] += 1
        except Exception as e:
            company_result["errors"].append({"signal": "tech_stack", "error": str(e)})
            results["summary"]["total_errors"] += 1

        # Leadership signals
        try:
            service = get_leadership_service()
            company_result["leadership"] = await service.analyze_company(ticker)
            results["summary"]["leadership_success"] += 1
        except Exception as e:
            company_result["errors"].append({"signal": "leadership", "error": str(e)})
            results["summary"]["total_errors"] += 1

        results["companies"].append(company_result)

    if results["summary"]["total_errors"] > 0:
        results["status"] = "partial_success"

    return results