from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone
import logging
from app.services.leadership_service import get_leadership_service
from app.repositories.signal_repository import get_signal_repository
from app.repositories.company_repository import CompanyRepository
import json

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/signals",
    # tags=["Signals"],
)


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
# SECTION 4: PLACEHOLDER FOR FUTURE SIGNALS
# ============================================================

@router.post(
    "/hiring/{ticker}",
    tags=["4. Future Signals (Placeholder)"],
    summary="[PLACEHOLDER] Analyze hiring signals",
    description="This endpoint will be implemented when job posting APIs are integrated."
)
async def analyze_hiring_signals(ticker: str):
    """Placeholder for hiring signal analysis."""
    return {
        "status": "not_implemented",
        "message": "Hiring signals require integration with LinkedIn/Indeed APIs. To be implemented in future case study.",
        "ticker": ticker
    }


@router.post(
    "/innovation/{ticker}",
    tags=["4. Future Signals (Placeholder)"],
    summary="[PLACEHOLDER] Analyze innovation signals",
    description="This endpoint will be implemented when USPTO patent API is integrated."
)
async def analyze_innovation_signals(ticker: str):
    """Placeholder for innovation signal analysis."""
    return {
        "status": "not_implemented",
        "message": "Innovation signals require integration with USPTO patent API. To be implemented in future case study.",
        "ticker": ticker
    }


@router.post(
    "/tech/{ticker}",
    tags=["4. Future Signals (Placeholder)"],
    summary="[PLACEHOLDER] Analyze tech stack signals",
    description="This endpoint will be implemented when BuiltWith API is integrated."
)
async def analyze_tech_signals(ticker: str):
    """Placeholder for tech stack signal analysis."""
    return {
        "status": "not_implemented",
        "message": "Tech stack signals require integration with BuiltWith API. To be implemented in future case study.",
        "ticker": ticker
    }