# from __future__ import annotations

# from fastapi import APIRouter, HTTPException, Query
# from pydantic import BaseModel
# from typing import Optional

# from app.services.snowflake import SnowflakeService

# router = APIRouter(prefix="/api/v1/documents", tags=["documents"])


# class DocumentCollectRequest(BaseModel):
#     company_id: str
#     ticker: str
#     filing_types: list[str] = ["10-K", "10-Q", "8-K", "DEF 14A"]
#     after: str = "2021-01-01"
#     limit: int = 10


# class DocumentCollectResponse(BaseModel):
#     status: str
#     message: str


# @router.post("/collect", response_model=DocumentCollectResponse)
# async def collect_documents(req: DocumentCollectRequest):
#     """
#     DEPRECATED: Use the new SEC Pipeline endpoints instead.
    
#     New endpoints:
#     - POST /api/v1/sec/download
#     - GET /api/v1/sec/parse
#     - GET /api/v1/sec/deduplicate
#     - POST /api/v1/sec/chunk
#     - GET /api/v1/sec/extract-items
#     - GET /api/v1/sec/stats
#     """
#     return DocumentCollectResponse(
#         status="deprecated",
#         message="This endpoint is deprecated. Please use the new SEC Pipeline: POST /api/v1/sec/download",
#     )


# @router.get("")
# async def list_documents(ticker: Optional[str] = Query(default=None)):
#     """List all documents from Snowflake, optionally filtered by ticker."""
#     db = SnowflakeService()
#     try:
#         return db.list_documents(ticker=ticker.upper() if ticker else None)
#     finally:
#         db.close()


# @router.get("/{doc_id}")
# async def get_document(doc_id: str):
#     """Get a single document by ID."""
#     db = SnowflakeService()
#     try:
#         doc = db.get_document(doc_id)
#         if not doc:
#             raise HTTPException(status_code=404, detail="Document not found")
#         return doc
#     finally:
#         db.close()


# @router.get("/{doc_id}/chunks")
# async def get_document_chunks(doc_id: str):
#     """Get all chunks for a document."""
#     db = SnowflakeService()
#     try:
#         return db.get_document_chunks(doc_id)
#     finally:
#         db.close()


# from fastapi import APIRouter, HTTPException, Query
# from typing import List, Optional
# import logging
# from app.models.document import (
#     DocumentCollectionRequest,
#     DocumentCollectionResponse,
#     DocumentMetadata,
#     FilingType,
#     ParseByTickerResponse,
#     ParseAllResponse
# )
# from app.services.document_collector import get_document_collector_service
# from app.services.document_parsing import get_document_parsing_service
# from app.repositories.document_repository import get_document_repository

# logger = logging.getLogger(__name__)

# router = APIRouter(
#     prefix="/api/v1/documents",
#     tags=["Documents"],
# )


# @router.post(
#     "/collect",
#     response_model=DocumentCollectionResponse,
#     summary="Trigger document collection for a company",
#     description="""
#     Collect SEC filings for a single company.
    
#     This endpoint:
#     1. Downloads filings from SEC EDGAR (with rate limiting)
#     2. Uploads raw documents to S3
#     3. Saves metadata to Snowflake
#     4. Deduplicates based on content hash
    
#     **Filing Types:**
#     - 10-K: Annual reports (Strategy, Risk Factors, MD&A)
#     - 10-Q: Quarterly reports (Recent developments)
#     - 8-K: Material events (AI announcements, executive changes)
#     - DEF 14A: Proxy statements (Executive compensation)
    
#     **Target Companies:** CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS
#     """
# )
# async def collect_documents(request: DocumentCollectionRequest):
#     """
#     Trigger document collection for a company.
    
#     Progress is logged to the terminal in real-time.
#     """
#     logger.info(f"📥 Received collection request for: {request.ticker}")
    
#     try:
#         service = get_document_collector_service()
#         result = service.collect_for_company(request)
#         return result
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         logger.error(f"Collection failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


# @router.post(
#     "/collect/all",
#     response_model=List[DocumentCollectionResponse],
#     summary="Collect documents for all 10 target companies",
#     description="Batch collection for all target companies: CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS"
# )
# async def collect_all_documents(
#     filing_types: List[FilingType] = Query(
#         default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A],
#         description="Filing types to collect"
#     ),
#     years_back: int = Query(default=3, ge=1, le=10, description="Years of history")
# ):
#     """Collect documents for all 10 target companies"""
#     logger.info(f"📥 Starting batch collection for all companies")
    
#     try:
#         service = get_document_collector_service()
#         results = service.collect_for_all_companies(
#             filing_types=[ft.value for ft in filing_types],
#             years_back=years_back
#         )
#         return results
#     except Exception as e:
#         logger.error(f"Batch collection failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Batch collection failed: {str(e)}")


# @router.get(
#     "",
#     summary="List documents",
#     description="Get all documents, optionally filtered by company or filing type"
# )
# async def list_documents(
#     ticker: Optional[str] = Query(None, description="Filter by ticker"),
#     filing_type: Optional[str] = Query(None, description="Filter by filing type"),
#     limit: int = Query(100, ge=1, le=500),
#     offset: int = Query(0, ge=0)
# ):
#     """List documents with optional filtering"""
#     repo = get_document_repository()
    
#     if ticker:
#         docs = repo.get_by_ticker(ticker.upper())
#     else:
#         docs = repo.get_all(limit=limit, offset=offset)
    
#     # Filter by filing type if specified
#     if filing_type:
#         docs = [d for d in docs if d['filing_type'] == filing_type]
    
#     return docs


# @router.get(
#     "/stats/{ticker}",
#     summary="Get document statistics for a company",
#     description="Get counts of documents by filing type for a ticker"
# )
# async def get_document_stats(ticker: str):
#     """Get document statistics for a company"""
#     repo = get_document_repository()
#     counts = repo.count_by_ticker(ticker.upper())
    
#     total = sum(counts.values())
    
#     return {
#         "ticker": ticker.upper(),
#         "total_documents": total,
#         "by_filing_type": counts
#     }


# @router.get(
#     "/{document_id}",
#     summary="Get document by ID",
#     description="Get a single document with its metadata"
# )
# async def get_document(document_id: str):
#     """Get document by ID"""
#     repo = get_document_repository()
#     doc = repo.get_by_id(document_id)
    
#     if not doc:
#         raise HTTPException(status_code=404, detail="Document not found")
    
#     return doc


# # ============================================================
# # PARSING ENDPOINTS
# # ============================================================

# @router.post(
#     "/parse/{ticker}",
#     response_model=ParseByTickerResponse,
#     summary="Parse all documents for a company",
#     description="""
#     Parse all collected SEC filings for a single company.
    
#     This endpoint:
#     1. Downloads raw documents from S3 (sec/raw/{ticker}/...)
#     2. Parses HTML and PDF content (extracts text & tables)
#     3. Uploads parsed JSON to S3 (sec/parsed/{ticker}/...)
#     4. Updates document status in Snowflake
    
#     **Extracts:**
#     - Full text content
#     - Tables (with headers and rows)
#     - Key sections (Risk Factors, MD&A, Business, etc.)
    
#     **Skips:** Already parsed documents (status = 'parsed')
#     """
# )
# async def parse_documents_by_ticker(ticker: str):
#     """Parse all documents for a specific company"""
#     logger.info(f"📥 Received parse request for: {ticker}")
    
#     try:
#         service = get_document_parsing_service()
#         result = service.parse_by_ticker(ticker)
#         return result
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         logger.error(f"Parsing failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Parsing failed: {str(e)}")


# @router.post(
#     "/parse",
#     response_model=ParseAllResponse,
#     summary="Parse documents for all companies",
#     description="Parse all collected SEC filings for all 10 target companies: CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS"
# )
# async def parse_all_documents():
#     """Parse documents for all 10 target companies"""
#     logger.info(f"📥 Starting batch parsing for all companies")
    
#     try:
#         service = get_document_parsing_service()
#         result = service.parse_all_companies()
#         return result
#     except Exception as e:
#         logger.error(f"Batch parsing failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Batch parsing failed: {str(e)}")


# @router.post(
#     "/parse/document/{document_id}",
#     summary="Parse a single document",
#     description="Parse a specific document by its ID"
# )
# async def parse_single_document(document_id: str):
#     """Parse a single document by ID"""
#     logger.info(f"📥 Received parse request for document: {document_id}")
    
#     try:
#         service = get_document_parsing_service()
#         result = service.parse_document(document_id)
#         return result
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         logger.error(f"Parsing failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Parsing failed: {str(e)}")


from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone
import logging
from app.models.document import (
    DocumentCollectionRequest,
    DocumentCollectionResponse,
    DocumentMetadata,
    FilingType,
    ParseByTickerResponse,
    ParseAllResponse,
    EvidenceCollectionReport,
    SummaryStatistics,
    CompanyDocumentStats
)
from app.services.document_collector import get_document_collector_service
from app.services.document_parsing import get_document_parsing_service
from app.repositories.document_repository import get_document_repository

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/documents",
    tags=["Documents"],
)

@router.post(
    "/collect",
    response_model=DocumentCollectionResponse,
    summary="Trigger document collection for a company",
    description="""
    Collect SEC filings for a single company.
    
    This endpoint:
    1. Downloads filings from SEC EDGAR (with rate limiting)
    2. Uploads raw documents to S3
    3. Saves metadata to Snowflake
    4. Deduplicates based on content hash
    
    **Filing Types:**
    - 10-K: Annual reports (Strategy, Risk Factors, MD&A)
    - 10-Q: Quarterly reports (Recent developments)
    - 8-K: Material events (AI announcements, executive changes)
    - DEF 14A: Proxy statements (Executive compensation)
    
    **Target Companies:** CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS
    """
)
async def collect_documents(request: DocumentCollectionRequest):
    """
    Trigger document collection for a company.
    
    Progress is logged to the terminal in real-time.
    """
    logger.info(f"📥 Received collection request for: {request.ticker}")
    
    try:
        service = get_document_collector_service()
        result = service.collect_for_company(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@router.post(
    "/collect/all",
    response_model=List[DocumentCollectionResponse],
    summary="Collect documents for all 10 target companies",
    description="Batch collection for all target companies: CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS"
)
async def collect_all_documents(
    filing_types: List[FilingType] = Query(
        default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A],
        description="Filing types to collect"
    ),
    years_back: int = Query(default=3, ge=1, le=10, description="Years of history")
):
    """Collect documents for all 10 target companies"""
    logger.info(f"📥 Starting batch collection for all companies")
    
    try:
        service = get_document_collector_service()
        results = service.collect_for_all_companies(
            filing_types=[ft.value for ft in filing_types],
            years_back=years_back
        )
        return results
    except Exception as e:
        logger.error(f"Batch collection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Batch collection failed: {str(e)}")


@router.get(
    "",
    summary="List documents",
    description="Get all documents, optionally filtered by company or filing type"
)
async def list_documents(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    filing_type: Optional[str] = Query(None, description="Filter by filing type"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List documents with optional filtering"""
    repo = get_document_repository()
    
    if ticker:
        docs = repo.get_by_ticker(ticker.upper())
    else:
        docs = repo.get_all(limit=limit, offset=offset)
    
    # Filter by filing type if specified
    if filing_type:
        docs = [d for d in docs if d['filing_type'] == filing_type]
    
    return docs


@router.get(
    "/stats/{ticker}",
    summary="Get document statistics for a company",
    description="Get counts of documents by filing type for a ticker"
)
async def get_document_stats(ticker: str):
    """Get document statistics for a company"""
    repo = get_document_repository()
    counts = repo.count_by_ticker(ticker.upper())
    
    total = sum(counts.values())
    
    return {
        "ticker": ticker.upper(),
        "total_documents": total,
        "by_filing_type": counts
    }


@router.get(
    "/{document_id}",
    summary="Get document by ID",
    description="Get a single document with its metadata"
)
async def get_document(document_id: str):
    """Get document by ID"""
    repo = get_document_repository()
    doc = repo.get_by_id(document_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return doc


# ============================================================
# PARSING ENDPOINTS
# ============================================================

@router.post(
    "/parse/{ticker}",
    response_model=ParseByTickerResponse,
    summary="Parse all documents for a company",
    description="""
    Parse all collected SEC filings for a single company.
    
    This endpoint:
    1. Downloads raw documents from S3 (sec/raw/{ticker}/...)
    2. Parses HTML and PDF content (extracts text & tables)
    3. Uploads parsed JSON to S3 (sec/parsed/{ticker}/...)
    4. Updates document status in Snowflake
    
    **Extracts:**
    - Full text content
    - Tables (with headers and rows)
    - Key sections (Risk Factors, MD&A, Business, etc.)
    
    **Skips:** Already parsed documents (status = 'parsed')
    """
)
async def parse_documents_by_ticker(ticker: str):
    """Parse all documents for a specific company"""
    logger.info(f"📥 Received parse request for: {ticker}")
    
    try:
        service = get_document_parsing_service()
        result = service.parse_by_ticker(ticker)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Parsing failed: {str(e)}")


@router.post(
    "/parse",
    response_model=ParseAllResponse,
    summary="Parse documents for all companies",
    description="Parse all collected SEC filings for all 10 target companies: CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS"
)
async def parse_all_documents():
    """Parse documents for all 10 target companies"""
    logger.info(f"📥 Starting batch parsing for all companies")
    
    try:
        service = get_document_parsing_service()
        result = service.parse_all_companies()
        return result
    except Exception as e:
        logger.error(f"Batch parsing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Batch parsing failed: {str(e)}")


@router.post(
    "/parse/document/{document_id}",
    summary="Parse a single document",
    description="Parse a specific document by its ID"
)
async def parse_single_document(document_id: str):
    """Parse a single document by ID"""
    logger.info(f"📥 Received parse request for document: {document_id}")
    
    try:
        service = get_document_parsing_service()
        result = service.parse_document(document_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Parsing failed: {str(e)}")
    
# ============================================================
# REPORT ENDPOINT (at top to avoid route conflicts)
# ============================================================

@router.get(
    "/report",
    response_model=EvidenceCollectionReport,
    summary="Generate Evidence Collection Report",
    description="""
    Generate a comprehensive report of all collected evidence.
    
    **Returns:**
    - Summary statistics (companies, documents, chunks, words)
    - Documents by company (10-K, 10-Q, 8-K, DEF 14A counts)
    - Status breakdown (pending, parsed, chunked, etc.)
    
    Use this for the Evidence Collection Report template.
    """
)
async def get_evidence_report():
    """Generate evidence collection report"""
    logger.info("📊 Generating evidence collection report...")
    
    repo = get_document_repository()
    
    # Get summary statistics
    summary_data = repo.get_summary_statistics()
    status_breakdown = repo.get_status_breakdown()
    
    summary = SummaryStatistics(
        companies_processed=summary_data["companies_processed"],
        total_documents=summary_data["total_documents"],
        total_chunks=summary_data["total_chunks"],
        total_words=summary_data["total_words"],
        documents_by_status=status_breakdown
    )
    
    # Get per-company stats
    company_stats_raw = repo.get_all_company_stats()
    company_stats = [
        CompanyDocumentStats(
            ticker=cs["ticker"],
            form_10k=cs["form_10k"],
            form_10q=cs["form_10q"],
            form_8k=cs["form_8k"],
            def_14a=cs["def_14a"],
            total=cs["total"],
            chunks=cs["chunks"],
            word_count=cs["word_count"]
        )
        for cs in company_stats_raw
    ]
    
    logger.info(f"✅ Report generated: {summary.total_documents} documents across {summary.companies_processed} companies")
    
    return EvidenceCollectionReport(
        report_generated_at=datetime.now(timezone.utc),
        summary=summary,
        documents_by_company=company_stats,
        status_breakdown=status_breakdown
    )


@router.get(
    "/report/markdown",
    summary="Generate Evidence Collection Report (Markdown)",
    description="Generate the report in Markdown format for easy copy-paste"
)
async def get_evidence_report_markdown():
    """Generate evidence collection report in Markdown format"""
    logger.info("📊 Generating Markdown evidence collection report...")
    
    repo = get_document_repository()
    
    # Get data
    summary = repo.get_summary_statistics()
    status_breakdown = repo.get_status_breakdown()
    company_stats = repo.get_all_company_stats()
    
    # Build Markdown
    md = []
    md.append("# Evidence Collection Report")
    md.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    md.append("")
    
    # Summary Statistics
    md.append("## Summary Statistics")
    md.append("| Metric | Value |")
    md.append("|--------|-------|")
    md.append(f"| Companies processed | {summary['companies_processed']} |")
    md.append(f"| Total documents | {summary['total_documents']} |")
    md.append(f"| Total chunks | {summary['total_chunks']} |")
    md.append(f"| Total words | {summary['total_words']:,} |")
    md.append("")
    
    # Status Breakdown
    md.append("## Status Breakdown")
    md.append("| Status | Count |")
    md.append("|--------|-------|")
    for status, count in sorted(status_breakdown.items()):
        md.append(f"| {status} | {count} |")
    md.append("")
    
    # Documents by Company
    md.append("## Documents by Company")
    md.append("| Ticker | 10-K | 10-Q | 8-K | DEF 14A | Total | Chunks | Words |")
    md.append("|--------|------|------|-----|---------|-------|--------|-------|")
    for cs in company_stats:
        md.append(f"| {cs['ticker']} | {cs['form_10k']} | {cs['form_10q']} | {cs['form_8k']} | {cs['def_14a']} | {cs['total']} | {cs['chunks']} | {cs['word_count']:,} |")
    md.append("")
    
    # Totals row
    total_10k = sum(cs['form_10k'] for cs in company_stats)
    total_10q = sum(cs['form_10q'] for cs in company_stats)
    total_8k = sum(cs['form_8k'] for cs in company_stats)
    total_def14a = sum(cs['def_14a'] for cs in company_stats)
    total_docs = sum(cs['total'] for cs in company_stats)
    total_chunks = sum(cs['chunks'] for cs in company_stats)
    total_words = sum(cs['word_count'] for cs in company_stats)
    md.append(f"| **TOTAL** | **{total_10k}** | **{total_10q}** | **{total_8k}** | **{total_def14a}** | **{total_docs}** | **{total_chunks}** | **{total_words:,}** |")
    
    return {"markdown": "\n".join(md)}

