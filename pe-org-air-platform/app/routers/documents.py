# from fastapi import APIRouter, HTTPException, Query
# from typing import List, Optional
# from datetime import datetime, timezone
# import logging
# from app.models.document import (
#     DocumentCollectionRequest,
#     DocumentCollectionResponse,
#     FilingType,
#     ParseByTickerResponse,
#     ParseAllResponse,
#     EvidenceCollectionReport,
#     SummaryStatistics,
#     CompanyDocumentStats
# )
# from app.services.document_collector import get_document_collector_service
# from app.services.document_parsing_service import get_document_parsing_service
# from app.services.document_chunking_service import get_document_chunking_service
# from app.repositories.document_repository import get_document_repository
# from app.repositories.chunk_repository import get_chunk_repository
# from app.services.s3_storage import get_s3_service
# from app.repositories.signal_repository import get_signal_repository
# import json

# logger = logging.getLogger(__name__)

# router = APIRouter(
#     prefix="/api/v1/documents",
#     # tags=["Documents"],
# )


# 
# # SECTION 1: DOCUMENT COLLECTION
# 

# @router.post(
#     "/collect",
#     response_model=DocumentCollectionResponse,
#     tags=["1. Collection"],
#     summary="Collect SEC filings for a company",
#     description="""
#     Download SEC filings for a single company.
    
#     **Process:**
#     1. Downloads filings from SEC EDGAR (with rate limiting)
#     2. Uploads raw documents to S3 (sec/raw/{ticker}/...)
#     3. Saves metadata to Snowflake
#     4. Deduplicates based on content hash
    
#     **Filing Types:** 10-K, 10-Q, 8-K, DEF 14A
#     """
# )
# async def collect_documents(request: DocumentCollectionRequest):
#     """Collect SEC filings for a company"""
#     logger.info(f"📥 Collection request for: {request.ticker}")
#     try:
#         service = get_document_collector_service()
#         return service.collect_for_company(request)
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         logger.error(f"Collection failed: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# @router.post(
#     "/collect/all",
#     response_model=List[DocumentCollectionResponse],
#     tags=["1. Collection"],
#     summary="Collect SEC filings for all 10 companies"
# )
# async def collect_all_documents(
#     filing_types: List[FilingType] = Query(
#         default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A]
#     ),
#     years_back: int = Query(default=3, ge=1, le=10)
# ):
#     """Collect documents for all 10 target companies"""
#     logger.info("📥 Batch collection for all companies")
#     try:
#         service = get_document_collector_service()
#         return service.collect_for_all_companies([ft.value for ft in filing_types], years_back)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# 
# # SECTION 2: DOCUMENT PARSING
# 

# @router.post(
#     "/parse/{ticker}",
#     response_model=ParseByTickerResponse,
#     tags=["2. Parsing"],
#     summary="Parse all documents for a company",
#     description="""
#     Parse all collected SEC filings for a company.
    
#     **Process:**
#     1. Downloads raw documents from S3
#     2. Extracts text and tables (HTML/PDF)
#     3. Identifies key sections (Risk Factors, MD&A, etc.)
#     4. Uploads parsed JSON to S3 (sec/parsed/{ticker}/...)
#     5. Updates word_count in Snowflake
#     """
# )
# async def parse_documents_by_ticker(ticker: str):
#     """Parse all documents for a company"""
#     logger.info(f"📄 Parse request for: {ticker}")
#     try:
#         service = get_document_parsing_service()
#         return service.parse_by_ticker(ticker)
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.post(
#     "/parse",
#     response_model=ParseAllResponse,
#     tags=["2. Parsing"],
#     summary="Parse documents for all companies"
# )
# async def parse_all_documents():
#     """Parse documents for all 10 target companies"""
#     logger.info("📄 Batch parsing for all companies")
#     try:
#         service = get_document_parsing_service()
#         return service.parse_all_companies()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.get(
#     "/parsed/{document_id}",
#     tags=["2. Parsing"],
#     summary="View parsed document content",
#     description="Get the parsed content of a document from S3"
# )
# async def get_parsed_document(document_id: str):
#     """Get parsed document content by ID"""
#     logger.info(f"📄 Getting parsed document: {document_id}")
    
#     repo = get_document_repository()
#     doc = repo.get_by_id(document_id)
    
#     if not doc:
#         raise HTTPException(status_code=404, detail="Document not found")
    
#     ticker = doc['ticker']
#     filing_type = doc['filing_type']
#     filing_date = str(doc['filing_date'])
    
#     # Get parsed content from S3
#     clean_filing_type = filing_type.replace(" ", "")
#     s3_key = f"sec/parsed/{ticker}/{clean_filing_type}/{filing_date}_full.json"
    
#     s3_service = get_s3_service()
#     content = s3_service.get_file(s3_key)
    
#     if not content:
#         raise HTTPException(status_code=404, detail=f"Parsed content not found. Document may not be parsed yet. S3 key: {s3_key}")
    
#     parsed_data = json.loads(content.decode('utf-8'))
    
#     return {
#         "document_id": document_id,
#         "ticker": ticker,
#         "filing_type": filing_type,
#         "filing_date": filing_date,
#         "s3_key": s3_key,
#         "word_count": parsed_data.get('word_count', 0),
#         "table_count": parsed_data.get('table_count', 0),
#         "sections": list(parsed_data.get('sections', {}).keys()),
#         "text_preview": parsed_data.get('text_content', '')[:2000] + "..." if parsed_data.get('text_content') else "",
#         "tables": parsed_data.get('tables', [])[:5]  # First 5 tables
#     }


# 
# # SECTION 3: DOCUMENT CHUNKING
# 

# @router.post(
#     "/chunk/{ticker}",
#     tags=["3. Chunking"],
#     summary="Chunk all parsed documents for a company",
#     description="""
#     Split parsed documents into smaller chunks for LLM processing.
    
#     **Process:**
#     1. Downloads parsed content from S3
#     2. Splits into overlapping chunks (preserves context)
#     3. Uploads chunks to S3 (sec/chunks/{ticker}/...)
#     4. Saves chunk metadata to Snowflake
    
#     **Parameters:**
#     - chunk_size: Target words per chunk (default: 750)
#     - chunk_overlap: Overlap between chunks (default: 50)
#     """
# )
# async def chunk_documents_by_ticker(
#     ticker: str,
#     chunk_size: int = Query(default=750, ge=100, le=2000, description="Words per chunk"),
#     chunk_overlap: int = Query(default=50, ge=0, le=200, description="Overlap between chunks")
# ):
#     """Chunk all parsed documents for a company"""
#     logger.info(f"📦 Chunk request for: {ticker}")
#     try:
#         service = get_document_chunking_service()
#         return service.chunk_by_ticker(ticker, chunk_size, chunk_overlap)
#     except ValueError as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.post(
#     "/chunk",
#     tags=["3. Chunking"],
#     summary="Chunk documents for all companies"
# )
# async def chunk_all_documents(
#     chunk_size: int = Query(default=750, ge=100, le=2000),
#     chunk_overlap: int = Query(default=50, ge=0, le=200)
# ):
#     """Chunk documents for all 10 target companies"""
#     logger.info("📦 Batch chunking for all companies")
#     try:
#         service = get_document_chunking_service()
#         return service.chunk_all_companies(chunk_size, chunk_overlap)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.get(
#     "/chunks/{document_id}",
#     tags=["3. Chunking"],
#     summary="Get chunks for a document"
# )
# async def get_document_chunks(document_id: str):
#     """Get all chunks for a specific document"""
#     chunk_repo = get_chunk_repository()
#     chunks = chunk_repo.get_by_document_id(document_id)
    
#     if not chunks:
#         raise HTTPException(status_code=404, detail="No chunks found for this document")
    
#     return {
#         "document_id": document_id,
#         "chunk_count": len(chunks),
#         "chunks": chunks
#     }


# @router.get(
#     "/chunk/stats/{ticker}",
#     tags=["3. Chunking"],
#     summary="Get chunk statistics for a company"
# )
# async def get_chunk_stats(ticker: str):
#     """Get chunk statistics for a company"""
#     chunk_repo = get_chunk_repository()
#     stats = chunk_repo.get_stats_by_ticker(ticker.upper())
#     total = chunk_repo.count_by_ticker(ticker.upper())
    
#     return {
#         "ticker": ticker.upper(),
#         "total_chunks": total,
#         "by_filing_type": stats
#     }


# 
# # SECTION 4: REPORTS & STATISTICS
# 

# @router.get(
#     "/report",
#     tags=["4. Reports"],
#     summary="Get Evidence Collection Report",
#     description="Get comprehensive statistics in JSON format"
# )
# async def get_evidence_report():
#     """Generate evidence collection report"""
#     logger.info("📊 Generating report...")
    
#     repo = get_document_repository()
#     chunk_repo = get_chunk_repository()
#     signal_repo = get_signal_repository()
    
#     summary = repo.get_summary_statistics()
#     status_breakdown = repo.get_status_breakdown()
#     company_stats = repo.get_all_company_stats()
    
#     # Get chunk counts
#     total_chunks = chunk_repo.get_total_chunks()
    
#     # Get total signals
#     total_signals = signal_repo.get_total_signal_count()
    
#     return {
#         "report_generated_at": datetime.now(timezone.utc).isoformat(),
#         "summary": {
#             "companies_processed": summary["companies_processed"],
#             "total_documents": summary["total_documents"],
#             "total_chunks": total_chunks,
#             "total_signals": total_signals,
#             "total_words": summary["total_words"]
#         },
#         "status_breakdown": status_breakdown,
#         "documents_by_company": company_stats
#     }


# @router.get(
#     "/report/table",
#     tags=["4. Reports"],
#     summary="Get Evidence Collection Report (Table Format)",
#     description="Get report formatted as tables for easy viewing"
# )
# async def get_evidence_report_table():
#     """Generate report in table format"""
#     logger.info("📊 Generating table report...")
    
#     repo = get_document_repository()
#     chunk_repo = get_chunk_repository()
#     signal_repo = get_signal_repository()
    
#     summary = repo.get_summary_statistics()
#     status_breakdown = repo.get_status_breakdown()
#     company_stats = repo.get_all_company_stats()
#     total_chunks = chunk_repo.get_total_chunks()
#     total_signals = signal_repo.get_total_signal_count()
    
#     # Build summary table
#     summary_table = {
#         "headers": ["Metric", "Value"],
#         "rows": [
#             ["Companies Processed", summary["companies_processed"]],
#             ["Total Documents", summary["total_documents"]],
#             ["Total Chunks", total_chunks],
#             ["Total Signals", total_signals],
#             ["Total Words", f"{summary['total_words']:,}"]
#         ]
#     }
    
#     # Build status table
#     status_table = {
#         "headers": ["Status", "Count"],
#         "rows": [[status, count] for status, count in sorted(status_breakdown.items())]
#     }
    
#     # Build company table
#     company_table = {
#         "headers": ["Ticker", "10-K", "10-Q", "8-K", "DEF 14A", "Total", "Chunks", "Words"],
#         "rows": []
#     }
    
#     for cs in company_stats:
#         company_table["rows"].append([
#             cs["ticker"],
#             cs["form_10k"],
#             cs["form_10q"],
#             cs["form_8k"],
#             cs["def_14a"],
#             cs["total"],
#             cs["chunks"],
#             f"{cs['word_count']:,}"
#         ])
    
#     # Add totals row
#     totals = [
#         "TOTAL",
#         sum(cs["form_10k"] for cs in company_stats),
#         sum(cs["form_10q"] for cs in company_stats),
#         sum(cs["form_8k"] for cs in company_stats),
#         sum(cs["def_14a"] for cs in company_stats),
#         sum(cs["total"] for cs in company_stats),
#         sum(cs["chunks"] for cs in company_stats),
#         f"{sum(cs['word_count'] for cs in company_stats):,}"
#     ]
#     company_table["rows"].append(totals)
    
#     return {
#         "report_generated_at": datetime.now(timezone.utc).isoformat(),
#         "summary_table": summary_table,
#         "status_table": status_table,
#         "company_table": company_table
#     }


# 
# # SECTION 5: DOCUMENT MANAGEMENT
# 

# @router.get(
#     "",
#     tags=["5. Management"],
#     summary="List all documents"
# )
# async def list_documents(
#     ticker: Optional[str] = Query(None),
#     filing_type: Optional[str] = Query(None),
#     status: Optional[str] = Query(None),
#     limit: int = Query(100, ge=1, le=500),
#     offset: int = Query(0, ge=0)
# ):
#     """List documents with optional filters"""
#     repo = get_document_repository()
    
#     if ticker:
#         docs = repo.get_by_ticker(ticker.upper())
#     else:
#         docs = repo.get_all(limit=limit, offset=offset)
    
#     if filing_type:
#         docs = [d for d in docs if d['filing_type'] == filing_type]
#     if status:
#         docs = [d for d in docs if d.get('status') == status]
    
#     return {"count": len(docs), "documents": docs}


# @router.get(
#     "/stats/{ticker}",
#     tags=["5. Management"],
#     summary="Get document statistics for a company"
# )
# async def get_document_stats(ticker: str):
#     """Get document statistics for a company"""
#     repo = get_document_repository()
#     return repo.get_company_stats(ticker.upper())


# @router.get(
#     "/{document_id}",
#     tags=["5. Management"],
#     summary="Get document by ID"
# )
# async def get_document(document_id: str):
#     """Get document metadata by ID"""
#     repo = get_document_repository()
#     doc = repo.get_by_id(document_id)
    
#     if not doc:
#         raise HTTPException(status_code=404, detail="Document not found")
    
#     return doc


# 
# # SECTION 6: DELETE / RESET (For Demo/Testing)
# 

# @router.delete(
#     "/reset/{ticker}",
#     tags=["6. Reset (Demo)"],
#     summary="Delete all data for a company",
#     description="""
#     **⚠️ FOR DEMO/TESTING ONLY**
    
#     Deletes ALL data for a company:
#     - S3: raw/, parsed/, chunks/ folders
#     - Snowflake: documents and document_chunks records
    
#     Use this to demonstrate the full pipeline from scratch.
#     """
# )
# async def reset_company_data(ticker: str):
#     """Delete all data for a company (raw, parsed, chunks)"""
#     ticker = ticker.upper()
#     logger.info(f"🗑️ RESETTING ALL DATA FOR: {ticker}")
    
#     doc_repo = get_document_repository()
#     chunk_repo = get_chunk_repository()
#     s3_service = get_s3_service()
    
#     results = {
#         "ticker": ticker,
#         "s3_deleted": {"raw": 0, "parsed": 0, "chunks": 0},
#         "snowflake_deleted": {"chunks": 0, "documents": 0}
#     }
    
#     # 1. Delete chunks from Snowflake
#     logger.info(f"  💾 Deleting chunks from Snowflake...")
#     chunks_deleted = chunk_repo.delete_by_ticker(ticker)
#     results["snowflake_deleted"]["chunks"] = chunks_deleted
#     logger.info(f"  ✅ Deleted {chunks_deleted} chunk records")
    
#     # 2. Delete documents from Snowflake
#     logger.info(f"  💾 Deleting documents from Snowflake...")
#     docs_deleted = doc_repo.delete_by_ticker(ticker)
#     results["snowflake_deleted"]["documents"] = docs_deleted
#     logger.info(f"  ✅ Deleted {docs_deleted} document records")
    
#     # 3. Delete from S3 (raw, parsed, chunks folders)
#     for folder in ["raw", "parsed", "chunks"]:
#         prefix = f"sec/{folder}/{ticker}/"
#         logger.info(f"  🪣 Deleting S3 folder: {prefix}")
        
#         try:
#             # List all objects with this prefix
#             response = s3_service.s3_client.list_objects_v2(
#                 Bucket=s3_service.bucket_name,
#                 Prefix=prefix
#             )
            
#             objects = response.get('Contents', [])
#             if objects:
#                 # Delete all objects
#                 delete_keys = [{'Key': obj['Key']} for obj in objects]
#                 s3_service.s3_client.delete_objects(
#                     Bucket=s3_service.bucket_name,
#                     Delete={'Objects': delete_keys}
#                 )
#                 results["s3_deleted"][folder] = len(delete_keys)
#                 logger.info(f"  ✅ Deleted {len(delete_keys)} files from {folder}/")
#             else:
#                 logger.info(f"  ℹ️ No files found in {folder}/")
#         except Exception as e:
#             logger.error(f"  ❌ Error deleting {folder}/: {e}")
    
#     logger.info(f"🗑️ RESET COMPLETE FOR: {ticker}")
#     return results


# @router.delete(
#     "/reset/{ticker}/raw",
#     tags=["6. Reset (Demo)"],
#     summary="Delete only raw files for a company"
# )
# async def reset_raw_only(ticker: str):
#     """Delete only raw files (keeps parsed and chunks)"""
#     ticker = ticker.upper()
#     logger.info(f"🗑️ Deleting RAW files for: {ticker}")
    
#     s3_service = get_s3_service()
#     prefix = f"sec/raw/{ticker}/"
    
#     try:
#         response = s3_service.s3_client.list_objects_v2(
#             Bucket=s3_service.bucket_name,
#             Prefix=prefix
#         )
        
#         objects = response.get('Contents', [])
#         deleted = 0
        
#         if objects:
#             delete_keys = [{'Key': obj['Key']} for obj in objects]
#             s3_service.s3_client.delete_objects(
#                 Bucket=s3_service.bucket_name,
#                 Delete={'Objects': delete_keys}
#             )
#             deleted = len(delete_keys)
        
#         return {"ticker": ticker, "folder": "raw", "files_deleted": deleted}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.delete(
#     "/reset/{ticker}/parsed",
#     tags=["6. Reset (Demo)"],
#     summary="Delete parsed files and reset status"
# )
# async def reset_parsed_only(ticker: str):
#     """Delete parsed files and reset document status to 'uploaded'"""
#     ticker = ticker.upper()
#     logger.info(f"🗑️ Deleting PARSED files for: {ticker}")
    
#     s3_service = get_s3_service()
#     doc_repo = get_document_repository()
    
#     # Delete from S3
#     prefix = f"sec/parsed/{ticker}/"
#     try:
#         response = s3_service.s3_client.list_objects_v2(
#             Bucket=s3_service.bucket_name,
#             Prefix=prefix
#         )
        
#         objects = response.get('Contents', [])
#         deleted = 0
        
#         if objects:
#             delete_keys = [{'Key': obj['Key']} for obj in objects]
#             s3_service.s3_client.delete_objects(
#                 Bucket=s3_service.bucket_name,
#                 Delete={'Objects': delete_keys}
#             )
#             deleted = len(delete_keys)
        
#         # Reset status in Snowflake
#         doc_repo.reset_status_by_ticker(ticker, from_status='parsed', to_status='uploaded')
        
#         return {"ticker": ticker, "folder": "parsed", "files_deleted": deleted, "status_reset": "uploaded"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @router.delete(
#     "/reset/{ticker}/chunks",
#     tags=["6. Reset (Demo)"],
#     summary="Delete chunks and reset status"
# )
# async def reset_chunks_only(ticker: str):
#     """Delete chunks and reset document status to 'parsed'"""
#     ticker = ticker.upper()
#     logger.info(f"🗑️ Deleting CHUNKS for: {ticker}")
    
#     s3_service = get_s3_service()
#     doc_repo = get_document_repository()
#     chunk_repo = get_chunk_repository()
    
#     # Delete chunks from Snowflake
#     chunks_deleted = chunk_repo.delete_by_ticker(ticker)
    
#     # Delete from S3
#     prefix = f"sec/chunks/{ticker}/"
#     try:
#         response = s3_service.s3_client.list_objects_v2(
#             Bucket=s3_service.bucket_name,
#             Prefix=prefix
#         )
        
#         objects = response.get('Contents', [])
#         s3_deleted = 0
        
#         if objects:
#             delete_keys = [{'Key': obj['Key']} for obj in objects]
#             s3_service.s3_client.delete_objects(
#                 Bucket=s3_service.bucket_name,
#                 Delete={'Objects': delete_keys}
#             )
#             s3_deleted = len(delete_keys)
        
#         # Reset status and chunk_count in Snowflake
#         doc_repo.reset_status_by_ticker(ticker, from_status='chunked', to_status='parsed')
#         doc_repo.reset_chunk_count_by_ticker(ticker)
        
#         return {
#             "ticker": ticker, 
#             "folder": "chunks", 
#             "s3_files_deleted": s3_deleted,
#             "snowflake_chunks_deleted": chunks_deleted,
#             "status_reset": "parsed"
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone
import logging
from app.models.document import (
    DocumentCollectionRequest,
    DocumentCollectionResponse,
    FilingType,
    ParseByTickerResponse,
    ParseAllResponse,
    EvidenceCollectionReport,
    SummaryStatistics,
    CompanyDocumentStats
)
from app.services.document_collector import get_document_collector_service
from app.services.document_parsing_service import get_document_parsing_service
from app.services.document_chunking_service import get_document_chunking_service
from app.repositories.document_repository import get_document_repository
from app.repositories.chunk_repository import get_chunk_repository
from app.services.section_analysis_service import get_section_analysis_service
from app.services.s3_storage import get_s3_service
import json
from app.repositories.signal_repository import get_signal_repository

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/documents",
    # tags=["Documents"],
)



# SECTION 1: DOCUMENT COLLECTION


@router.post(
    "/collect",
    response_model=DocumentCollectionResponse,
    tags=["1. Collection"],
    summary="Collect SEC filings for a company",
    description="""
    Download SEC filings for a single company.
    
    **Process:**
    1. Downloads filings from SEC EDGAR (with rate limiting)
    2. Uploads raw documents to S3 (sec/raw/{ticker}/...)
    3. Saves metadata to Snowflake
    4. Deduplicates based on content hash
    
    **Filing Types:** 10-K, 10-Q, 8-K, DEF 14A
    """
)
async def collect_documents(request: DocumentCollectionRequest):
    """Collect SEC filings for a company"""
    logger.info(f"📥 Collection request for: {request.ticker}")
    try:
        service = get_document_collector_service()
        return service.collect_for_company(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/collect/all",
    response_model=List[DocumentCollectionResponse],
    tags=["1. Collection"],
    summary="Collect SEC filings for all 10 companies"
)
async def collect_all_documents(
    filing_types: List[FilingType] = Query(
        default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A]
    ),
    years_back: int = Query(default=3, ge=1, le=10)
):
    """Collect documents for all 10 target companies"""
    logger.info("📥 Batch collection for all companies")
    try:
        service = get_document_collector_service()
        return service.collect_for_all_companies([ft.value for ft in filing_types], years_back)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# SECTION 2: DOCUMENT PARSING


@router.post(
    "/parse/{ticker}",
    response_model=ParseByTickerResponse,
    tags=["2. Parsing"],
    summary="Parse all documents for a company",
    description="""
    Parse all collected SEC filings for a company.
    
    **Process:**
    1. Downloads raw documents from S3
    2. Extracts text and tables (HTML/PDF)
    3. Identifies key sections (Risk Factors, MD&A, etc.)
    4. Uploads parsed JSON to S3 (sec/parsed/{ticker}/...)
    5. Updates word_count in Snowflake
    """
)
async def parse_documents_by_ticker(ticker: str):
    """Parse all documents for a company"""
    logger.info(f"📄 Parse request for: {ticker}")
    try:
        service = get_document_parsing_service()
        return service.parse_by_ticker(ticker)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/parse",
    response_model=ParseAllResponse,
    tags=["2. Parsing"],
    summary="Parse documents for all companies"
)
async def parse_all_documents():
    """Parse documents for all 10 target companies"""
    logger.info("📄 Batch parsing for all companies")
    try:
        service = get_document_parsing_service()
        return service.parse_all_companies()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/parsed/{document_id}",
    tags=["2. Parsing"],
    summary="View parsed document content",
    description="Get the parsed content of a document from S3"
)
async def get_parsed_document(document_id: str):
    """Get parsed document content by ID"""
    logger.info(f"📄 Getting parsed document: {document_id}")
    
    repo = get_document_repository()
    doc = repo.get_by_id(document_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    ticker = doc['ticker']
    filing_type = doc['filing_type']
    filing_date = str(doc['filing_date'])
    
    # Get parsed content from S3
    clean_filing_type = filing_type.replace(" ", "")
    s3_key = f"sec/parsed/{ticker}/{clean_filing_type}/{filing_date}_full.json"
    
    s3_service = get_s3_service()
    content = s3_service.get_file(s3_key)
    
    if not content:
        raise HTTPException(status_code=404, detail=f"Parsed content not found. Document may not be parsed yet. S3 key: {s3_key}")
    
    parsed_data = json.loads(content.decode('utf-8'))
    
    return {
        "document_id": document_id,
        "ticker": ticker,
        "filing_type": filing_type,
        "filing_date": filing_date,
        "s3_key": s3_key,
        "word_count": parsed_data.get('word_count', 0),
        "table_count": parsed_data.get('table_count', 0),
        "sections": list(parsed_data.get('sections', {}).keys()),
        "text_preview": parsed_data.get('text_content', '')[:2000] + "..." if parsed_data.get('text_content') else "",
        "tables": parsed_data.get('tables', [])[:5]  # First 5 tables
    }



# SECTION 3: DOCUMENT CHUNKING


@router.post(
    "/chunk/{ticker}",
    tags=["3. Chunking"],
    summary="Chunk all parsed documents for a company",
    description="""
    Split parsed documents into smaller chunks for LLM processing.
    
    **Process:**
    1. Downloads parsed content from S3
    2. Splits into overlapping chunks (preserves context)
    3. Uploads chunks to S3 (sec/chunks/{ticker}/...)
    4. Saves chunk metadata to Snowflake
    
    **Parameters:**
    - chunk_size: Target words per chunk (default: 750)
    - chunk_overlap: Overlap between chunks (default: 50)
    """
)
async def chunk_documents_by_ticker(
    ticker: str,
    chunk_size: int = Query(default=750, ge=100, le=2000, description="Words per chunk"),
    chunk_overlap: int = Query(default=50, ge=0, le=200, description="Overlap between chunks")
):
    """Chunk all parsed documents for a company"""
    logger.info(f"📦 Chunk request for: {ticker}")
    try:
        service = get_document_chunking_service()
        return service.chunk_by_ticker(ticker, chunk_size, chunk_overlap)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/chunk",
    tags=["3. Chunking"],
    summary="Chunk documents for all companies"
)
async def chunk_all_documents(
    chunk_size: int = Query(default=750, ge=100, le=2000),
    chunk_overlap: int = Query(default=50, ge=0, le=200)
):
    """Chunk documents for all 10 target companies"""
    logger.info("📦 Batch chunking for all companies")
    try:
        service = get_document_chunking_service()
        return service.chunk_all_companies(chunk_size, chunk_overlap)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/chunks/{document_id}",
    tags=["3. Chunking"],
    summary="Get chunks for a document"
)
async def get_document_chunks(document_id: str):
    """Get all chunks for a specific document"""
    chunk_repo = get_chunk_repository()
    chunks = chunk_repo.get_by_document_id(document_id)
    
    if not chunks:
        raise HTTPException(status_code=404, detail="No chunks found for this document")
    
    return {
        "document_id": document_id,
        "chunk_count": len(chunks),
        "chunks": chunks
    }


@router.get(
    "/chunk/stats/{ticker}",
    tags=["3. Chunking"],
    summary="Get chunk statistics for a company"
)
async def get_chunk_stats(ticker: str):
    """Get chunk statistics for a company"""
    chunk_repo = get_chunk_repository()
    stats = chunk_repo.get_stats_by_ticker(ticker.upper())
    total = chunk_repo.count_by_ticker(ticker.upper())
    
    return {
        "ticker": ticker.upper(),
        "total_chunks": total,
        "by_filing_type": stats
    }



# SECTION 4: REPORTS & STATISTICS


@router.get(
    "/report",
    tags=["4. Reports"],
    summary="Get Evidence Collection Report",
    description="Get comprehensive statistics in JSON format"
)
async def get_evidence_report():
    """Generate evidence collection report"""
    logger.info("📊 Generating report...")
    
    repo = get_document_repository()
    chunk_repo = get_chunk_repository()
    signal_repo = get_signal_repository()
    
    summary = repo.get_summary_statistics()
    status_breakdown = repo.get_status_breakdown()
    company_stats = repo.get_all_company_stats()
    
    # Get chunk counts
    total_chunks = chunk_repo.get_total_chunks()
    summary["total_chunks"] = total_chunks
    
    # Get total signals
    total_signals = signal_repo.get_total_signal_count()

    return {
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "companies_processed": summary["companies_processed"],
            "total_documents": summary["total_documents"],
            "total_chunks": total_chunks,
            "total_words": summary["total_words"],
            "total signals": total_signals
        },
        "status_breakdown": status_breakdown,
        "documents_by_company": company_stats
    }


@router.get(
    "/report/table",
    tags=["4. Reports"],
    summary="Get Evidence Collection Report (Table Format)",
    description="Get report formatted as tables for easy viewing"
)
async def get_evidence_report_table():
    """Generate report in table format"""
    logger.info("📊 Generating table report...")
    
    repo = get_document_repository()
    chunk_repo = get_chunk_repository()
    
    summary = repo.get_summary_statistics()
    status_breakdown = repo.get_status_breakdown()
    company_stats = repo.get_all_company_stats()
    total_chunks = chunk_repo.get_total_chunks()
    
    # Build summary table
    summary_table = {
        "headers": ["Metric", "Value"],
        "rows": [
            ["Companies Processed", summary["companies_processed"]],
            ["Total Documents", summary["total_documents"]],
            ["Total Chunks", total_chunks],
            ["Total Words", f"{summary['total_words']:,}"]
        ]
    }
    
    # Build status table
    status_table = {
        "headers": ["Status", "Count"],
        "rows": [[status, count] for status, count in sorted(status_breakdown.items())]
    }
    
    # Build company table
    company_table = {
        "headers": ["Ticker", "10-K", "10-Q", "8-K", "DEF 14A", "Total", "Chunks", "Words"],
        "rows": []
    }
    
    for cs in company_stats:
        company_table["rows"].append([
            cs["ticker"],
            cs["form_10k"],
            cs["form_10q"],
            cs["form_8k"],
            cs["def_14a"],
            cs["total"],
            cs["chunks"],
            f"{cs['word_count']:,}"
        ])
    
    # Add totals row
    totals = [
        "TOTAL",
        sum(cs["form_10k"] for cs in company_stats),
        sum(cs["form_10q"] for cs in company_stats),
        sum(cs["form_8k"] for cs in company_stats),
        sum(cs["def_14a"] for cs in company_stats),
        sum(cs["total"] for cs in company_stats),
        sum(cs["chunks"] for cs in company_stats),
        f"{sum(cs['word_count'] for cs in company_stats):,}"
    ]
    company_table["rows"].append(totals)
    
    return {
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "summary_table": summary_table,
        "status_table": status_table,
        "company_table": company_table
    }


# SECTION 4B: SECTION ANALYSIS


@router.get(
    "/analysis/export",
    tags=["4. Reports"],
    summary="Export section analysis as Markdown",
    description="Download sec_analysis.md file with all tables"
)
async def export_section_analysis():
    """Export section analysis as markdown file"""
    from fastapi.responses import PlainTextResponse
    logger.info("📊 Exporting analysis as markdown...")
    try:
        service = get_section_analysis_service()
        markdown = service.generate_markdown_report()
        return PlainTextResponse(
            content=markdown,
            media_type="text/markdown",
            headers={"Content-Disposition": "attachment; filename=sec_analysis.md"}
        )
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/analysis",
    tags=["4. Reports"],
    summary="Analyze sections for all companies",
    description="Get section analysis tables for all 10 companies (JSON)"
)
async def analyze_all_sections():
    """Analyze sections for all companies - returns table format"""
    logger.info("📊 Analysis request for all companies")
    try:
        service = get_section_analysis_service()
        return service.generate_analysis_tables()
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/analysis/{ticker}",
    tags=["4. Reports"],
    summary="Analyze sections for a company",
    description="Get section word counts and keyword mentions for a single company"
)
async def analyze_company_sections(ticker: str):
    """Analyze sections for a single company"""
    logger.info(f"📊 Analysis request for: {ticker}")
    try:
        service = get_section_analysis_service()
        return service.analyze_by_ticker(ticker)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# SECTION 5: DOCUMENT MANAGEMENT


@router.get(
    "",
    tags=["5. Management"],
    summary="List all documents"
)
async def list_documents(
    ticker: Optional[str] = Query(None),
    filing_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List documents with optional filters"""
    repo = get_document_repository()
    
    if ticker:
        docs = repo.get_by_ticker(ticker.upper())
    else:
        docs = repo.get_all(limit=limit, offset=offset)
    
    if filing_type:
        docs = [d for d in docs if d['filing_type'] == filing_type]
    if status:
        docs = [d for d in docs if d.get('status') == status]
    
    return {"count": len(docs), "documents": docs}


@router.get(
    "/stats/{ticker}",
    tags=["5. Management"],
    summary="Get document statistics for a company"
)
async def get_document_stats(ticker: str):
    """Get document statistics for a company"""
    repo = get_document_repository()
    return repo.get_company_stats(ticker.upper())


@router.get(
    "/{document_id}",
    tags=["5. Management"],
    summary="Get document by ID"
)
async def get_document(document_id: str):
    """Get document metadata by ID"""
    repo = get_document_repository()
    doc = repo.get_by_id(document_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return doc



# SECTION 6: DELETE / RESET (For Demo/Testing)


@router.delete(
    "/reset/{ticker}",
    tags=["6. Reset (Demo)"],
    summary="Delete all data for a company",
    description="""
    **⚠️ FOR DEMO/TESTING ONLY**
    
    Deletes ALL data for a company:
    - S3: raw/, parsed/, chunks/ folders
    - Snowflake: documents and document_chunks records
    
    Use this to demonstrate the full pipeline from scratch.
    """
)
async def reset_company_data(ticker: str):
    """Delete all data for a company (raw, parsed, chunks)"""
    ticker = ticker.upper()
    logger.info(f"🗑️ RESETTING ALL DATA FOR: {ticker}")
    
    doc_repo = get_document_repository()
    chunk_repo = get_chunk_repository()
    s3_service = get_s3_service()
    
    results = {
        "ticker": ticker,
        "s3_deleted": {"raw": 0, "parsed": 0, "chunks": 0},
        "snowflake_deleted": {"chunks": 0, "documents": 0}
    }
    
    # 1. Delete chunks from Snowflake
    logger.info(f"  💾 Deleting chunks from Snowflake...")
    chunks_deleted = chunk_repo.delete_by_ticker(ticker)
    results["snowflake_deleted"]["chunks"] = chunks_deleted
    logger.info(f"  ✅ Deleted {chunks_deleted} chunk records")
    
    # 2. Delete documents from Snowflake
    logger.info(f"  💾 Deleting documents from Snowflake...")
    docs_deleted = doc_repo.delete_by_ticker(ticker)
    results["snowflake_deleted"]["documents"] = docs_deleted
    logger.info(f"  ✅ Deleted {docs_deleted} document records")
    
    # 3. Delete from S3 (raw, parsed, chunks folders)
    for folder in ["raw", "parsed", "chunks"]:
        prefix = f"sec/{folder}/{ticker}/"
        logger.info(f"  🪣 Deleting S3 folder: {prefix}")
        
        try:
            # List all objects with this prefix
            response = s3_service.s3_client.list_objects_v2(
                Bucket=s3_service.bucket_name,
                Prefix=prefix
            )
            
            objects = response.get('Contents', [])
            if objects:
                # Delete all objects
                delete_keys = [{'Key': obj['Key']} for obj in objects]
                s3_service.s3_client.delete_objects(
                    Bucket=s3_service.bucket_name,
                    Delete={'Objects': delete_keys}
                )
                results["s3_deleted"][folder] = len(delete_keys)
                logger.info(f"  ✅ Deleted {len(delete_keys)} files from {folder}/")
            else:
                logger.info(f"  ℹ️ No files found in {folder}/")
        except Exception as e:
            logger.error(f"  ❌ Error deleting {folder}/: {e}")
    
    logger.info(f"🗑️ RESET COMPLETE FOR: {ticker}")
    return results


@router.delete(
    "/reset/{ticker}/raw",
    tags=["6. Reset (Demo)"],
    summary="Delete only raw files for a company"
)
async def reset_raw_only(ticker: str):
    """Delete only raw files (keeps parsed and chunks)"""
    ticker = ticker.upper()
    logger.info(f"🗑️ Deleting RAW files for: {ticker}")
    
    s3_service = get_s3_service()
    prefix = f"sec/raw/{ticker}/"
    
    try:
        response = s3_service.s3_client.list_objects_v2(
            Bucket=s3_service.bucket_name,
            Prefix=prefix
        )
        
        objects = response.get('Contents', [])
        deleted = 0
        
        if objects:
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3_service.s3_client.delete_objects(
                Bucket=s3_service.bucket_name,
                Delete={'Objects': delete_keys}
            )
            deleted = len(delete_keys)
        
        return {"ticker": ticker, "folder": "raw", "files_deleted": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/reset/{ticker}/parsed",
    tags=["6. Reset (Demo)"],
    summary="Delete parsed files and reset status"
)
async def reset_parsed_only(ticker: str):
    """Delete parsed files and reset document status to 'uploaded'"""
    ticker = ticker.upper()
    logger.info(f"🗑️ Deleting PARSED files for: {ticker}")
    
    s3_service = get_s3_service()
    doc_repo = get_document_repository()
    
    # Delete from S3
    prefix = f"sec/parsed/{ticker}/"
    try:
        response = s3_service.s3_client.list_objects_v2(
            Bucket=s3_service.bucket_name,
            Prefix=prefix
        )
        
        objects = response.get('Contents', [])
        deleted = 0
        
        if objects:
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3_service.s3_client.delete_objects(
                Bucket=s3_service.bucket_name,
                Delete={'Objects': delete_keys}
            )
            deleted = len(delete_keys)
        
        # Reset status in Snowflake
        doc_repo.reset_status_by_ticker(ticker, from_status='parsed', to_status='uploaded')
        
        return {"ticker": ticker, "folder": "parsed", "files_deleted": deleted, "status_reset": "uploaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/reset/{ticker}/chunks",
    tags=["6. Reset (Demo)"],
    summary="Delete chunks and reset status"
)
async def reset_chunks_only(ticker: str):
    """Delete chunks and reset document status to 'parsed'"""
    ticker = ticker.upper()
    logger.info(f"🗑️ Deleting CHUNKS for: {ticker}")
    
    s3_service = get_s3_service()
    doc_repo = get_document_repository()
    chunk_repo = get_chunk_repository()
    
    # Delete chunks from Snowflake
    chunks_deleted = chunk_repo.delete_by_ticker(ticker)
    
    # Delete from S3
    prefix = f"sec/chunks/{ticker}/"
    try:
        response = s3_service.s3_client.list_objects_v2(
            Bucket=s3_service.bucket_name,
            Prefix=prefix
        )
        
        objects = response.get('Contents', [])
        s3_deleted = 0
        
        if objects:
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3_service.s3_client.delete_objects(
                Bucket=s3_service.bucket_name,
                Delete={'Objects': delete_keys}
            )
            s3_deleted = len(delete_keys)
        
        # Reset status and chunk_count in Snowflake
        doc_repo.reset_status_by_ticker(ticker, from_status='chunked', to_status='parsed')
        doc_repo.reset_chunk_count_by_ticker(ticker)
        
        return {
            "ticker": ticker, 
            "folder": "chunks", 
            "s3_files_deleted": s3_deleted,
            "snowflake_chunks_deleted": chunks_deleted,
            "status_reset": "parsed"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))