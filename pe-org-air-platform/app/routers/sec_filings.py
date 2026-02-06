from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Any

from app.pipelines.runner import (
    step_download_filings,
    step_parse_documents,
    step_deduplicate,
    step_chunk_documents,
    step_extract_items,
    get_pipeline_stats,
    preview_filings,
    step_download_all_companies,
)
from app.pipelines.pipeline_state import PipelineStateManager

router = APIRouter(prefix="/api/v1/sec", tags=["SEC Filings Pipeline"])



# REQUEST MODELS


class DownloadRequest(BaseModel):
    company_id: str = Field(..., description="Company UUID from Snowflake", min_length=1)
    ticker: str = Field(..., description="Stock ticker (e.g., GS)", min_length=1, max_length=10)
    filing_types: List[str] = Field(default=["10-K", "10-Q", "8-K", "DEF 14A"], description="Filing types to download. Options: 10-K, 10-Q, 8-K, DEF 14A")
    from_date: str = Field(..., description="Start date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$")
    to_date: str = Field(..., description="End date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$")
    rate_limit: float = Field(default=0.1, ge=0.1, le=5.0, description="Delay between requests (0.1-5.0s)")
    limit_per_filing_type: int = Field(default=5, ge=1, le=100, description="Max filings per filing type (1-100)")

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "summary": "All 4 filing types",
                    "value": {
                        "company_id": "comp-gs-010",
                        "ticker": "GS",
                        "filing_types": ["10-K", "10-Q", "8-K", "DEF 14A"],
                        "from_date": "2022-01-01",
                        "to_date": "2024-01-01",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 3
                    }
                },
                {
                    "summary": "Only 10-K filings",
                    "value": {
                        "company_id": "comp-gs-010",
                        "ticker": "GS",
                        "filing_types": ["10-K"],
                        "from_date": "2022-01-01",
                        "to_date": "2024-01-01",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 5
                    }
                },
                {
                    "summary": "10-K and 10-Q only",
                    "value": {
                        "company_id": "comp-gs-010",
                        "ticker": "GS",
                        "filing_types": ["10-K", "10-Q"],
                        "from_date": "2022-01-01",
                        "to_date": "2024-01-01",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 5
                    }
                }
            ]
        }


class DownloadAllRequest(BaseModel):
    from_date: str = Field(default="2023-01-01", description="Start date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$")
    to_date: str = Field(default="2024-12-31", description="End date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$")
    rate_limit: float = Field(default=0.2, ge=0.1, le=5.0, description="Delay between requests (0.1-5.0s)")
    limit_per_filing_type: int = Field(default=5, ge=1, le=100, description="Max filings per company per filing type (1-100)")
    filing_types: List[str] = Field(default=["10-K", "10-Q", "8-K", "DEF 14A"], description="Filing types to download. Options: 10-K, 10-Q, 8-K, DEF 14A")

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "summary": "All 4 filing types (default)",
                    "value": {
                        "from_date": "2023-01-01",
                        "to_date": "2024-12-31",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 3,
                        "filing_types": ["10-K", "10-Q", "8-K", "DEF 14A"]
                    }
                },
                {
                    "summary": "Only 10-K filings",
                    "value": {
                        "from_date": "2023-01-01",
                        "to_date": "2024-12-31",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 5,
                        "filing_types": ["10-K"]
                    }
                },
                {
                    "summary": "10-K and 8-K only",
                    "value": {
                        "from_date": "2023-01-01",
                        "to_date": "2024-12-31",
                        "rate_limit": 0.2,
                        "limit_per_filing_type": 5,
                        "filing_types": ["10-K", "8-K"]
                    }
                }
            ]
        }


class ChunkRequest(BaseModel):
    chunk_size: int = Field(default=1000, ge=100, le=5000, description="Words per chunk (100-5000)")
    chunk_overlap: int = Field(default=100, ge=0, le=500, description="Overlap words (0-500)")

    class Config:
        json_schema_extra = {"example": {"chunk_size": 1000, "chunk_overlap": 100}}


class PipelineResponse(BaseModel):
    status: str = Field(description="Status: success or error")
    message: str = Field(description="Human-readable message")
    data: Optional[Any] = Field(default=None, description="Response data")



# ENDPOINTS


@router.post("/download-all", response_model=PipelineResponse)
async def download_all_companies(request: DownloadAllRequest):
    """
    **BULK DOWNLOAD: All Companies, All Filing Types**
    
    Complete pipeline for ALL companies in Snowflake:
    1. Downloads filings from SEC EDGAR
    2. Parses documents and extracts tables
    3. Creates chunks
    4. Uploads to S3 (raw, parsed, tables, chunks)
    5. Inserts into Snowflake (documents + document_chunks)
    
    **Auto-fetches:**
    - All tickers from Snowflake `companies` table
    
    **Parameters:**
    - `from_date`: Start date (e.g., "2023-01-01")
    - `to_date`: End date (e.g., "2024-12-31")
    - `rate_limit`: Delay between requests (default: 0.2s)
    - `limit_per_filing_type`: Max filings per company per filing type (default: 5)
    - `filing_types`: List of filing types to download (default: all 4 types)
    
    **⚠️ Note:** This can take a LONG time!
    """
    try:
        result = step_download_all_companies(
            from_date=request.from_date,
            to_date=request.to_date,
            rate_limit=request.rate_limit,
            limit_per_filing_type=request.limit_per_filing_type,
            filing_types=request.filing_types,
        )
        return PipelineResponse(
            status="success",
            message=f"Processed {result['results']['companies_processed']} companies: {result['results']['snowflake_documents_inserted']} docs, {result['results']['snowflake_chunks_inserted']} chunks in Snowflake",
            data=result
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview", response_model=PipelineResponse)
async def preview_sec_filings(ticker: str, filing_type: str = "8-K", limit: int = 10):
    """**PREVIEW:** Check for PDF/Excel exhibits before downloading."""
    try:
        result = preview_filings(ticker=ticker.upper(), filing_type=filing_type, limit=limit)
        if result.get("status") == "error":
            return PipelineResponse(status="error", message=result.get("message", "Preview failed"), data=None)
        return PipelineResponse(status="success", message=f"Found {result['filings_with_pdf']} with PDFs, {result['filings_with_excel']} with Excel", data=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download", response_model=PipelineResponse)
async def download_filings(req: DownloadRequest):
    """
    **STEP 1:** Download SEC Filings (Single Company).
    
    **Parameters:**
    - `company_id`: Company UUID from Snowflake
    - `ticker`: Stock ticker (e.g., GS, AAPL)
    - `filing_types`: List of filing types (10-K, 10-Q, 8-K, DEF 14A)
    - `from_date`: Start date (YYYY-MM-DD)
    - `to_date`: End date (YYYY-MM-DD)
    - `rate_limit`: Delay between requests (default: 0.1s)
    - `limit_per_filing_type`: Max filings per filing type (default: 5)
    """
    if not req.ticker or not req.from_date or not req.to_date or not req.filing_types or not req.company_id:
        raise HTTPException(status_code=400, detail="All fields required")
    
    valid_types = {"10-K", "10-Q", "8-K", "DEF 14A"}
    for ft in req.filing_types:
        if ft not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid filing_type: {ft}")
    
    try:
        result = step_download_filings(
            company_id=req.company_id,
            ticker=req.ticker.upper(),
            filing_types=req.filing_types,
            from_date=req.from_date,
            to_date=req.to_date,
            rate_limit=req.rate_limit,
            limit_per_filing_type=req.limit_per_filing_type
        )
        return PipelineResponse(status="success", message=f"Downloaded {result['downloaded']} filings for {req.ticker.upper()}", data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/parse", response_model=PipelineResponse)
async def parse_documents():
    """**STEP 2:** Parse Downloaded Documents."""
    try:
        result = step_parse_documents()
        return PipelineResponse(status="success", message=f"Parsed {result['parsed']} documents", data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deduplicate", response_model=PipelineResponse)
async def deduplicate_documents():
    """**STEP 3:** Deduplicate Documents."""
    try:
        result = step_deduplicate()
        return PipelineResponse(status="success", message=f"Found {result['unique_filings']} unique ({result['duplicates_skipped']} duplicates)", data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chunk", response_model=PipelineResponse)
async def chunk_documents(req: ChunkRequest):
    """**STEP 4:** Chunk Documents → S3 + Snowflake."""
    try:
        result = step_chunk_documents(chunk_size=req.chunk_size, chunk_overlap=req.chunk_overlap)
        return PipelineResponse(status="success", message=f"Created {result['total_chunks']} chunks from {result['documents_chunked']} docs", data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/extract-items", response_model=PipelineResponse)
async def extract_items():
    """**STEP 5:** Extract Items 1, 1A, 7 from 10-K filings."""
    try:
        result = step_extract_items()
        return PipelineResponse(status="success", message=f"Extracted items from {result['items_extracted']} docs", data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=PipelineResponse)
async def pipeline_stats():
    """**STEP 6:** Get Pipeline Statistics."""
    try:
        result = get_pipeline_stats()
        return PipelineResponse(status="success", message="Stats retrieved", data=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset", response_model=PipelineResponse)
async def reset_pipeline():
    """**Reset Pipeline State** (does not delete files)."""
    try:
        PipelineStateManager.reset_state()
        return PipelineResponse(status="success", message="Pipeline reset", data={"steps_completed": {"download": False, "parse": False, "deduplicate": False, "chunk": False, "extract_items": False}})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))