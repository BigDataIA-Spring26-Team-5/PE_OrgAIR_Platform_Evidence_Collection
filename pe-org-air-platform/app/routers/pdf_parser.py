"""
FastAPI Router for PDF Parser Endpoint

- Parses 10-K PDF from data/sample_10k/
- Saves to local data/ folder
- Uploads to S3 (raw, parsed, tables, chunks)
- NO Snowflake (sample file only)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.pipelines.pdf_parser import PDFParser
# from app.services.s3_storage import S3Storage
from app.services.s3_storage import S3StorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sec", tags=["Sample PDF Parsing"])


@router.get("/parse-pdf")
async def parse_sample_pdf(
    ticker: str = Query(default="SAMPLE", description="Company ticker symbol"),
    upload_to_s3: bool = Query(default=True, description="Upload to S3"),
):
    """
    Parse 10-K PDF from data/sample_10k/ folder.

    **Workflow:**
    1. Auto-detect PDF in data/sample_10k/
    2. Parse text + extract tables
    3. Save to local data/ folder
    4. Upload to S3 (if enabled)

    **Local Storage:**
    - `data/parsed/{ticker}/{hash}.json`
    - `data/parsed/{ticker}/{hash}_content.txt`
    - `data/tables/{ticker}_{hash}_tables.json`

    **S3 Storage:**
    - `raw/{ticker}/10-K/{filename}.pdf`
    - `parsed/{ticker}/{hash}.json`
    - `parsed/{ticker}/{hash}_content.txt`
    - `tables/{ticker}/{hash}_tables.json`
    """

    # Find PDF
    sample_dir = Path("data/sample_10k")
    if not sample_dir.exists():
        raise HTTPException(status_code=404, detail=f"Directory not found: {sample_dir}")

    pdfs = list(sample_dir.glob("*.pdf"))
    if not pdfs:
        raise HTTPException(status_code=404, detail=f"No PDF files in {sample_dir}")

    pdf_path = pdfs[0]
    logger.info(f"Found PDF: {pdf_path}")

    try:
        # Parse PDF
        parser = PDFParser(output_dir="data/parsed")
        doc = parser.parse_pdf(str(pdf_path), ticker=ticker)
        
        # Save locally
        local_paths = parser.save_output(doc)

        # Prepare data for S3
        doc_metadata = {
            "ticker": doc.ticker,
            "filing_type": doc.filing_type,
            "filing_date": doc.filing_date,
            "content_hash": doc.content_hash,
            "word_count": doc.word_count,
            "page_count": doc.page_count,
            "table_count": doc.table_count,
            "source_path": doc.source_path,
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }

        tables_data = {
            "ticker": doc.ticker,
            "filing_type": doc.filing_type,
            "content_hash": doc.content_hash,
            "table_count": doc.table_count,
            "tables": doc.tables,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        } if doc.tables else None

        # Upload to S3
        s3_keys = {}
        if upload_to_s3:
            try:
                s3 = S3Storage()
                s3_keys = s3.upload_all_outputs(
                    pdf_path=pdf_path,
                    ticker=ticker,
                    filing_type="10-K",
                    doc_hash=doc.content_hash,
                    doc_metadata=doc_metadata,
                    full_content=doc.content,
                    tables_data=tables_data,
                    chunks_data=None,  # No chunks for simple parse
                )
                logger.info(f"S3 upload complete: {len(s3_keys)} files")
            except Exception as e:
                logger.error(f"S3 upload failed: {e}")
                s3_keys["error"] = str(e)

        # Build response
        tables_summary = [
            {
                "table_index": t["table_index"],
                "page": t["page"],
                "row_count": t["row_count"],
                "col_count": t["col_count"],
                "headers": t["headers"][:5]
            }
            for t in doc.tables[:10]
        ]

        return {
            "status": "success",
            "message": f"Parsed {doc.page_count} pages, {doc.table_count} tables",
            "pdf_file": str(pdf_path),
            "ticker": doc.ticker,
            "filing_type": doc.filing_type,
            "filing_date": doc.filing_date,
            "page_count": doc.page_count,
            "word_count": doc.word_count,
            "table_count": doc.table_count,
            "content_hash": doc.content_hash,
            "content_preview": doc.content[:500] + "..." if len(doc.content) > 500 else doc.content,
            "tables_summary": tables_summary,
            "local_files": local_paths,
            "s3_uploads": s3_keys if upload_to_s3 else "disabled",
        }

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))