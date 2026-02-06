from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser, ParsedDocument
from app.pipelines.chunking import SemanticChunker
from app.pipelines.registry import DocumentRegistry
from app.pipelines.pipeline_state import PipelineStateManager
from app.services.snowflake import SnowflakeService
from app.services.s3_storage import S3Storage

logger = logging.getLogger(__name__)


def _generate_uuid() -> str:
    return str(uuid.uuid4())


def _get_s3() -> S3Storage:
    return S3Storage()



# STEP 1: DOWNLOAD FILINGS → Local + S3 (raw/)

def step_download_filings(
    company_id: str,
    ticker: str,
    filing_types: List[str],
    from_date: str,
    to_date: str,
    rate_limit: float = 0.1,
    limit_per_filing_type: int = 5,
) -> Dict[str, Any]:
    
    state = PipelineStateManager.get_state()
    state.reset()
    
    company_name = os.getenv("SEC_COMPANY_NAME", "PE-OrgAIR-Platform")
    email = os.getenv("SEC_EMAIL")
    if not email:
        raise ValueError("SEC_EMAIL not set in .env")
    
    db = SnowflakeService()
    try:
        companies = db.list_companies()
        valid_ids = {c["ID"] for c in companies}
        if company_id not in valid_ids:
            raise ValueError(f"company_id '{company_id}' not found in companies table")
    finally:
        db.close()
    
    state.company_id = company_id
    state.ticker = ticker.upper()
    state.filing_types = filing_types
    state.from_date = from_date
    state.to_date = to_date
    state.rate_limit = rate_limit
    
    download_dir = Path("data/raw/sec")
    sec = SECEdgarPipeline(company_name=company_name, email=email, download_dir=download_dir)
    
    s3 = _get_s3()
    s3_uploads = []
    
    for filing_type in filing_types:
        try:
            time.sleep(rate_limit)
            sec.dl.get(filing_type, ticker.upper(), limit=limit_per_filing_type, after=from_date, before=to_date)
            
            filing_dir = download_dir / "sec-edgar-filings" / ticker.upper() / filing_type
            if filing_dir.exists():
                for fp in filing_dir.glob("**/full-submission.txt"):
                    sec._download_exhibits(fp, ticker.upper())
                    
                    exhibits_dir = fp.parent / "exhibits"
                    pdf_count = len(list(exhibits_dir.glob("*.pdf"))) if exhibits_dir.exists() else 0
                    
                    try:
                        s3_key = s3.upload_raw_file(fp, ticker.upper(), filing_type, f"{fp.parent.name}_{fp.name}")
                        s3_uploads.append(s3_key)
                        
                        if exhibits_dir.exists():
                            for pdf_file in exhibits_dir.glob("*.pdf"):
                                s3_key = s3.upload_raw_file(pdf_file, ticker.upper(), filing_type, pdf_file.name)
                                s3_uploads.append(s3_key)
                            
                            for xls_file in exhibits_dir.glob("*.xls*"):
                                s3_key = s3.upload_raw_file(xls_file, ticker.upper(), filing_type, xls_file.name)
                                s3_uploads.append(s3_key)
                                
                    except Exception as e:
                        logger.error(f"S3 upload failed for {fp}: {e}")
                    
                    state.downloaded_filings.append({
                        "path": str(fp),
                        "ticker": ticker.upper(),
                        "filing_type": filing_type,
                        "accession": fp.parent.name,
                        "pdf_exhibits": pdf_count,
                        "s3_uploads": len(s3_uploads),
                        "downloaded_at": datetime.now(timezone.utc).isoformat()
                    })
        except Exception as e:
            state.stats["errors"] += 1
            state.stats["error_details"].append({"step": "download", "filing_type": filing_type, "error": str(e)})
    
    state.stats["downloaded"] = len(state.downloaded_filings)
    state.stats["s3_uploads"] = len(s3_uploads)
    state.mark_step_complete("download")
    PipelineStateManager.save_state()
    
    return {"status": "success", "downloaded": state.stats["downloaded"], "s3_uploads": len(s3_uploads), "s3_files": s3_uploads, "filings": state.downloaded_filings, "errors": state.stats["errors"]}



# STEP 2: PARSE DOCUMENTS → Local + S3 (parsed/, tables/)

def step_parse_documents() -> Dict[str, Any]:
    
    state = PipelineStateManager.get_state()
    
    if not state.is_step_complete("download"):
        raise ValueError("Download step not complete. Run /sec/download first.")
    
    parser = DocumentParser()
    parsed_dir = Path("data/parsed")
    tables_dir = Path("data/tables")
    parsed_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    
    s3 = _get_s3()
    s3_uploads = []
    
    state.parsed_filings = []
    total_tables = 0
    total_pdf_tables = 0
    
    for filing in state.downloaded_filings:
        try:
            fp = Path(filing["path"])
            if not fp.exists():
                continue
            
            doc = parser.parse_filing(fp, filing["ticker"])
            html_tables = doc.tables.copy()
            
            pdf_tables = []
            exhibits_dir = fp.parent / "exhibits"
            parsed_pdfs = []
            
            if exhibits_dir.exists():
                for pdf_file in exhibits_dir.glob("*.pdf"):
                    try:
                        pdf_doc = parser.parse_filing(pdf_file, filing["ticker"])
                        for table in pdf_doc.tables:
                            table["table_index"] = len(html_tables) + len(pdf_tables)
                            table["source"] = f"PDF: {pdf_file.name}"
                            pdf_tables.append(table)
                        parsed_pdfs.append({"filename": pdf_file.name, "tables_extracted": len(pdf_doc.tables), "word_count": pdf_doc.word_count})
                    except Exception as e:
                        logger.warning(f"Failed to parse PDF {pdf_file.name}: {e}")
            
            all_tables = html_tables + pdf_tables
            total_pdf_tables += len(pdf_tables)
            
            out_dir = parsed_dir / doc.company_ticker
            tables_out_dir = tables_dir / doc.company_ticker
            out_dir.mkdir(parents=True, exist_ok=True)
            tables_out_dir.mkdir(parents=True, exist_ok=True)
            
            parsed_data = {
                "document_id": doc.content_hash, "ticker": doc.company_ticker, "filing_type": doc.filing_type,
                "filing_date": doc.filing_date.isoformat(), "content_hash": doc.content_hash, "word_count": doc.word_count,
                "table_count": len(all_tables), "html_table_count": len(html_tables), "pdf_table_count": len(pdf_tables),
                "pdf_exhibits_parsed": parsed_pdfs, "sections": doc.sections, "source_path": doc.source_path,
                "accession": filing["accession"], "parsed_at": datetime.now(timezone.utc).isoformat()
            }
            
            out_path = out_dir / f"{doc.content_hash}.json"
            out_path.write_text(json.dumps(parsed_data, indent=2, ensure_ascii=False), encoding="utf-8")
            
            content_path = out_dir / f"{doc.content_hash}_content.txt"
            content_path.write_text(doc.content, encoding="utf-8")
            
            try:
                s3_key = s3.upload_parsed_json(parsed_data, doc.company_ticker, doc.content_hash)
                s3_uploads.append(s3_key)
                s3_key = s3.upload_parsed_content(doc.content, doc.company_ticker, doc.content_hash)
                s3_uploads.append(s3_key)
            except Exception as e:
                logger.error(f"S3 upload failed for parsed doc {doc.content_hash}: {e}")
            
            if all_tables:
                tables_data = {
                    "document_id": doc.content_hash, "ticker": doc.company_ticker, "filing_type": doc.filing_type,
                    "filing_date": doc.filing_date.isoformat(), "accession": filing["accession"],
                    "table_count": len(all_tables), "html_table_count": len(html_tables), "pdf_table_count": len(pdf_tables),
                    "tables": all_tables, "extracted_at": datetime.now(timezone.utc).isoformat()
                }
                tables_path = tables_out_dir / f"{doc.content_hash}_tables.json"
                tables_path.write_text(json.dumps(tables_data, indent=2, ensure_ascii=False), encoding="utf-8")
                total_tables += len(all_tables)
                
                try:
                    s3_key = s3.upload_tables_json(tables_data, doc.company_ticker, doc.content_hash)
                    s3_uploads.append(s3_key)
                except Exception as e:
                    logger.error(f"S3 upload failed for tables {doc.content_hash}: {e}")
            
            state.parsed_filings.append({**parsed_data, "parsed_path": str(out_path), "tables_path": str(tables_out_dir / f"{doc.content_hash}_tables.json") if all_tables else None, "content": doc.content})
            state.stats["parsed"] += 1
            
        except Exception as e:
            state.stats["errors"] += 1
            state.stats["error_details"].append({"step": "parse", "file": filing["path"], "error": str(e)})
    
    state.mark_step_complete("parse")
    PipelineStateManager.save_state()
    
    return {"status": "success", "parsed": state.stats["parsed"], "total_tables_extracted": total_tables, "html_tables": total_tables - total_pdf_tables, "pdf_tables": total_pdf_tables, "s3_uploads": len(s3_uploads), "errors": state.stats["errors"]}



# STEP 3: DEDUPLICATE

def step_deduplicate() -> Dict[str, Any]:
    
    state = PipelineStateManager.get_state()
    
    if not state.is_step_complete("parse"):
        raise ValueError("Parse step not complete. Run /sec/parse first.")
    
    registry = DocumentRegistry()
    db = SnowflakeService()
    
    state.deduplicated_filings = []
    skipped = 0
    
    try:
        for filing in state.parsed_filings:
            content_hash = filing["content_hash"]
            is_local_dup = registry.is_processed(content_hash)
            is_snowflake_dup = db.document_exists_by_hash(content_hash)
            
            if is_local_dup or is_snowflake_dup:
                skipped += 1
                state.stats["duplicates_skipped"] += 1
            else:
                registry.mark_as_processed(content_hash)
                state.deduplicated_filings.append(filing)
    finally:
        db.close()
    
    state.stats["unique_filings"] = len(state.deduplicated_filings)
    state.mark_step_complete("deduplicate")
    PipelineStateManager.save_state()
    
    return {"status": "success", "unique_filings": len(state.deduplicated_filings), "duplicates_skipped": skipped}



# STEP 4: CHUNK DOCUMENTS → Local + S3 (chunks/) + Snowflake

def step_chunk_documents(chunk_size: int = 1000, chunk_overlap: int = 100) -> Dict[str, Any]:
    
    state = PipelineStateManager.get_state()
    
    if not state.is_step_complete("deduplicate"):
        raise ValueError("Deduplicate step not complete. Run /sec/deduplicate first.")
    
    state.chunk_size = chunk_size
    state.chunk_overlap = chunk_overlap
    
    chunker = SemanticChunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks_dir = Path("data/chunks")
    chunks_dir.mkdir(parents=True, exist_ok=True)
    
    s3 = _get_s3()
    db = SnowflakeService()
    
    s3_uploads = []
    state.chunked_filings = []
    total_chunks = 0
    
    try:
        for filing in state.deduplicated_filings:
            try:
                doc = ParsedDocument(
                    company_ticker=filing["ticker"], filing_type=filing["filing_type"],
                    filing_date=datetime.fromisoformat(filing["filing_date"]), content=filing.get("content", ""),
                    sections=filing.get("sections", {}), tables=[], source_path=filing["source_path"],
                    content_hash=filing["content_hash"], word_count=filing["word_count"], table_count=filing.get("table_count", 0),
                )
                
                chunks = chunker.chunk_document(doc)
                doc_uuid = _generate_uuid()
                
                out_dir = chunks_dir / filing["ticker"]
                out_dir.mkdir(parents=True, exist_ok=True)
                
                chunks_data = {
                    "document_id": doc_uuid, "content_hash": filing["content_hash"], "ticker": filing["ticker"],
                    "filing_type": filing["filing_type"], "chunk_size": chunk_size, "chunk_overlap": chunk_overlap,
                    "chunk_count": len(chunks),
                    "chunks": [{"chunk_index": c.chunk_index, "section": c.section, "start_char": c.start_char, "end_char": c.end_char, "word_count": c.word_count, "content": c.content} for c in chunks],
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                
                out_path = out_dir / f"{filing['content_hash']}_chunks.json"
                out_path.write_text(json.dumps(chunks_data, indent=2, ensure_ascii=False), encoding="utf-8")
                
                try:
                    s3_key = s3.upload_chunks_json(chunks_data, filing["ticker"], filing["content_hash"])
                    s3_uploads.append(s3_key)
                except Exception as e:
                    logger.error(f"S3 upload failed for chunks {filing['content_hash']}: {e}")
                
                accession = filing.get("accession", "").replace("-", "")
                source_url = f"https://www.sec.gov/Archives/edgar/data/{accession}" if accession else None
                raw_s3_key = f"raw/{filing['ticker']}/{filing['filing_type']}/{accession}_full-submission.txt" if accession else None
                
                db.insert_document(
                    doc_id=doc_uuid, company_id=state.company_id, ticker=filing["ticker"], filing_type=filing["filing_type"],
                    filing_date=datetime.fromisoformat(filing["filing_date"]), source_url=source_url, local_path=filing["source_path"],
                    s3_key=raw_s3_key, content_hash=filing["content_hash"], word_count=filing["word_count"],
                    chunk_count=len(chunks), status="chunked", error_message=None, processed_at=datetime.now(timezone.utc)
                )
                
                for chunk in chunks:
                    chunk.document_id = doc_uuid
                
                db.insert_chunks(chunks)
                
                total_chunks += len(chunks)
                state.chunked_filings.append({"document_id": doc_uuid, "content_hash": filing["content_hash"], "ticker": filing["ticker"], "chunks_path": str(out_path), "chunk_count": len(chunks), "s3_key": s3_key if s3_uploads else None})
                
            except Exception as e:
                state.stats["errors"] += 1
                state.stats["error_details"].append({"step": "chunk", "document": filing.get("content_hash", "unknown"), "error": str(e)})
    finally:
        db.close()
    
    state.stats["total_chunks"] = total_chunks
    state.mark_step_complete("chunk")
    PipelineStateManager.save_state()
    
    return {"status": "success", "documents_chunked": len(state.chunked_filings), "total_chunks": total_chunks, "s3_uploads": len(s3_uploads), "snowflake_inserts": {"documents": len(state.chunked_filings), "chunks": total_chunks}, "errors": state.stats["errors"]}



# STEP 5: EXTRACT ITEMS → Local + S3 (output_items/)

def step_extract_items() -> Dict[str, Any]:
    
    state = PipelineStateManager.get_state()
    
    if not state.is_step_complete("parse"):
        raise ValueError("Parse step not complete. Run /sec/parse first.")
    
    items_dir = Path("data/output_items")
    items_dir.mkdir(parents=True, exist_ok=True)
    
    s3 = _get_s3()
    s3_uploads = []
    
    state.extracted_items = []
    
    for filing in state.parsed_filings:
        sections = filing.get("sections", {})
        
        if not any([sections.get("item_1"), sections.get("item_1a"), sections.get("item_7")]):
            continue
        
        extracted = {
            "document_id": filing["content_hash"], "ticker": filing["ticker"], "filing_type": filing["filing_type"],
            "filing_date": filing["filing_date"], "accession": filing.get("accession", ""),
            "items": {"item_1": sections.get("item_1", ""), "item_1a": sections.get("item_1a", ""), "item_7": sections.get("item_7", "")},
            "item_word_counts": {"item_1": len(sections.get("item_1", "").split()), "item_1a": len(sections.get("item_1a", "").split()), "item_7": len(sections.get("item_7", "").split())},
            "extracted_at": datetime.now(timezone.utc).isoformat()
        }
        
        out_dir = items_dir / filing["ticker"]
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{filing['content_hash']}_items.json"
        out_path.write_text(json.dumps(extracted, indent=2, ensure_ascii=False), encoding="utf-8")
        
        try:
            s3_key = f"output_items/{filing['ticker']}/{filing['content_hash']}_items.json"
            s3.upload_json(extracted, s3_key)
            s3_uploads.append(s3_key)
        except Exception as e:
            logger.error(f"S3 upload failed for items {filing['content_hash']}: {e}")
        
        state.extracted_items.append({"document_id": filing["content_hash"], "ticker": filing["ticker"], "items_path": str(out_path), "s3_key": s3_key if s3_uploads else None, "has_item_1": bool(sections.get("item_1")), "has_item_1a": bool(sections.get("item_1a")), "has_item_7": bool(sections.get("item_7"))})
        state.stats["items_extracted"] += 1
    
    state.mark_step_complete("extract_items")
    PipelineStateManager.save_state()
    
    return {"status": "success", "items_extracted": state.stats["items_extracted"], "s3_uploads": len(s3_uploads), "files": state.extracted_items}



# STEP 6: GET PIPELINE STATS

def get_pipeline_stats() -> Dict[str, Any]:
    state = PipelineStateManager.get_state()
    return {
        "ticker": state.ticker, "company_id": state.company_id,
        "config": {"filing_types": state.filing_types, "from_date": state.from_date, "to_date": state.to_date, "chunk_size": state.chunk_size, "chunk_overlap": state.chunk_overlap},
        "steps_completed": state.steps_completed, "stats": state.stats, "last_updated": state.last_updated
    }



# PREVIEW FILINGS

def preview_filings(ticker: str, filing_type: str = "8-K", limit: int = 10) -> Dict[str, Any]:
    import requests
    
    email = os.getenv("SEC_EMAIL", "test@example.com")
    company_name = os.getenv("SEC_COMPANY_NAME", "PE-OrgAIR-Platform")
    headers = {"User-Agent": f"{company_name} ({email})", "Accept-Encoding": "gzip, deflate"}
    limit = min(limit, 15)
    
    try:
        cik = None
        time.sleep(0.5)
        resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=60)
        if resp.status_code != 200:
            return {"status": "error", "message": f"Failed to fetch ticker list: HTTP {resp.status_code}"}
        
        for item in resp.json().values():
            if item.get("ticker", "").upper() == ticker.upper():
                cik = str(item.get("cik_str", ""))
                break
        
        if not cik:
            return {"status": "error", "message": f"Could not find CIK for ticker {ticker}"}
        
        time.sleep(0.5)
        response = requests.get(f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json", headers=headers, timeout=60)
        if response.status_code != 200:
            return {"status": "error", "message": f"Failed to fetch submissions: HTTP {response.status_code}"}
        
        data = response.json()
        recent = data.get("filings", {}).get("recent", {})
        forms, accessions, dates, primary_docs = recent.get("form", []), recent.get("accessionNumber", []), recent.get("filingDate", []), recent.get("primaryDocument", [])
        
        results, total_with_pdf, total_with_excel, checked = [], 0, 0, 0
        
        for i, form in enumerate(forms):
            if checked >= limit or form != filing_type:
                continue
            
            accession = accessions[i] if i < len(accessions) else ""
            time.sleep(0.3)
            
            try:
                idx_response = requests.get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/index.json", headers=headers, timeout=60)
                if idx_response.status_code == 200:
                    documents = idx_response.json().get("directory", {}).get("item", [])
                    pdf_files = [d.get("name", "") for d in documents if d.get("name", "").lower().endswith(".pdf")]
                    excel_files = [d.get("name", "") for d in documents if d.get("name", "").lower().endswith((".xlsx", ".xls"))]
                    
                    if pdf_files: total_with_pdf += 1
                    if excel_files: total_with_excel += 1
                    
                    results.append({"accession": accession, "filing_date": dates[i] if i < len(dates) else "", "primary_document": primary_docs[i] if i < len(primary_docs) else "", "has_pdf": bool(pdf_files), "has_excel": bool(excel_files), "pdf_files": pdf_files, "excel_files": excel_files})
            except Exception as e:
                logger.warning(f"Error fetching index for {accession}: {e}")
            
            checked += 1
        
        return {"status": "success", "ticker": ticker.upper(), "cik": cik, "filing_type": filing_type, "total_filings_checked": len(results), "filings_with_pdf": total_with_pdf, "filings_with_excel": total_with_excel, "filings": results}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}



# DOWNLOAD ALL COMPANIES - Complete pipeline

def step_download_all_companies(
    from_date: str,
    to_date: str,
    rate_limit: float = 0.2,
    limit_per_filing_type: int = 5,
    filing_types: List[str] = None,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
) -> Dict[str, Any]:
    """
    Complete pipeline for ALL companies: Download → Parse → Chunk → S3 → Snowflake
    
    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        rate_limit: Delay between SEC requests (default: 0.2s)
        limit_per_filing_type: Max filings per company per filing type (default: 5)
        filing_types: List of filing types (default: all supported)
        chunk_size: Words per chunk (default: 1000)
        chunk_overlap: Overlap words (default: 100)
    """
    
    company_name = os.getenv("SEC_COMPANY_NAME", "PE-OrgAIR-Platform")
    email = os.getenv("SEC_EMAIL")
    if not email:
        raise ValueError("SEC_EMAIL not set in .env")
    
    db = SnowflakeService()
    try:
        companies = db.list_companies()
        if not companies:
            raise ValueError("No companies found in Snowflake companies table")
    finally:
        db.close()
    
    if filing_types is None:
        filing_types = SECEdgarPipeline.SUPPORTED_FILING_TYPES
    
    logger.info(f"Starting pipeline for {len(companies)} companies, filing types: {filing_types}, limit: {limit_per_filing_type} per type")
    
    download_dir = Path("data/raw/sec")
    parsed_dir = Path("data/parsed")
    tables_dir = Path("data/tables")
    chunks_dir = Path("data/chunks")
    for d in [parsed_dir, tables_dir, chunks_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    sec = SECEdgarPipeline(company_name=company_name, email=email, download_dir=download_dir)
    parser = DocumentParser()
    chunker = SemanticChunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    s3 = _get_s3()
    
    results = {"companies_processed": 0, "companies_failed": 0, "total_filings_downloaded": 0, "total_documents_parsed": 0, "total_chunks_created": 0, "total_s3_uploads": 0, "snowflake_documents_inserted": 0, "snowflake_chunks_inserted": 0, "by_company": [], "errors": []}
    
    for company in companies:
        company_id, ticker = company.get("ID"), company.get("TICKER")
        if not ticker:
            continue
        
        ticker = ticker.upper()
        logger.info(f"Processing {ticker}")
        
        company_result = {"company_id": company_id, "ticker": ticker, "filings_downloaded": 0, "documents_parsed": 0, "chunks_created": 0, "s3_uploads": 0, "snowflake_docs": 0, "snowflake_chunks": 0, "errors": []}
        
        db = SnowflakeService()
        
        try:
            for filing_type in filing_types:
                try:
                    time.sleep(rate_limit)
                    sec.dl.get(filing_type, ticker, limit=limit_per_filing_type, after=from_date, before=to_date)
                    
                    filing_dir = download_dir / "sec-edgar-filings" / ticker / filing_type
                    if not filing_dir.exists():
                        continue
                    
                    for fp in filing_dir.glob("**/full-submission.txt"):
                        try:
                            sec._download_exhibits(fp, ticker)
                            company_result["filings_downloaded"] += 1
                            
                            # Parse
                            doc = parser.parse_filing(fp, ticker)
                            out_dir = parsed_dir / ticker
                            out_dir.mkdir(parents=True, exist_ok=True)
                            
                            parsed_data = {"document_id": doc.content_hash, "ticker": doc.company_ticker, "filing_type": doc.filing_type, "filing_date": doc.filing_date.isoformat(), "content_hash": doc.content_hash, "word_count": doc.word_count, "table_count": len(doc.tables), "sections": doc.sections, "source_path": doc.source_path, "accession": fp.parent.name, "parsed_at": datetime.now(timezone.utc).isoformat()}
                            
                            (out_dir / f"{doc.content_hash}.json").write_text(json.dumps(parsed_data, indent=2, ensure_ascii=False), encoding="utf-8")
                            (out_dir / f"{doc.content_hash}_content.txt").write_text(doc.content, encoding="utf-8")
                            
                            if doc.tables:
                                tables_out_dir = tables_dir / ticker
                                tables_out_dir.mkdir(parents=True, exist_ok=True)
                                tables_data = {"document_id": doc.content_hash, "ticker": ticker, "filing_type": filing_type, "table_count": len(doc.tables), "tables": doc.tables, "extracted_at": datetime.now(timezone.utc).isoformat()}
                                (tables_out_dir / f"{doc.content_hash}_tables.json").write_text(json.dumps(tables_data, indent=2, ensure_ascii=False), encoding="utf-8")
                            
                            company_result["documents_parsed"] += 1
                            
                            # Chunk
                            chunks = chunker.chunk_document(doc)
                            doc_uuid = _generate_uuid()
                            
                            chunks_out_dir = chunks_dir / ticker
                            chunks_out_dir.mkdir(parents=True, exist_ok=True)
                            chunks_data = {"document_id": doc_uuid, "content_hash": doc.content_hash, "ticker": ticker, "filing_type": filing_type, "chunk_count": len(chunks), "chunks": [{"chunk_index": c.chunk_index, "section": c.section, "start_char": c.start_char, "end_char": c.end_char, "word_count": c.word_count, "content": c.content} for c in chunks], "created_at": datetime.now(timezone.utc).isoformat()}
                            (chunks_out_dir / f"{doc.content_hash}_chunks.json").write_text(json.dumps(chunks_data, indent=2, ensure_ascii=False), encoding="utf-8")
                            
                            company_result["chunks_created"] += len(chunks)
                            
                            # S3 Upload
                            try:
                                s3.upload_raw_file(fp, ticker, filing_type, f"{fp.parent.name}_{fp.name}")
                                s3.upload_parsed_json(parsed_data, ticker, doc.content_hash)
                                s3.upload_parsed_content(doc.content, ticker, doc.content_hash)
                                if doc.tables:
                                    s3.upload_tables_json(tables_data, ticker, doc.content_hash)
                                s3.upload_chunks_json(chunks_data, ticker, doc.content_hash)
                                company_result["s3_uploads"] += 5
                                
                                exhibits_dir = fp.parent / "exhibits"
                                if exhibits_dir.exists():
                                    for exhibit in exhibits_dir.glob("*"):
                                        s3.upload_raw_file(exhibit, ticker, filing_type, exhibit.name)
                                        company_result["s3_uploads"] += 1
                            except Exception as e:
                                company_result["errors"].append(f"S3: {str(e)}")
                            
                            # Snowflake
                            try:
                                accession = fp.parent.name.replace("-", "")
                                db.insert_document(doc_id=doc_uuid, company_id=company_id, ticker=ticker, filing_type=filing_type, filing_date=doc.filing_date, source_url=f"https://www.sec.gov/Archives/edgar/data/{accession}", local_path=str(fp), s3_key=f"raw/{ticker}/{filing_type}/{fp.parent.name}_{fp.name}", content_hash=doc.content_hash, word_count=doc.word_count, chunk_count=len(chunks), status="indexed", processed_at=datetime.now(timezone.utc))
                                company_result["snowflake_docs"] += 1
                                
                                for chunk in chunks:
                                    chunk.document_id = doc_uuid
                                db.insert_chunks(chunks)
                                company_result["snowflake_chunks"] += len(chunks)
                            except Exception as e:
                                company_result["errors"].append(f"Snowflake: {str(e)}")
                            
                        except Exception as e:
                            company_result["errors"].append(f"Processing {fp.name}: {str(e)}")
                            
                except Exception as e:
                    company_result["errors"].append(f"{ticker}/{filing_type}: {str(e)}")
        finally:
            db.close()
        
        results["total_filings_downloaded"] += company_result["filings_downloaded"]
        results["total_documents_parsed"] += company_result["documents_parsed"]
        results["total_chunks_created"] += company_result["chunks_created"]
        results["total_s3_uploads"] += company_result["s3_uploads"]
        results["snowflake_documents_inserted"] += company_result["snowflake_docs"]
        results["snowflake_chunks_inserted"] += company_result["snowflake_chunks"]
        
        if company_result["errors"]:
            results["companies_failed"] += 1
            results["errors"].extend(company_result["errors"])
        
        results["companies_processed"] += 1
        results["by_company"].append({"ticker": ticker, "filings": company_result["filings_downloaded"], "parsed": company_result["documents_parsed"], "chunks": company_result["chunks_created"], "s3_uploads": company_result["s3_uploads"], "snowflake_docs": company_result["snowflake_docs"], "snowflake_chunks": company_result["snowflake_chunks"], "errors": len(company_result["errors"])})
        
        logger.info(f"Completed {ticker}: {company_result['snowflake_docs']} docs, {company_result['snowflake_chunks']} chunks")
    
    return {"status": "success", "config": {"from_date": from_date, "to_date": to_date, "rate_limit": rate_limit, "limit_per_filing_type": limit_per_filing_type, "filing_types": filing_types, "chunk_size": chunk_size, "chunk_overlap": chunk_overlap, "total_companies": len(companies)}, "results": results}