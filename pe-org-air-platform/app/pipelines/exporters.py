from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from app.pipelines.chunking import DocumentChunk
from app.pipelines.document_parser import ParsedDocument


def export_sample_json(
    out_dir: Path,
    document_id: str,
    doc: ParsedDocument,
    s3_key: str | None,
    source_url: str | None,
    chunks: List[DocumentChunk],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "document_id": document_id,
        "company_ticker": doc.company_ticker,
        "filing_type": doc.filing_type,
        "filing_date": doc.filing_date.isoformat(),
        "source": {
            "local_path": doc.source_path,
            "s3_key": s3_key,
            "source_url": source_url,
        },
        "content_metadata": {
            "content_hash": doc.content_hash,
            "word_count": doc.word_count,
            "chunk_count": len(chunks),
        },
        "sections": doc.sections,
        "chunks": [
            {
                "chunk_index": c.chunk_index,
                "section": c.section,
                "content": c.content,
                "start_char": c.start_char,
                "end_char": c.end_char,
                "word_count": c.word_count,
            }
            for c in chunks
        ],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    

    safe_date = doc.filing_date.date().isoformat()
    fname = f"{doc.company_ticker}_{doc.filing_type}_{safe_date}.json".replace("/", "-")
    out_path = out_dir / fname

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path

def export_parsed_document_json(
    base_dir: Path,
    document_id: str,
    doc: ParsedDocument,
    raw_s3_key: str | None,
) -> Path:
    out_dir = base_dir / "processed" / "parsed" / doc.company_ticker
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "document_id": document_id,
        "ticker": doc.company_ticker,
        "filing_type": doc.filing_type,
        "filing_date": doc.filing_date.isoformat(),
        "content_hash": doc.content_hash,
        "word_count": doc.word_count,
        "sections": doc.sections,
        "source": {
            "local_path": doc.source_path,
            "s3_raw_key": raw_s3_key,
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    out_path = out_dir / f"{document_id}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def export_chunks_json(
    base_dir: Path,
    document_id: str,
    ticker: str,
    chunks: List[DocumentChunk],
) -> Path:
    out_dir = base_dir / "processed" / "chunks" / ticker
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "document_id": document_id,
        "ticker": ticker,
        "chunk_count": len(chunks),
        "chunks": [
            {
                "chunk_index": c.chunk_index,
                "section": c.section,
                "start_char": c.start_char,
                "end_char": c.end_char,
                "word_count": c.word_count,
                "content": c.content,
            }
            for c in chunks
        ],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    out_path = out_dir / f"{document_id}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path
