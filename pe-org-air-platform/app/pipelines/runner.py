# from __future__ import annotations

# import os
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import List, Optional

# from app.pipelines.sec_edgar import SECEdgarPipeline
# from app.pipelines.document_parser import DocumentParser
# from app.pipelines.chunking import SemanticChunker
# from app.pipelines.exporters import export_sample_json
# from app.services.s3_storage import S3Storage
# from app.services.snowflake import SnowflakeService
# from app.pipelines.exporters import (
#     export_sample_json,
#     export_parsed_document_json,
#     export_chunks_json,
# )



# def _build_s3_key(local_path: Path, ticker: str, filing_type: str) -> str:
#     """
#     Deterministic key aligned to download layout:
#       sec/{ticker}/{filing_type}/{accession}/full-submission.txt
#     """
#     accession = local_path.parent.name
#     return f"sec/{ticker}/{filing_type}/{accession}/full-submission.txt"

# def _build_parsed_s3_key(ticker: str, document_id: str) -> str:
#     return f"processed/parsed/{ticker}/{document_id}.json"


# def _build_chunks_s3_key(ticker: str, document_id: str) -> str:
#     return f"processed/chunks/{ticker}/{document_id}.json"


# def run_pipeline_1_for_company(
#     company_id: str,
#     ticker: str,
#     filing_types: Optional[List[str]] = None,
#     limit: int = 10,
#     after: str = "2021-01-01",
#     export_samples: bool = True,
#     samples_dir: Path = Path("data/samples"),
#     max_samples: int = 3,
# ) -> dict:
#     """
#     SEC -> S3 raw -> parse -> dedupe -> chunk -> Snowflake (documents + chunks) -> export sample JSONs
#     """
#     company_name = os.getenv("SEC_COMPANY_NAME", "PE-OrgAIR-Platform")
#     email = os.getenv("SEC_EMAIL")
#     if not email:
#         raise ValueError("SEC_EMAIL not set in .env")

#     db = SnowflakeService()
#     s3 = S3Storage()
#     sec = SECEdgarPipeline(company_name=company_name, email=email)
#     parser = DocumentParser()
#     chunker = SemanticChunker()

#     stats = {"downloaded": 0, "processed": 0, "deduped": 0, "failed": 0, "chunks": 0, "samples_written": 0}

#     try:
#         filings = sec.download_filings(ticker=ticker, filing_types=filing_types, limit=limit, after=after)
#         stats["downloaded"] = len(filings)

#         for filing_path in filings:
#             try:
#                 # Upload raw to S3
#                 # filing_type from path is accurate here
#                 filing_type_from_path = filing_path.parts[-3]
#                 s3_key = _build_s3_key(filing_path, ticker, filing_type_from_path)
#                 s3.upload_file(filing_path, s3_key)

#                 # Parse
#                 doc = parser.parse_filing(filing_path, ticker)

#                 # Dedupe by content hash
#                 if db.document_exists_by_hash(doc.content_hash):
#                     stats["deduped"] += 1
#                     # still record status if you want; skipping insert is ok
#                     continue

#                 # Chunk
#                 chunks = chunker.chunk_document(doc)

#                 # Insert document
#                 doc_id = db.insert_document(
#                     company_id=company_id,
#                     ticker=ticker,
#                     filing_type=doc.filing_type,
#                     filing_date=doc.filing_date,
#                     local_path=str(filing_path),
#                     s3_key=s3_key,
#                     content_hash=doc.content_hash,
#                     word_count=doc.word_count,
#                     chunk_count=len(chunks),
#                     status="chunked",
#                     processed_at=datetime.now(timezone.utc),
#                 )

#                 # Insert chunks
#                 inserted = db.insert_chunks(chunks)
#                 stats["chunks"] += inserted
#                 stats["processed"] += 1

#                 # Export sample JSON (first N successful)
#                 if export_samples and stats["samples_written"] < max_samples:
#                     export_sample_json(
#                         out_dir=samples_dir,
#                         document_id=doc_id,
#                         doc=doc,
#                         s3_key=s3_key,
#                         source_url=None,
#                         chunks=chunks,
#                     )
#                     stats["samples_written"] += 1

#             except Exception:
#                 stats["failed"] += 1
#                 # best effort: mark document failed if hash known; otherwise skip
#                 continue

#         return stats

#     finally:
#         db.close()

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser
from app.pipelines.chunking import SemanticChunker
from app.pipelines.exporters import (
    export_sample_json,
    export_parsed_document_json,
    export_chunks_json,
)
from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService


def _build_raw_s3_key(local_path: Path, ticker: str, filing_type: str) -> str:
    accession = local_path.parent.name
    return f"raw/sec/{ticker}/{filing_type}/{accession}/full-submission.txt"


def _build_parsed_s3_key(ticker: str, document_id: str) -> str:
    return f"processed/parsed/{ticker}/{document_id}.json"


def _build_chunks_s3_key(ticker: str, document_id: str) -> str:
    return f"processed/chunks/{ticker}/{document_id}.json"


def run_pipeline_1_for_company(
    company_id: str,
    ticker: str,
    filing_types: Optional[List[str]] = None,
    limit: int = 10,
    after: str = "2021-01-01",
    export_samples: bool = True,
    samples_dir: Path = Path("data/samples"),
    max_samples: int = 3,
) -> dict:
    """
    Pipeline 1:
    SEC → RAW (local + S3)
        → PARSE + DEDUPE
        → PARSED JSON (local + S3)
        → CHUNK
        → CHUNKS JSON (local + S3)
        → Snowflake (documents + document_chunks)
    """

    company_name = os.getenv("SEC_COMPANY_NAME", "PE-OrgAIR-Platform")
    email = os.getenv("SEC_EMAIL")
    if not email:
        raise ValueError("SEC_EMAIL not set in .env")

    base_data_dir = Path("data")

    db = SnowflakeService()
    s3 = S3Storage()
    sec = SECEdgarPipeline(company_name=company_name, email=email)
    parser = DocumentParser()
    chunker = SemanticChunker()

    stats = {
        "downloaded": 0,
        "processed": 0,
        "deduped": 0,
        "failed": 0,
        "chunks": 0,
        "samples_written": 0,
    }

    try:
        filings = sec.download_filings(
            ticker=ticker,
            filing_types=filing_types,
            limit=limit,
            after=after,
        )
        stats["downloaded"] = len(filings)

        for filing_path in filings:
            try:
                # --------------------------------------------------
                # RAW → S3
                # --------------------------------------------------
                filing_type = filing_path.parts[-3]
                raw_s3_key = _build_raw_s3_key(filing_path, ticker, filing_type)
                s3.upload_file(filing_path, raw_s3_key)

                # --------------------------------------------------
                # PARSE
                # --------------------------------------------------
                doc = parser.parse_filing(filing_path, ticker)
                document_id = doc.content_hash  # stable ID

                # --------------------------------------------------
                # DEDUPE
                # --------------------------------------------------
                if db.document_exists_by_hash(document_id):
                    stats["deduped"] += 1
                    continue

                # --------------------------------------------------
                # EXPORT PARSED → local + S3
                # --------------------------------------------------
                parsed_local_path = export_parsed_document_json(
                    base_dir=base_data_dir,
                    document_id=document_id,
                    doc=doc,
                    raw_s3_key=raw_s3_key,
                )

                parsed_s3_key = _build_parsed_s3_key(ticker, document_id)
                s3.upload_file(parsed_local_path, parsed_s3_key)

                # --------------------------------------------------
                # CHUNK
                # --------------------------------------------------
                chunks = chunker.chunk_document(doc)

                # --------------------------------------------------
                # EXPORT CHUNKS → local + S3
                # --------------------------------------------------
                chunks_local_path = export_chunks_json(
                    base_dir=base_data_dir,
                    document_id=document_id,
                    ticker=ticker,
                    chunks=chunks,
                )

                chunks_s3_key = _build_chunks_s3_key(ticker, document_id)
                s3.upload_file(chunks_local_path, chunks_s3_key)

                # --------------------------------------------------
                # SNOWFLAKE INSERTS
                # --------------------------------------------------
                db.insert_document(
                    company_id=company_id,
                    ticker=ticker,
                    filing_type=doc.filing_type,
                    filing_date=doc.filing_date,
                    local_path=str(filing_path),
                    s3_key=raw_s3_key,
                    content_hash=document_id,
                    word_count=doc.word_count,
                    chunk_count=len(chunks),
                    status="chunked",
                    processed_at=datetime.now(timezone.utc),
                )

                inserted = db.insert_chunks(chunks)
                stats["chunks"] += inserted
                stats["processed"] += 1

                # --------------------------------------------------
                # OPTIONAL SAMPLE EXPORT (debug / grading)
                # --------------------------------------------------
                if export_samples and stats["samples_written"] < max_samples:
                    export_sample_json(
                        out_dir=samples_dir,
                        document_id=document_id,
                        doc=doc,
                        s3_key=raw_s3_key,
                        source_url=None,
                        chunks=chunks,
                    )
                    stats["samples_written"] += 1

            except Exception:
                stats["failed"] += 1
                continue

        return stats

    finally:
        db.close()
