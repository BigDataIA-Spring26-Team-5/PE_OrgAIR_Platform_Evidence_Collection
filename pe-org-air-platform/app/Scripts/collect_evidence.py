#!/usr/bin/env python
"""
Collect evidence for all target companies.

Usage:
    python -m app.Scripts.collect_evidence --companies all
    python -m app.Scripts.collect_evidence --companies CAT,DE,UNH
"""

import argparse
import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID

import structlog

from app.pipelines.sec_edgar import SECEdgarCollector, SECFiling
from app.pipelines.document_parser import DocumentParser
from app.pipelines.chunking import SemanticChunker
from app.pipelines.job_signals import calculate_job_score
from app.pipelines.tech_signals import TechStackCollector, TechnologyDetection
from app.services.snowflake import SnowflakeService
from app.services.s3_storage import get_s3_service

logger = structlog.get_logger()

TARGET_COMPANIES = {
    "CAT": {"name": "Caterpillar Inc.", "sector": "Manufacturing"},
    "DE": {"name": "Deere & Company", "sector": "Manufacturing"},
    "UNH": {"name": "UnitedHealth Group", "sector": "Healthcare"},
    "HCA": {"name": "HCA Healthcare", "sector": "Healthcare"},
    "ADP": {"name": "Automatic Data Processing", "sector": "Services"},
    "PAYX": {"name": "Paychex Inc.", "sector": "Services"},
    "WMT": {"name": "Walmart Inc.", "sector": "Retail"},
    "TGT": {"name": "Target Corporation", "sector": "Retail"},
    "JPM": {"name": "JPMorgan Chase", "sector": "Financial"},
    "GS": {"name": "Goldman Sachs", "sector": "Financial"},
}


async def collect_documents(ticker: str, db: SnowflakeService) -> int:
    """Collect SEC documents for a company."""
    logger.info("Collecting documents", ticker=ticker)

    collector = SECEdgarCollector()
    parser = DocumentParser()
    chunker = SemanticChunker()

    # Download filings
    filings = list(collector.get_company_filings(
        ticker=ticker,
        filing_types=["10-K", "10-Q", "8-K"],
        years_back=3
    ))

    logger.info("Downloaded filings", ticker=ticker, count=len(filings))

    processed_count = 0
    # Parse and chunk each filing
    for filing in filings:
        try:
            # Download the filing content
            content = collector.download_filing(filing)
            if not content:
                logger.warning("Failed to download filing", ticker=ticker, filing_type=filing.filing_type)
                continue

            # Generate document ID
            doc_id = f"{ticker}_{filing.filing_type}_{filing.filing_date}"

            # Parse the document
            doc = parser.parse(
                content=content,
                document_id=doc_id,
                ticker=ticker,
                filing_type=filing.filing_type,
                filing_date=filing.filing_date,
                filename=filing.primary_document
            )

            # Chunk the document
            chunks = chunker.chunk_document(
                document_id=doc.document_id,
                content=doc.text_content,
                sections=doc.sections
            )

            # Store in database
            db.insert_document(
                company_id=ticker,  # Using ticker as company_id for simplicity
                ticker=ticker,
                filing_type=doc.filing_type,
                filing_date=datetime.strptime(doc.filing_date, "%Y-%m-%d"),
                local_path="",  # Not storing locally
                content_hash=str(hash(doc.text_content)),
                word_count=doc.word_count,
                status="chunked",
                chunk_count=len(chunks)
            )

            # Insert chunks
            db.insert_chunks(chunks)

            logger.info(
                "Processed document",
                ticker=ticker,
                filing_type=doc.filing_type,
                chunks=len(chunks)
            )
            processed_count += 1

        except Exception as e:
            logger.error("Failed to process", filing_type=filing.filing_type, error=str(e))

    return processed_count


async def collect_signals(ticker: str, company_id: UUID, db: SnowflakeService) -> int:
    """Collect external signals for a company."""
    logger.info("Collecting signals", ticker=ticker)

    signals = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Job postings (simplified - in practice, use API)
    # Note: The actual job signal collection uses the job_signals pipeline
    job_analysis = calculate_job_score(
        jobs=[]  # Would be populated from API
    )
    job_signal = {
        "signal_id": f"{company_id}_job_market_{timestamp}",
        "company_id": str(company_id),
        "category": "job_market",
        "source": "collect_evidence_script",
        "score": job_analysis["score"],
        "evidence_count": job_analysis.get("ai_jobs", 0),
        "summary": f"Found {job_analysis.get('ai_jobs', 0)} AI roles out of {job_analysis.get('total_tech_jobs', 0)} tech jobs",
        "raw_payload": job_analysis
    }
    signals.append(job_signal)

    # Technology stack
    tech_collector = TechStackCollector()
    tech_analysis = tech_collector.analyze_tech_stack(
        company_id=str(company_id),
        technologies=[]  # Would be populated from BuiltWith API
    )
    tech_signal = {
        "signal_id": f"{company_id}_tech_stack_{timestamp}",
        "company_id": str(company_id),
        "category": "tech_stack",
        "source": "collect_evidence_script",
        "score": tech_analysis["score"],
        "evidence_count": len(tech_analysis.get("ai_technologies", [])),
        "summary": f"Found {len(tech_analysis.get('ai_technologies', []))} AI technologies",
        "raw_payload": tech_analysis
    }
    signals.append(tech_signal)

    # Patents (simplified)
    # Note: The actual patent signal collection uses PatentsView API
    patent_signal = {
        "signal_id": f"{company_id}_patent_portfolio_{timestamp}",
        "company_id": str(company_id),
        "category": "patent_portfolio",
        "source": "collect_evidence_script",
        "score": 0.0,
        "evidence_count": 0,
        "summary": "No patents collected (placeholder)",
        "raw_payload": {"patents": []}  # Would be populated from USPTO/PatentsView API
    }
    signals.append(patent_signal)

    # Store signals to Snowflake external_signals table
    for signal in signals:
        try:
            _insert_external_signal(db, signal)
        except Exception as e:
            logger.warning("Failed to insert signal", signal_type=signal["category"], error=str(e))

    logger.info("Collected signals", ticker=ticker, count=len(signals))

    return len(signals)


def _insert_external_signal(db: SnowflakeService, signal: dict) -> None:
    """
    Insert an external signal into Snowflake.

    Table: external_signals
    Columns expected:
    - id: VARCHAR(100) PRIMARY KEY
    - company_id: VARCHAR(36) NOT NULL
    - category: VARCHAR(50) NOT NULL
    - source: VARCHAR(100)
    - score: FLOAT
    - evidence_count: INT
    - summary: VARCHAR(500)
    - raw_payload: VARIANT
    - created_at: TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    """
    sql = """
    INSERT INTO external_signals (
        id, company_id, category, source, score,
        evidence_count, summary, raw_payload, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP())
    """

    params = (
        signal["signal_id"],
        signal["company_id"],
        signal["category"],
        signal["source"],
        signal["score"],
        signal["evidence_count"],
        signal["summary"],
        json.dumps(signal["raw_payload"])
    )

    cur = db.conn.cursor()
    try:
        cur.execute(sql, params)
        db.conn.commit()
    finally:
        cur.close()


async def main(companies: list[str]) -> dict:
    """Main collection routine."""
    db = SnowflakeService()

    stats = {
        "companies": 0,
        "documents": 0,
        "signals": 0,
        "errors": 0
    }

    try:
        for ticker in companies:
            if ticker not in TARGET_COMPANIES:
                logger.warning("Unknown ticker", ticker=ticker)
                continue

            try:
                # Ensure company exists in database
                # Note: Using ticker as company_id for simplicity
                # In production, you would query/create the company in the database
                company_info = TARGET_COMPANIES[ticker]

                # Generate a deterministic UUID from ticker
                company_id = uuid.uuid5(uuid.NAMESPACE_DNS, ticker)

                # Collect documents
                doc_count = await collect_documents(ticker, db)
                stats["documents"] += doc_count

                # Collect signals
                signal_count = await collect_signals(ticker, company_id, db)
                stats["signals"] += signal_count

                stats["companies"] += 1

            except Exception as e:
                logger.error("Failed to process company", ticker=ticker, error=str(e))
                stats["errors"] += 1

        logger.info("Collection complete", **stats)
        return stats

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect evidence for target companies"
    )
    parser.add_argument(
        "--companies",
        default="all",
        help="Comma-separated tickers or 'all'"
    )
    args = parser.parse_args()

    if args.companies == "all":
        companies = list(TARGET_COMPANIES.keys())
    else:
        companies = [t.strip().upper() for t in args.companies.split(",")]

    asyncio.run(main(companies))
