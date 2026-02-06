"""
Evidence API Router
app/routers/evidence.py

Endpoints:
- GET /api/v1/companies/{ticker}/evidence - Get all collected evidence for a company
"""

from fastapi import APIRouter, HTTPException
import logging

from app.repositories.company_repository import CompanyRepository
from app.repositories.document_repository import get_document_repository
from app.repositories.signal_repository import get_signal_repository
from app.models.evidence import (
    CompanyEvidenceResponse,
    DocumentEvidence,
    SignalEvidence,
    SignalSummary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Evidence"])


@router.get(
    "/companies/{ticker}/evidence",
    response_model=CompanyEvidenceResponse,
    summary="Get all collected evidence for a company",
    description="Returns existing SEC filing documents and external signals stored in Snowflake for the given company ticker.",
)
async def get_company_evidence(ticker: str):
    """Retrieve all collected evidence (documents + signals) for a company by ticker."""
    ticker = ticker.upper()

    # Look up company
    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(ticker)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found for ticker: {ticker}")

    company_id = str(company["id"])

    # Fetch documents and signals from Snowflake
    doc_repo = get_document_repository()
    signal_repo = get_signal_repository()

    documents = doc_repo.get_by_ticker(ticker)
    signals = signal_repo.get_signals_by_ticker(ticker)
    summary = signal_repo.get_summary_by_ticker(ticker)

    # Build response
    doc_evidence = [
        DocumentEvidence(
            id=doc["id"],
            filing_type=doc.get("filing_type", ""),
            filing_date=doc.get("filing_date"),
            source_url=doc.get("source_url"),
            s3_key=doc.get("s3_key"),
            word_count=doc.get("word_count"),
            chunk_count=doc.get("chunk_count"),
            status=doc.get("status", "unknown"),
            created_at=doc.get("created_at"),
            processed_at=doc.get("processed_at"),
        )
        for doc in documents
    ]

    signal_evidence = [
        SignalEvidence(
            id=sig["id"],
            category=sig.get("category", ""),
            source=sig.get("source", ""),
            signal_date=sig.get("signal_date"),
            raw_value=sig.get("raw_value"),
            normalized_score=sig.get("normalized_score"),
            confidence=sig.get("confidence"),
            metadata=sig.get("metadata"),
            created_at=sig.get("created_at"),
        )
        for sig in signals
    ]

    signal_summary = None
    if summary:
        signal_summary = SignalSummary(
            technology_hiring_score=summary.get("technology_hiring_score"),
            innovation_activity_score=summary.get("innovation_activity_score"),
            digital_presence_score=summary.get("digital_presence_score"),
            leadership_signals_score=summary.get("leadership_signals_score"),
            composite_score=summary.get("composite_score"),
            signal_count=summary.get("signal_count", 0),
            last_updated=summary.get("last_updated"),
        )

    return CompanyEvidenceResponse(
        company_id=company_id,
        company_name=company.get("name", ""),
        ticker=ticker,
        documents=doc_evidence,
        document_count=len(doc_evidence),
        signals=signal_evidence,
        signal_count=len(signal_evidence),
        signal_summary=signal_summary,
    )
