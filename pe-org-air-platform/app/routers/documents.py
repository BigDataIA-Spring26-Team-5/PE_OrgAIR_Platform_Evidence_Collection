from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from app.services.snowflake import SnowflakeService

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])


class DocumentCollectRequest(BaseModel):
    company_id: str
    ticker: str
    filing_types: list[str] = ["10-K", "10-Q", "8-K", "DEF 14A"]
    after: str = "2021-01-01"
    limit: int = 10


class DocumentCollectResponse(BaseModel):
    status: str
    message: str


@router.post("/collect", response_model=DocumentCollectResponse)
async def collect_documents(req: DocumentCollectRequest):
    """
    DEPRECATED: Use the new SEC Pipeline endpoints instead.
    
    New endpoints:
    - POST /api/v1/sec/download
    - GET /api/v1/sec/parse
    - GET /api/v1/sec/deduplicate
    - POST /api/v1/sec/chunk
    - GET /api/v1/sec/extract-items
    - GET /api/v1/sec/stats
    """
    return DocumentCollectResponse(
        status="deprecated",
        message="This endpoint is deprecated. Please use the new SEC Pipeline: POST /api/v1/sec/download",
    )


@router.get("")
async def list_documents(ticker: Optional[str] = Query(default=None)):
    """List all documents from Snowflake, optionally filtered by ticker."""
    db = SnowflakeService()
    try:
        return db.list_documents(ticker=ticker.upper() if ticker else None)
    finally:
        db.close()


@router.get("/{doc_id}")
async def get_document(doc_id: str):
    """Get a single document by ID."""
    db = SnowflakeService()
    try:
        doc = db.get_document(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        return doc
    finally:
        db.close()


@router.get("/{doc_id}/chunks")
async def get_document_chunks(doc_id: str):
    """Get all chunks for a document."""
    db = SnowflakeService()
    try:
        return db.get_document_chunks(doc_id)
    finally:
        db.close()