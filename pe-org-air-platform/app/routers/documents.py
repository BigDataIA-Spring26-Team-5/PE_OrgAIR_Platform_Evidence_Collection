from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from app.services.snowflake import SnowflakeService
from app.pipelines.runner import run_pipeline_1_for_company

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
async def collect_documents(req: DocumentCollectRequest, background_tasks: BackgroundTasks):
    """
    Trigger SEC document collection (Pipeline 1) as background task.
    """
    if not req.ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    background_tasks.add_task(
        run_pipeline_1_for_company,
        company_id=req.company_id,
        ticker=req.ticker.upper(),
        filing_types=req.filing_types,
        limit=req.limit,
        after=req.after,
        export_samples=True,
    )

    return DocumentCollectResponse(
        status="queued",
        message=f"Document collection queued for {req.ticker.upper()}",
    )

def list_companies(self) -> list[dict]:
    sql = """
        SELECT id, ticker
        FROM companies
        WHERE is_deleted = FALSE
          AND ticker IS NOT NULL
    """
    cur = self.conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()
        
@router.get("")
async def list_documents(ticker: Optional[str] = Query(default=None)):
    db = SnowflakeService()
    try:
        return db.list_documents(ticker=ticker.upper() if ticker else None)
    finally:
        db.close()


@router.get("/{doc_id}")
async def get_document(doc_id: str):
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
    db = SnowflakeService()
    try:
        return db.get_document_chunks(doc_id)
    finally:
        db.close()
