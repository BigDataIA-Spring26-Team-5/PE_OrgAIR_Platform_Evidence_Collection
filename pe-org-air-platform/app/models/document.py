from __future__ import annotations

from typing import Dict, Any, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class Document(BaseModel):
    """
    Represents one SEC filing after parsing.
    Maps to the `documents` table in Snowflake.
    """
    document_id: str
    company_id: str
    ticker: str
    filing_type: str                 # 10-K, 10-Q, 8-K, etc.

    filing_date: Optional[str] = None
    source_path: str                 # local path to full-submission.txt
    content_hash: str                # used for de-duplication

    full_text: str                   # extracted text
    sections: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class DocumentChunk(BaseModel):
    """
    Represents one chunk of a document.
    Maps to the `document_chunks` table in Snowflake.
    """
    chunk_id: str
    document_id: str
    company_id: str

    chunk_index: int = Field(..., ge=0)
    section: Optional[str] = None

    text: str
    token_count: Optional[int] = Field(default=None, ge=0)

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
