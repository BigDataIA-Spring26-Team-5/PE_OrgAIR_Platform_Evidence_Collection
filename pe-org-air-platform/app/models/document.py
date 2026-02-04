# """
# Document Models for Pipeline 1: SEC EDGAR Document Collection
# app/models/document.py

# Matches Snowflake tables: documents, document_chunks
# As per Case Study 2 Assignment (Pages 10-12, 20)
# """

# from __future__ import annotations

# from dataclasses import dataclass
# from datetime import datetime, timezone
# from enum import Enum
# from typing import Any, Dict, List, Optional
# from uuid import uuid4

# from pydantic import BaseModel, Field, field_validator


# # =============================================================================
# # ENUMS (Page 12 - Document Registry)
# # =============================================================================

# class DocumentStatus(str, Enum):
#     """Pipeline status for document processing."""
#     PENDING = "pending"
#     DOWNLOADED = "downloaded"
#     PARSED = "parsed"
#     CHUNKED = "chunked"
#     INDEXED = "indexed"
#     FAILED = "failed"


# class FilingType(str, Enum):
#     """SEC filing types we collect (Page 6)."""
#     FORM_10K = "10-K"
#     FORM_10Q = "10-Q"
#     FORM_8K = "8-K"
#     FORM_DEF14A = "DEF 14A"


# # =============================================================================
# # DATABASE MODELS - Match Snowflake Schema (Page 20)
# # =============================================================================

# class DocumentRecord(BaseModel):
#     """
#     Metadata record for a document.
#     Maps to the `documents` table in Snowflake (Page 20, lines 2-20).
#     """
#     id: str = Field(default_factory=lambda: str(uuid4()))
#     company_id: str
#     ticker: str
#     filing_type: str
#     filing_date: datetime
#     source_url: Optional[str] = None
#     local_path: Optional[str] = None
#     s3_key: Optional[str] = None
#     content_hash: Optional[str] = None
#     word_count: Optional[int] = None
#     chunk_count: Optional[int] = None
#     status: DocumentStatus = DocumentStatus.PENDING
#     error_message: Optional[str] = None
#     created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
#     processed_at: Optional[datetime] = None

#     class Config:
#         from_attributes = True
#         use_enum_values = True

#     @field_validator("filing_type")
#     @classmethod
#     def validate_filing_type(cls, v: str) -> str:
#         valid_types = ["10-K", "10-Q", "8-K", "DEF 14A"]
#         if v not in valid_types:
#             raise ValueError(f"filing_type must be one of {valid_types}")
#         return v

#     @field_validator("status", mode="before")
#     @classmethod
#     def validate_status(cls, v) -> str:
#         valid_statuses = ["pending", "downloaded", "parsed", "chunked", "indexed", "failed"]
#         if isinstance(v, DocumentStatus):
#             return v.value
#         if v not in valid_statuses:
#             raise ValueError(f"status must be one of {valid_statuses}")
#         return v


# class DocumentChunkRecord(BaseModel):
#     """
#     A chunk of a document for LLM processing.
#     Maps to the `document_chunks` table in Snowflake (Page 20, lines 23-34).
#     """
#     id: str = Field(default_factory=lambda: str(uuid4()))
#     document_id: str
#     chunk_index: int = Field(..., ge=0)
#     content: str
#     section: Optional[str] = None
#     start_char: Optional[int] = None
#     end_char: Optional[int] = None
#     word_count: Optional[int] = None
#     created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

#     class Config:
#         from_attributes = True


# # =============================================================================
# # PIPELINE MODELS - Semantic Chunking (Page 10-11)
# # =============================================================================

# @dataclass
# class DocumentChunk:
#     """
#     A chunk of a document for processing (Page 10, lines 3-12).
#     Used internally by SemanticChunker.
#     """
#     document_id: str
#     chunk_index: int
#     content: str
#     section: str | None
#     start_char: int
#     end_char: int
#     word_count: int


# class ParsedDocument(BaseModel):
#     """
#     Represents a parsed SEC document (Page 7-9).
#     Contains the full extracted content before chunking.
#     """
#     company_ticker: str
#     filing_type: str
#     filing_date: datetime
#     content: str
#     sections: Dict[str, str] = Field(default_factory=dict)
#     source_path: str
#     content_hash: str
#     word_count: int

#     @property
#     def has_sections(self) -> bool:
#         return len(self.sections) > 0


# # =============================================================================
# # SEMANTIC CHUNKER (Page 10-11)
# # =============================================================================

# class SemanticChunker:
#     """Chunk documents with section awareness (Page 10-11)."""

#     def __init__(
#         self,
#         chunk_size: int = 1000,      # Target words per chunk
#         chunk_overlap: int = 100,    # Overlap in words
#         min_chunk_size: int = 200    # Minimum chunk size
#     ):
#         self.chunk_size = chunk_size
#         self.chunk_overlap = chunk_overlap
#         self.min_chunk_size = min_chunk_size

#     def chunk_document(self, doc: ParsedDocument) -> List[DocumentChunk]:
#         """Split document into overlapping chunks."""
#         chunks = []

#         # Chunk each section separately to preserve context
#         for section_name, section_content in doc.sections.items():
#             section_chunks = self._chunk_text(
#                 section_content,
#                 doc.content_hash,
#                 section_name
#             )
#             chunks.extend(section_chunks)

#         # Also chunk any remaining content
#         if not doc.sections:
#             chunks = self._chunk_text(doc.content, doc.content_hash, None)

#         return chunks

#     def _chunk_text(
#         self,
#         text: str,
#         doc_id: str,
#         section: str | None
#     ) -> List[DocumentChunk]:
#         """Split text into overlapping chunks."""
#         words = text.split()
#         chunks = []

#         start_idx = 0
#         chunk_index = 0

#         while start_idx < len(words):
#             end_idx = min(start_idx + self.chunk_size, len(words))

#             # Don't create tiny final chunks
#             if len(words) - end_idx < self.min_chunk_size:
#                 end_idx = len(words)

#             chunk_words = words[start_idx:end_idx]
#             chunk_content = " ".join(chunk_words)

#             # Calculate character positions (approximate)
#             start_char = len(" ".join(words[:start_idx]))
#             end_char = start_char + len(chunk_content)

#             chunks.append(DocumentChunk(
#                 document_id=doc_id,
#                 chunk_index=chunk_index,
#                 content=chunk_content,
#                 section=section,
#                 start_char=start_char,
#                 end_char=end_char,
#                 word_count=len(chunk_words)
#             ))

#             # Move forward with overlap
#             start_idx = end_idx - self.chunk_overlap
#             chunk_index += 1

#             if end_idx >= len(words):
#                 break

#         return chunks


# # =============================================================================
# # API REQUEST/RESPONSE MODELS
# # =============================================================================

# class DocumentCollectionRequest(BaseModel):
#     """Request to trigger document collection (Page 22)."""
#     company_id: Optional[str] = None
#     ticker: Optional[str] = None
#     filing_types: List[str] = Field(default=["10-K", "10-Q", "8-K", "DEF 14A"])
#     after_date: str = "2021-01-01"
#     limit: int = Field(default=10, ge=1, le=50)

#     @field_validator("filing_types")
#     @classmethod
#     def validate_filing_types(cls, v: List[str]) -> List[str]:
#         valid = ["10-K", "10-Q", "8-K", "DEF 14A"]
#         for ft in v:
#             if ft not in valid:
#                 raise ValueError(f"Invalid filing type: {ft}. Must be one of {valid}")
#         return v


# class DocumentCollectionResponse(BaseModel):
#     """Response after triggering document collection (Page 22-23)."""
#     task_id: str
#     status: str
#     message: str
#     company_id: Optional[str] = None
#     ticker: Optional[str] = None


# class DocumentResponse(BaseModel):
#     """Response for a single document."""
#     id: str
#     company_id: str
#     ticker: str
#     filing_type: str
#     filing_date: Optional[datetime] = None
#     s3_key: Optional[str] = None
#     content_hash: Optional[str] = None
#     word_count: Optional[int] = None
#     chunk_count: Optional[int] = None
#     status: str
#     created_at: datetime
#     processed_at: Optional[datetime] = None


# class DocumentListResponse(BaseModel):
#     """Response for listing documents."""
#     documents: List[DocumentResponse]
#     total: int
#     page: int = 1
#     page_size: int = 20


# class DocumentChunkResponse(BaseModel):
#     """Response for a document chunk."""
#     id: str
#     document_id: str
#     chunk_index: int
#     section: Optional[str] = None
#     content: str
#     word_count: Optional[int] = None


# class DocumentChunksResponse(BaseModel):
#     """Response for listing document chunks."""
#     document_id: str
#     chunks: List[DocumentChunkResponse]
#     total: int


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def create_document_record(
#     company_id: str,
#     ticker: str,
#     filing_type: str,
#     filing_date: datetime,
#     local_path: str,
#     source_url: Optional[str] = None,
# ) -> DocumentRecord:
#     """Factory function to create a new document record."""
#     return DocumentRecord(
#         company_id=company_id,
#         ticker=ticker,
#         filing_type=filing_type,
#         filing_date=filing_date,
#         source_url=source_url,
#         local_path=local_path,
#         status=DocumentStatus.DOWNLOADED,
#     )


# def create_chunk_record(
#     document_id: str,
#     chunk: DocumentChunk,
# ) -> DocumentChunkRecord:
#     """Factory function to create a chunk record from pipeline chunk."""
#     return DocumentChunkRecord(
#         document_id=document_id,
#         chunk_index=chunk.chunk_index,
#         section=chunk.section,
#         content=chunk.content,
#         start_char=chunk.start_char,
#         end_char=chunk.end_char,
#         word_count=chunk.word_count,
#     )


# from pydantic import BaseModel, Field
# from typing import Optional, List
# from datetime import date, datetime
# from enum import Enum

# class FilingType(str, Enum):
#     FORM_10K = "10-K"
#     FORM_10Q = "10-Q"
#     FORM_8K = "8-K"
#     DEF_14A = "DEF 14A"

# class DocumentStatus(str, Enum):
#     PENDING = "pending"
#     DOWNLOADING = "downloading"
#     UPLOADED = "uploaded"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"

# class DocumentCollectionRequest(BaseModel):
#     ticker: str = Field(..., description="Company ticker symbol", example="CAT")
#     filing_types: List[FilingType] = Field(
#         default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A],
#         description="Types of SEC filings to collect"
#     )
#     years_back: int = Field(default=3, ge=1, le=10, description="Number of years to look back")

# class DocumentMetadata(BaseModel):
#     id: str
#     company_id: str
#     ticker: str
#     filing_type: str
#     filing_date: date
#     source_url: Optional[str] = None
#     s3_key: Optional[str] = None
#     content_hash: Optional[str] = None
#     word_count: Optional[int] = None
#     chunk_count: Optional[int] = None
#     status: DocumentStatus = DocumentStatus.PENDING
#     error_message: Optional[str] = None
#     created_at: Optional[datetime] = None
#     processed_at: Optional[datetime] = None

# class DocumentCollectionResponse(BaseModel):
#     ticker: str
#     company_id: str
#     company_name: str
#     filing_types: List[str]
#     years_back: int
#     documents_found: int
#     documents_uploaded: int
#     documents_failed: int
#     documents: List[DocumentMetadata]

# class DocumentChunk(BaseModel):
#     id: str
#     document_id: str
#     chunk_index: int
#     content: str
#     section: Optional[str] = None
#     start_char: Optional[int] = None
#     end_char: Optional[int] = None
#     word_count: Optional[int] = None

# class CollectionProgress(BaseModel):
#     ticker: str
#     current_filing: str
#     status: str
#     files_processed: int
#     total_files: int
#     current_file: Optional[str] = None
#     message: str


from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from enum import Enum

class FilingType(str, Enum):
    FORM_10K = "10-K"
    FORM_10Q = "10-Q"
    FORM_8K = "8-K"
    DEF_14A = "DEF 14A"  # SEC returns it with space, no hyphen

class DocumentStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    INDEXED = "indexed"
    COMPLETED = "completed"
    FAILED = "failed"

class DocumentCollectionRequest(BaseModel):
    ticker: str = Field(..., description="Company ticker symbol", example="CAT")
    filing_types: List[FilingType] = Field(
        default=[FilingType.FORM_10K, FilingType.FORM_10Q, FilingType.FORM_8K, FilingType.DEF_14A],
        description="Types of SEC filings to collect"
    )
    years_back: int = Field(default=3, ge=1, le=10, description="Number of years to look back")

class DocumentMetadata(BaseModel):
    id: str
    company_id: str
    ticker: str
    filing_type: str
    filing_date: date
    source_url: Optional[str] = None
    s3_key: Optional[str] = None
    content_hash: Optional[str] = None
    word_count: Optional[int] = None
    chunk_count: Optional[int] = None
    status: DocumentStatus = DocumentStatus.PENDING
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None

class DocumentCollectionResponse(BaseModel):
    ticker: str
    company_id: str
    company_name: str
    filing_types: List[str]
    years_back: int
    documents_found: int
    documents_uploaded: int
    documents_skipped: int
    documents_failed: int
    # Summary by filing type
    summary: dict = {}  # e.g., {"10-K": 3, "10-Q": 9, "8-K": 5, "DEF 14A": 3}

class DocumentChunk(BaseModel):
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: Optional[str] = None
    start_char: Optional[int] = None
    end_char: Optional[int] = None
    word_count: Optional[int] = None

class CollectionProgress(BaseModel):
    ticker: str
    current_filing: str
    status: str
    files_processed: int
    total_files: int
    current_file: Optional[str] = None
    message: str