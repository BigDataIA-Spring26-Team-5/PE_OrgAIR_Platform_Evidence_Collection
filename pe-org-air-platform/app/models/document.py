from pydantic import BaseModel, Field
from typing import Optional, List, Dict
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
    PARSED = "parsed"
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


class ParsedDocumentResult(BaseModel):
    document_id: str
    ticker: str
    filing_type: str
    filing_date: str
    source_format: str
    word_count: int
    table_count: int
    sections_found: List[str]
    parse_errors: List[str]
    s3_parsed_key: str


class ParseByTickerResponse(BaseModel):
    ticker: str
    total_documents: int
    parsed: int
    skipped: int
    failed: int
    results: List[ParsedDocumentResult] = []


class ParseAllResponse(BaseModel):
    total_parsed: int
    total_skipped: int
    total_failed: int
    by_company: List[dict]



# REPORT MODELS


class CompanyDocumentStats(BaseModel):
    """Document statistics for a single company"""
    ticker: str
    form_10k: int = 0
    form_10q: int = 0
    form_8k: int = 0
    def_14a: int = 0
    total: int = 0
    chunks: int = 0
    word_count: int = 0


class SummaryStatistics(BaseModel):
    """Overall summary statistics"""
    companies_processed: int
    total_documents: int
    total_chunks: int
    total_words: int
    documents_by_status: Dict[str, int]


class EvidenceCollectionReport(BaseModel):
    """Complete evidence collection report"""
    report_generated_at: datetime
    summary: SummaryStatistics
    documents_by_company: List[CompanyDocumentStats]
    status_breakdown: Dict[str, int]



# CHUNKING MODELS


class ChunkByTickerResponse(BaseModel):
    """Response for chunking by ticker"""
    ticker: str
    total_documents: int
    chunked: int
    skipped: int
    failed: int
    total_chunks: int
    chunk_size: int
    chunk_overlap: int


class ChunkAllResponse(BaseModel):
    """Response for chunking all companies"""
    total_documents_chunked: int
    total_chunks_created: int
    chunk_size: int
    chunk_overlap: int
    by_company: List[dict]


class DocumentChunkResponse(BaseModel):
    """Single chunk response"""
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: Optional[str]
    start_char: int
    end_char: int
    word_count: int