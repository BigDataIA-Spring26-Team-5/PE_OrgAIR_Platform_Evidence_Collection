# tests/test_sec_edgar.py
# Comprehensive tests for SEC EDGAR Pipeline - Models and APIs

import pytest
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from pydantic import ValidationError

# ============================================================
# SECTION 1: MODEL TESTS
# ============================================================

class TestFilingTypeEnum:
    """Tests for FilingType enum"""
    
    def test_filing_type_values(self):
        from app.models.document import FilingType
        
        assert FilingType.FORM_10K.value == "10-K"
        assert FilingType.FORM_10Q.value == "10-Q"
        assert FilingType.FORM_8K.value == "8-K"
        assert FilingType.DEF_14A.value == "DEF 14A"
    
    def test_filing_type_from_string(self):
        from app.models.document import FilingType
        
        assert FilingType("10-K") == FilingType.FORM_10K
        assert FilingType("10-Q") == FilingType.FORM_10Q
        assert FilingType("8-K") == FilingType.FORM_8K
        assert FilingType("DEF 14A") == FilingType.DEF_14A
    
    def test_invalid_filing_type(self):
        from app.models.document import FilingType
        
        with pytest.raises(ValueError):
            FilingType("INVALID")


class TestDocumentStatusEnum:
    """Tests for DocumentStatus enum"""
    
    def test_document_status_values(self):
        from app.models.document import DocumentStatus
        
        assert DocumentStatus.PENDING.value == "pending"
        assert DocumentStatus.DOWNLOADING.value == "downloading"
        assert DocumentStatus.UPLOADED.value == "uploaded"
        assert DocumentStatus.PROCESSING.value == "processing"
        assert DocumentStatus.PARSED.value == "parsed"
        assert DocumentStatus.INDEXED.value == "indexed"
        assert DocumentStatus.COMPLETED.value == "completed"
        assert DocumentStatus.FAILED.value == "failed"


class TestDocumentCollectionRequest:
    """Tests for DocumentCollectionRequest model"""
    
    def test_valid_request_minimal(self):
        from app.models.document import DocumentCollectionRequest
        
        request = DocumentCollectionRequest(ticker="CAT")
        
        assert request.ticker == "CAT"
        assert len(request.filing_types) == 4  # Default all 4 types
        assert request.years_back == 3  # Default
    
    def test_valid_request_full(self):
        from app.models.document import DocumentCollectionRequest, FilingType
        
        request = DocumentCollectionRequest(
            ticker="AAPL",
            filing_types=[FilingType.FORM_10K, FilingType.FORM_10Q],
            years_back=5
        )
        
        assert request.ticker == "AAPL"
        assert len(request.filing_types) == 2
        assert request.years_back == 5
    
    def test_invalid_years_back_too_low(self):
        from app.models.document import DocumentCollectionRequest
        
        with pytest.raises(ValidationError):
            DocumentCollectionRequest(ticker="CAT", years_back=0)
    
    def test_invalid_years_back_too_high(self):
        from app.models.document import DocumentCollectionRequest
        
        with pytest.raises(ValidationError):
            DocumentCollectionRequest(ticker="CAT", years_back=11)
    
    def test_missing_ticker(self):
        from app.models.document import DocumentCollectionRequest
        
        with pytest.raises(ValidationError):
            DocumentCollectionRequest()


class TestDocumentMetadata:
    """Tests for DocumentMetadata model"""
    
    def test_valid_metadata_minimal(self):
        from app.models.document import DocumentMetadata, DocumentStatus
        
        metadata = DocumentMetadata(
            id="doc-123",
            company_id="comp-456",
            ticker="CAT",
            filing_type="10-K",
            filing_date=date(2024, 2, 14)
        )
        
        assert metadata.id == "doc-123"
        assert metadata.ticker == "CAT"
        assert metadata.status == DocumentStatus.PENDING  # Default
        assert metadata.word_count is None
    
    def test_valid_metadata_full(self):
        from app.models.document import DocumentMetadata, DocumentStatus
        
        metadata = DocumentMetadata(
            id="doc-123",
            company_id="comp-456",
            ticker="AAPL",
            filing_type="10-Q",
            filing_date=date(2024, 6, 30),
            source_url="https://sec.gov/filing/123",
            s3_key="sec/raw/AAPL/10-Q/2024-06-30.html",
            content_hash="abc123hash",
            word_count=50000,
            chunk_count=100,
            status=DocumentStatus.PARSED
        )
        
        assert metadata.word_count == 50000
        assert metadata.chunk_count == 100
        assert metadata.status == DocumentStatus.PARSED


class TestDocumentCollectionResponse:
    """Tests for DocumentCollectionResponse model"""
    
    def test_valid_response(self):
        from app.models.document import DocumentCollectionResponse
        
        response = DocumentCollectionResponse(
            ticker="CAT",
            company_id="comp-123",
            company_name="Caterpillar Inc.",
            filing_types=["10-K", "10-Q"],
            years_back=3,
            documents_found=15,
            documents_uploaded=12,
            documents_skipped=2,
            documents_failed=1,
            summary={"10-K": 3, "10-Q": 9}
        )
        
        assert response.ticker == "CAT"
        assert response.documents_found == 15
        assert response.documents_uploaded == 12
        assert response.summary["10-K"] == 3


class TestDocumentChunk:
    """Tests for DocumentChunk model"""
    
    def test_valid_chunk(self):
        from app.models.document import DocumentChunk
        
        chunk = DocumentChunk(
            id="chunk-001",
            document_id="doc-123",
            chunk_index=0,
            content="This is the chunk content...",
            section="risk_factors",
            start_char=0,
            end_char=500,
            word_count=100
        )
        
        assert chunk.id == "chunk-001"
        assert chunk.chunk_index == 0
        assert chunk.section == "risk_factors"
        assert chunk.word_count == 100


class TestParsedDocumentResult:
    """Tests for ParsedDocumentResult model"""
    
    def test_valid_parsed_result(self):
        from app.models.document import ParsedDocumentResult
        
        result = ParsedDocumentResult(
            document_id="doc-123",
            ticker="CAT",
            filing_type="10-K",
            filing_date="2024-02-14",
            source_format="html",
            word_count=150000,
            table_count=45,
            sections_found=["business", "risk_factors", "mda"],
            parse_errors=[],
            s3_parsed_key="sec/parsed/CAT/10-K/2024-02-14_full.json"
        )
        
        assert result.word_count == 150000
        assert len(result.sections_found) == 3
        assert "business" in result.sections_found


class TestParseByTickerResponse:
    """Tests for ParseByTickerResponse model"""
    
    def test_valid_parse_response(self):
        from app.models.document import ParseByTickerResponse, ParsedDocumentResult
        
        result = ParsedDocumentResult(
            document_id="doc-123",
            ticker="CAT",
            filing_type="10-K",
            filing_date="2024-02-14",
            source_format="html",
            word_count=150000,
            table_count=45,
            sections_found=["business"],
            parse_errors=[],
            s3_parsed_key="sec/parsed/CAT/10-K/2024-02-14_full.json"
        )
        
        response = ParseByTickerResponse(
            ticker="CAT",
            total_documents=10,
            parsed=8,
            skipped=1,
            failed=1,
            results=[result]
        )
        
        assert response.ticker == "CAT"
        assert response.total_documents == 10
        assert len(response.results) == 1


class TestCompanyDocumentStats:
    """Tests for CompanyDocumentStats model"""
    
    def test_valid_stats(self):
        from app.models.document import CompanyDocumentStats
        
        stats = CompanyDocumentStats(
            ticker="CAT",
            form_10k=3,
            form_10q=12,
            form_8k=25,
            def_14a=3,
            total=43,
            chunks=500,
            word_count=2500000
        )
        
        assert stats.ticker == "CAT"
        assert stats.total == 43
        assert stats.chunks == 500
    
    def test_default_values(self):
        from app.models.document import CompanyDocumentStats
        
        stats = CompanyDocumentStats(ticker="NEW")
        
        assert stats.form_10k == 0
        assert stats.total == 0
        assert stats.word_count == 0


class TestChunkByTickerResponse:
    """Tests for ChunkByTickerResponse model"""
    
    def test_valid_chunk_response(self):
        from app.models.document import ChunkByTickerResponse
        
        response = ChunkByTickerResponse(
            ticker="CAT",
            total_documents=10,
            chunked=8,
            skipped=1,
            failed=1,
            total_chunks=500,
            chunk_size=750,
            chunk_overlap=50
        )
        
        assert response.ticker == "CAT"
        assert response.total_chunks == 500
        assert response.chunk_size == 750


# ============================================================
# SECTION 2: API ENDPOINT TESTS
# ============================================================

@pytest.fixture
def client():
    """Create test client"""
    from app.main import app
    return TestClient(app)


@pytest.fixture
def mock_document_collector():
    """Mock document collector service"""
    with patch('app.routers.documents.get_document_collector_service') as mock:
        yield mock


@pytest.fixture
def mock_parsing_service():
    """Mock parsing service"""
    with patch('app.routers.documents.get_document_parsing_service') as mock:
        yield mock


@pytest.fixture
def mock_chunking_service():
    """Mock chunking service"""
    with patch('app.routers.documents.get_document_chunking_service') as mock:
        yield mock


@pytest.fixture
def mock_document_repository():
    """Mock document repository"""
    with patch('app.routers.documents.get_document_repository') as mock:
        yield mock


@pytest.fixture
def mock_chunk_repository():
    """Mock chunk repository"""
    with patch('app.routers.documents.get_chunk_repository') as mock:
        yield mock


@pytest.fixture
def mock_s3_service():
    """Mock S3 service"""
    with patch('app.routers.documents.get_s3_service') as mock:
        yield mock


class TestCollectionEndpoints:
    """Tests for document collection endpoints"""
    
    def test_collect_documents_success(self, client, mock_document_collector):
        """Test successful document collection"""
        from app.models.document import DocumentCollectionResponse
        
        mock_service = Mock()
        mock_service.collect_for_company.return_value = DocumentCollectionResponse(
            ticker="CAT",
            company_id="comp-123",
            company_name="Caterpillar Inc.",
            filing_types=["10-K", "10-Q"],
            years_back=3,
            documents_found=15,
            documents_uploaded=12,
            documents_skipped=2,
            documents_failed=1,
            summary={"10-K": 3, "10-Q": 9}
        )
        mock_document_collector.return_value = mock_service
        
        response = client.post(
            "/api/v1/documents/collect",
            json={
                "ticker": "CAT",
                "filing_types": ["10-K", "10-Q"],
                "years_back": 3
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
        assert data["documents_found"] == 15
    
    def test_collect_documents_invalid_ticker(self, client, mock_document_collector):
        """Test collection with invalid ticker"""
        mock_service = Mock()
        mock_service.collect_for_company.side_effect = ValueError("Company not found")
        mock_document_collector.return_value = mock_service
        
        response = client.post(
            "/api/v1/documents/collect",
            json={"ticker": "INVALID"}
        )
        
        assert response.status_code == 404


class TestParsingEndpoints:
    """Tests for document parsing endpoints"""
    
    def test_parse_by_ticker_success(self, client, mock_parsing_service):
        """Test successful parsing by ticker"""
        from app.models.document import ParseByTickerResponse
        
        mock_service = Mock()
        mock_service.parse_by_ticker.return_value = ParseByTickerResponse(
            ticker="CAT",
            total_documents=10,
            parsed=8,
            skipped=1,
            failed=1,
            results=[]
        )
        mock_parsing_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/parse/CAT")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
        assert data["parsed"] == 8
    
    def test_parse_by_ticker_not_found(self, client, mock_parsing_service):
        """Test parsing with invalid ticker"""
        mock_service = Mock()
        mock_service.parse_by_ticker.side_effect = ValueError("Company not found")
        mock_parsing_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/parse/INVALID")
        
        assert response.status_code == 404
    
    def test_parse_all_documents(self, client, mock_parsing_service):
        """Test parsing all documents"""
        from app.models.document import ParseAllResponse
        
        mock_service = Mock()
        mock_service.parse_all_companies.return_value = ParseAllResponse(
            total_parsed=50,
            total_skipped=5,
            total_failed=2,
            by_company=[]
        )
        mock_parsing_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/parse")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_parsed"] == 50
    
    def test_get_parsed_document(self, client, mock_document_repository, mock_s3_service):
        """Test getting parsed document content"""
        # Mock repository
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = {
            "id": "doc-123",
            "ticker": "CAT",
            "filing_type": "10-K",
            "filing_date": date(2024, 2, 14)
        }
        mock_document_repository.return_value = mock_repo
        
        # Mock S3 service
        mock_s3 = Mock()
        mock_s3.get_file.return_value = b'{"word_count": 150000, "table_count": 45, "sections": {"business": "..."}, "text_content": "Sample text", "tables": []}'
        mock_s3_service.return_value = mock_s3
        
        response = client.get("/api/v1/documents/parsed/doc-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
        assert data["word_count"] == 150000
    
    def test_get_parsed_document_not_found(self, client, mock_document_repository):
        """Test getting non-existent parsed document"""
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = None
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/parsed/invalid-id")
        
        assert response.status_code == 404


class TestChunkingEndpoints:
    """Tests for document chunking endpoints"""
    
    def test_chunk_by_ticker_success(self, client, mock_chunking_service):
        """Test successful chunking by ticker"""
        mock_service = Mock()
        # Return a dict since the endpoint doesn't have response_model
        mock_service.chunk_by_ticker.return_value = {
            "ticker": "CAT",
            "total_documents": 10,
            "chunked": 8,
            "skipped": 1,
            "failed": 1,
            "total_chunks": 500,
            "chunk_size": 750,
            "chunk_overlap": 50
        }
        mock_chunking_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/chunk/CAT?chunk_size=750&chunk_overlap=50")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
        assert data["total_chunks"] == 500
    
    def test_chunk_by_ticker_not_found(self, client, mock_chunking_service):
        """Test chunking with invalid ticker"""
        mock_service = Mock()
        mock_service.chunk_by_ticker.side_effect = ValueError("Company not found")
        mock_chunking_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/chunk/INVALID")
        
        assert response.status_code == 404
    
    def test_chunk_all_documents(self, client, mock_chunking_service):
        """Test chunking all documents"""
        mock_service = Mock()
        mock_service.chunk_all_companies.return_value = {
            "total_documents_chunked": 100,
            "total_chunks_created": 5000,
            "chunk_size": 750,
            "chunk_overlap": 50,
            "by_company": []
        }
        mock_chunking_service.return_value = mock_service
        
        response = client.post("/api/v1/documents/chunk?chunk_size=750&chunk_overlap=50")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_chunks_created"] == 5000
    
    def test_get_document_chunks(self, client, mock_chunk_repository):
        """Test getting chunks for a document"""
        mock_repo = Mock()
        mock_repo.get_by_document_id.return_value = [
            {"id": "chunk-1", "chunk_index": 0, "content": "Content 1"},
            {"id": "chunk-2", "chunk_index": 1, "content": "Content 2"}
        ]
        mock_chunk_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/chunks/doc-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["chunk_count"] == 2
    
    def test_get_document_chunks_not_found(self, client, mock_chunk_repository):
        """Test getting chunks for document with no chunks"""
        mock_repo = Mock()
        mock_repo.get_by_document_id.return_value = []
        mock_chunk_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/chunks/invalid-id")
        
        assert response.status_code == 404
    
    def test_get_chunk_stats(self, client, mock_chunk_repository):
        """Test getting chunk statistics"""
        mock_repo = Mock()
        mock_repo.get_stats_by_ticker.return_value = {"10-K": 200, "10-Q": 300}
        mock_repo.count_by_ticker.return_value = 500
        mock_chunk_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/chunk/stats/CAT")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
        assert data["total_chunks"] == 500


class TestReportEndpoints:
    """Tests for report endpoints"""
    
    def test_get_evidence_report(self, client, mock_document_repository, mock_chunk_repository):
        """Test getting evidence report"""
        # Mock document repository
        mock_doc_repo = Mock()
        mock_doc_repo.get_summary_statistics.return_value = {
            "companies_processed": 10,
            "total_documents": 200,
            "total_words": 50000000
        }
        mock_doc_repo.get_status_breakdown.return_value = {"parsed": 180, "failed": 20}
        mock_doc_repo.get_all_company_stats.return_value = []
        mock_document_repository.return_value = mock_doc_repo
        
        # Mock chunk repository
        mock_chunk_repo = Mock()
        mock_chunk_repo.get_total_chunks.return_value = 10000
        mock_chunk_repository.return_value = mock_chunk_repo
        
        response = client.get("/api/v1/documents/report")
        
        assert response.status_code == 200
        data = response.json()
        assert "summary" in data
        assert data["summary"]["total_documents"] == 200
    
    def test_get_evidence_report_table(self, client, mock_document_repository, mock_chunk_repository):
        """Test getting evidence report in table format"""
        # Mock document repository
        mock_doc_repo = Mock()
        mock_doc_repo.get_summary_statistics.return_value = {
            "companies_processed": 10,
            "total_documents": 200,
            "total_words": 50000000
        }
        mock_doc_repo.get_status_breakdown.return_value = {"parsed": 180}
        mock_doc_repo.get_all_company_stats.return_value = [
            {"ticker": "CAT", "form_10k": 3, "form_10q": 12, "form_8k": 25, "def_14a": 3, "total": 43, "chunks": 500, "word_count": 2500000}
        ]
        mock_document_repository.return_value = mock_doc_repo
        
        # Mock chunk repository
        mock_chunk_repo = Mock()
        mock_chunk_repo.get_total_chunks.return_value = 10000
        mock_chunk_repository.return_value = mock_chunk_repo
        
        response = client.get("/api/v1/documents/report/table")
        
        assert response.status_code == 200
        data = response.json()
        assert "summary_table" in data
        assert "company_table" in data


class TestDocumentManagementEndpoints:
    """Tests for document management endpoints"""
    
    def test_list_documents(self, client, mock_document_repository):
        """Test listing all documents"""
        mock_repo = Mock()
        mock_repo.get_all.return_value = [
            {"id": "doc-1", "ticker": "CAT", "filing_type": "10-K"},
            {"id": "doc-2", "ticker": "CAT", "filing_type": "10-Q"}
        ]
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents?limit=100")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
    
    def test_list_documents_by_ticker(self, client, mock_document_repository):
        """Test listing documents filtered by ticker"""
        mock_repo = Mock()
        mock_repo.get_by_ticker.return_value = [
            {"id": "doc-1", "ticker": "CAT", "filing_type": "10-K"}
        ]
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents?ticker=CAT")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
    
    def test_get_document_by_id(self, client, mock_document_repository):
        """Test getting document by ID"""
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = {
            "id": "doc-123",
            "ticker": "CAT",
            "filing_type": "10-K"
        }
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/doc-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "doc-123"
    
    def test_get_document_not_found(self, client, mock_document_repository):
        """Test getting non-existent document"""
        mock_repo = Mock()
        mock_repo.get_by_id.return_value = None
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/invalid-id")
        
        assert response.status_code == 404
    
    def test_get_document_stats(self, client, mock_document_repository):
        """Test getting document statistics for a company"""
        mock_repo = Mock()
        mock_repo.get_company_stats.return_value = {
            "ticker": "CAT",
            "form_10k": 3,
            "form_10q": 12,
            "total": 43
        }
        mock_document_repository.return_value = mock_repo
        
        response = client.get("/api/v1/documents/stats/CAT")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"


class TestResetEndpoints:
    """Tests for reset/delete endpoints"""
    
    def test_reset_company_data(self, client, mock_document_repository, mock_chunk_repository, mock_s3_service):
        """Test resetting all data for a company"""
        # Mock repositories
        mock_doc_repo = Mock()
        mock_doc_repo.delete_by_ticker.return_value = 10
        mock_document_repository.return_value = mock_doc_repo
        
        mock_chunk_repo = Mock()
        mock_chunk_repo.delete_by_ticker.return_value = 500
        mock_chunk_repository.return_value = mock_chunk_repo
        
        # Mock S3
        mock_s3 = Mock()
        mock_s3.s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "test"}]}
        mock_s3.s3_client.delete_objects.return_value = {}
        mock_s3.bucket_name = "test-bucket"
        mock_s3_service.return_value = mock_s3
        
        response = client.delete("/api/v1/documents/reset/CAT")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "CAT"
    
    def test_reset_raw_only(self, client, mock_s3_service):
        """Test deleting only raw files"""
        mock_s3 = Mock()
        mock_s3.s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "test"}]}
        mock_s3.s3_client.delete_objects.return_value = {}
        mock_s3.bucket_name = "test-bucket"
        mock_s3_service.return_value = mock_s3
        
        response = client.delete("/api/v1/documents/reset/CAT/raw")
        
        assert response.status_code == 200
        data = response.json()
        assert data["folder"] == "raw"


# ============================================================
# SECTION 3: UTILITY/HELPER TESTS
# ============================================================

class TestValidationHelpers:
    """Tests for validation and helper functions"""
    
    def test_ticker_uppercase(self):
        """Test that tickers are uppercase"""
        from app.models.document import DocumentCollectionRequest
        
        request = DocumentCollectionRequest(ticker="cat")
        # The API should handle uppercase conversion
        assert request.ticker == "cat"  # Model doesn't auto-uppercase
    
    def test_filing_type_with_space(self):
        """Test DEF 14A filing type (has space)"""
        from app.models.document import FilingType
        
        # DEF 14A has a space, not hyphen
        assert FilingType.DEF_14A.value == "DEF 14A"
        assert " " in FilingType.DEF_14A.value


# ============================================================
# RUN CONFIGURATION
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])