-- =============================================================================
-- Pipeline 1: SEC EDGAR Document Collection Schema
-- app/database/document_schema.sql
-- =============================================================================

USE WAREHOUSE PE_ORGAIR_WH;

USE DATABASE PE_ORGAIR_DB;

USE SCHEMA PLATFORM;

-- Drop existing tables if they exist (in correct order)
DROP TABLE IF EXISTS document_chunks;

DROP TABLE IF EXISTS documents;

-- DOCUMENTS TABLE
CREATE TABLE documents (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    filing_type VARCHAR(20) NOT NULL,
    filing_date DATE,
    accession_number VARCHAR(30),
    source_url VARCHAR(500),
    local_path VARCHAR(500),
    s3_key VARCHAR(500),
    content_hash VARCHAR(64),
    word_count INT,
    chunk_count INT,
    status VARCHAR(20) DEFAULT 'pending',
    error_message VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP_NTZ,
    UNIQUE (ticker, filing_type, accession_number)
);

-- DOCUMENT_CHUNKS TABLE
CREATE TABLE document_chunks (
    id VARCHAR(36) PRIMARY KEY,
    document_id VARCHAR(36) NOT NULL,
    chunk_index INT NOT NULL,
    section VARCHAR(50),
    content TEXT NOT NULL,
    start_char INT,
    end_char INT,
    word_count INT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (document_id, chunk_index)
);