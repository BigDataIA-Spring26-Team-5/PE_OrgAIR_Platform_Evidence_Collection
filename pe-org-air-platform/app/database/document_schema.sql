-- -- =============================================================================
-- -- Pipeline 1: SEC EDGAR Document Collection Schema
-- -- app/database/document_schema.sql
-- -- =============================================================================

-- USE WAREHOUSE PE_ORGAIR_WH;

-- USE DATABASE PE_ORGAIR_DB;

-- USE SCHEMA PLATFORM;

-- -- Drop existing tables if they exist (in correct order)
-- DROP TABLE IF EXISTS document_chunks;

-- DROP TABLE IF EXISTS documents;

-- -- DOCUMENTS TABLE
-- CREATE TABLE documents (
--     id VARCHAR(36) PRIMARY KEY,
--     company_id VARCHAR(36) NOT NULL,
--     ticker VARCHAR(10) NOT NULL,
--     filing_type VARCHAR(20) NOT NULL,
--     filing_date DATE,
--     accession_number VARCHAR(30),
--     source_url VARCHAR(500),
--     local_path VARCHAR(500),
--     s3_key VARCHAR(500),
--     content_hash VARCHAR(64),
--     word_count INT,
--     chunk_count INT,
--     status VARCHAR(20) DEFAULT 'pending',
--     error_message VARCHAR(1000),
--     created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
--     processed_at TIMESTAMP_NTZ,
--     UNIQUE (ticker, filing_type, accession_number)
-- );

-- -- DOCUMENT_CHUNKS TABLE
-- CREATE TABLE document_chunks (
--     id VARCHAR(36) PRIMARY KEY,
--     document_id VARCHAR(36) NOT NULL,
--     chunk_index INT NOT NULL,
--     section VARCHAR(50),
--     content TEXT NOT NULL,
--     start_char INT,
--     end_char INT,
--     word_count INT,
--     created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
--     UNIQUE (document_id, chunk_index)
-- );


-- =============================================================================
-- Pipeline 1: SEC EDGAR Document Collection Schema
-- app/database/document_schema.sql
-- Case Study 2 - Page 20
-- =============================================================================

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

-- Drop existing tables (in correct order due to FK constraints)
DROP TABLE IF EXISTS document_chunks;
DROP TABLE IF EXISTS documents;

-- =============================================================================
-- DOCUMENTS TABLE
-- Stores metadata for each SEC filing downloaded
-- Note: CHECK constraint not supported in Snowflake - validation done in app
-- Valid status values: pending, downloaded, parsed, chunked, indexed, failed
-- =============================================================================

CREATE TABLE IF NOT EXISTS documents (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL REFERENCES companies(id),
    ticker VARCHAR(10) NOT NULL,
    filing_type VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    source_url VARCHAR(500),
    local_path VARCHAR(500),
    s3_key VARCHAR(500),
    content_hash VARCHAR(64),
    word_count INT,
    chunk_count INT,
    status VARCHAR(20) DEFAULT 'pending',
    error_message VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP_NTZ
);

-- =============================================================================
-- DOCUMENT CHUNKS TABLE
-- Stores individual chunks of parsed documents for LLM processing
-- =============================================================================

CREATE TABLE IF NOT EXISTS document_chunks (
    id VARCHAR(36) PRIMARY KEY,
    document_id VARCHAR(36) NOT NULL REFERENCES documents(id),
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    section VARCHAR(50),
    start_char INT,
    end_char INT,
    word_count INT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (document_id, chunk_index)
);