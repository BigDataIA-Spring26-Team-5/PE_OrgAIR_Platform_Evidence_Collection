USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;
DROP TABLE IF EXISTS document_chunks;
DROP TABLE IF EXISTS documents;

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