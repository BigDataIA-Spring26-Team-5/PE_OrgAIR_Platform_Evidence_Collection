-- Run this to update document_chunks table (metadata only, content in S3)
USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

-- Drop and recreate the table
DROP TABLE IF EXISTS document_chunks;

CREATE TABLE document_chunks (
    id VARCHAR(36) PRIMARY KEY,
    document_id VARCHAR(36) NOT NULL REFERENCES documents(id),
    chunk_index INT NOT NULL,
    section VARCHAR(100),          -- Section name (e.g., 'item_1a_risk_factors')
    start_char INT,                -- Start position in original document
    end_char INT,                  -- End position in original document
    word_count INT,                -- Words in this chunk
    s3_key VARCHAR(500),           -- S3 path where chunk content is stored
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (document_id, chunk_index)

);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON document_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_chunks_section ON document_chunks(section);