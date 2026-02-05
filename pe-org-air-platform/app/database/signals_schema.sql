-- Signals Schema for External Evidence

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

-- External signals table (individual signal observations)
CREATE TABLE IF NOT EXISTS external_signals (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL REFERENCES companies(id),
    category VARCHAR(30) NOT NULL
        CHECK (category IN ('technology_hiring', 'innovation_activity', 
                           'digital_presence', 'leadership_signals')),
    source VARCHAR(30) NOT NULL,
    signal_date DATE NOT NULL,
    raw_value VARCHAR(500),
    normalized_score DECIMAL(5,2) CHECK (normalized_score BETWEEN 0 AND 100),
    confidence DECIMAL(4,3) CHECK (confidence BETWEEN 0 AND 1),
    metadata VARIANT,  -- Snowflake JSON type
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Company signal summary (aggregated scores per company)
CREATE TABLE IF NOT EXISTS company_signal_summaries (
    company_id VARCHAR(36) PRIMARY KEY REFERENCES companies(id),
    ticker VARCHAR(10) NOT NULL,
    technology_hiring_score DECIMAL(5,2),
    innovation_activity_score DECIMAL(5,2),
    digital_presence_score DECIMAL(5,2),
    leadership_signals_score DECIMAL(5,2),
    composite_score DECIMAL(5,2),
    signal_count INT DEFAULT 0,
    last_updated TIMESTAMP_NTZ
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_signals_company ON external_signals(company_id);
CREATE INDEX IF NOT EXISTS idx_signals_category ON external_signals(category);
CREATE INDEX IF NOT EXISTS idx_signals_source ON external_signals(source);
CREATE INDEX IF NOT EXISTS idx_summaries_ticker ON company_signal_summaries(ticker);