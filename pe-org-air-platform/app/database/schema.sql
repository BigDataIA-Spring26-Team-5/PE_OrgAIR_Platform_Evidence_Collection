CREATE WAREHOUSE IF NOT EXISTS PE_ORGAIR_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS PE_ORGAIR_DB;


CREATE SCHEMA IF NOT EXISTS PE_ORGAIR_DB.PLATFORM;

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

USE WAREHOUSE PE_ORGAIR_WH;
USE DATABASE PE_ORGAIR_DB;
USE SCHEMA PLATFORM;

CREATE TABLE IF NOT EXISTS industries (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    sector VARCHAR(100) NOT NULL,
    h_r_base DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS companies (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10),
    industry_id VARCHAR(36),
    position_factor DECIMAL(4,3) DEFAULT 0.0,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (industry_id) REFERENCES industries(id)
);

CREATE TABLE IF NOT EXISTS assessments (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    assessment_type VARCHAR(20) NOT NULL,
    assessment_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'draft',
    primary_assessor VARCHAR(255),
    secondary_assessor VARCHAR(255),
    v_r_score DECIMAL(5,2),
    confidence_lower DECIMAL(5,2),
    confidence_upper DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (company_id) REFERENCES companies(id)
);


CREATE TABLE IF NOT EXISTS dimension_scores (
    id VARCHAR(36) PRIMARY KEY,
    assessment_id VARCHAR(36) NOT NULL,
    dimension VARCHAR(30) NOT NULL,
    score DECIMAL(5,2) NOT NULL,
    weight DECIMAL(4,3),
    confidence DECIMAL(4,3) DEFAULT 0.8,
    evidence_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (assessment_id) REFERENCES assessments(id),
    UNIQUE (assessment_id, dimension)
);

-- =============================================================================
-- PE Org-AI-R Platform Foundation - Stored Procedures (Snowflake)
-- =============================================================================
-- All procedures with validation (replacing CHECK constraints)
-- =============================================================================


-- =============================================================================
-- INSERT INDUSTRY
-- Validates: h_r_base between 0 and 100
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_industry(
    p_id VARCHAR,
    p_name VARCHAR,
    p_sector VARCHAR,
    p_h_r_base DECIMAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_h_r_base IS NOT NULL AND (:p_h_r_base < 0 OR :p_h_r_base > 100)) THEN
        RETURN 'ERROR: h_r_base must be between 0 and 100';
    END IF;
    
    INSERT INTO industries (id, name, sector, h_r_base)
    VALUES (:p_id, :p_name, :p_sector, :p_h_r_base);
    
    RETURN 'SUCCESS: Industry inserted';
END;
$$;


-- =============================================================================
-- INSERT COMPANY
-- Validates: position_factor between -1.0 and 1.0
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_company(
    p_name VARCHAR,
    p_ticker VARCHAR,
    p_industry_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_id VARCHAR;
BEGIN
    v_id := UUID_STRING();

    INSERT INTO companies (
        id,
        name,
        ticker,
        industry_id
    )
    VALUES (
        :v_id,
        :p_name,
        :p_ticker,
        :p_industry_id
    );

    RETURN 'SUCCESS: Company inserted with id = ' || v_id;
END;
$$;

-- =============================================================================
-- INSERT ASSESSMENT
-- Validates: 
--   - assessment_type: screening, due_diligence, quarterly, exit_prep
--   - status: draft, in_progress, submitted, approved, superseded
--   - v_r_score: between 0 and 100
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_assessment(
    p_id VARCHAR,
    p_company_id VARCHAR,
    p_assessment_type VARCHAR,
    p_assessment_date DATE,
    p_status VARCHAR,
    p_primary_assessor VARCHAR,
    p_secondary_assessor VARCHAR,
    p_v_r_score DECIMAL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_assessment_type NOT IN ('screening', 'due_diligence', 'quarterly', 'exit_prep')) THEN
        RETURN 'ERROR: assessment_type must be one of: screening, due_diligence, quarterly, exit_prep';
    END IF;
    
    IF (:p_status NOT IN ('draft', 'in_progress', 'submitted', 'approved', 'superseded')) THEN
        RETURN 'ERROR: status must be one of: draft, in_progress, submitted, approved, superseded';
    END IF;
    
    IF (:p_v_r_score IS NOT NULL AND (:p_v_r_score < 0 OR :p_v_r_score > 100)) THEN
        RETURN 'ERROR: v_r_score must be between 0 and 100';
    END IF;
    
    INSERT INTO assessments (id, company_id, assessment_type, assessment_date, status, primary_assessor, secondary_assessor, v_r_score)
    VALUES (:p_id, :p_company_id, :p_assessment_type, :p_assessment_date, :p_status, :p_primary_assessor, :p_secondary_assessor, :p_v_r_score);
    
    RETURN 'SUCCESS: Assessment inserted';
END;
$$;


-- =============================================================================
-- INSERT DIMENSION SCORE
-- Validates:
--   - dimension: data_infrastructure, ai_governance, technology_stack,
--                talent_skills, leadership_vision, use_case_portfolio, culture_change
--   - score: between 0 and 100
--   - weight: between 0 and 1
--   - confidence: between 0 and 1
-- =============================================================================

CREATE OR REPLACE PROCEDURE insert_dimension_score(
    p_id VARCHAR,
    p_assessment_id VARCHAR,
    p_dimension VARCHAR,
    p_score DECIMAL,
    p_weight DECIMAL,
    p_confidence DECIMAL,
    p_evidence_count INT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_dimension NOT IN ('data_infrastructure', 'ai_governance', 'technology_stack', 'talent_skills', 'leadership_vision', 'use_case_portfolio', 'culture_change')) THEN
        RETURN 'ERROR: dimension must be one of: data_infrastructure, ai_governance, technology_stack, talent_skills, leadership_vision, use_case_portfolio, culture_change';
    END IF;
    
    IF (:p_score < 0 OR :p_score > 100) THEN
        RETURN 'ERROR: score must be between 0 and 100';
    END IF;
    
    IF (:p_weight IS NOT NULL AND (:p_weight < 0 OR :p_weight > 1)) THEN
        RETURN 'ERROR: weight must be between 0 and 1';
    END IF;
    
    IF (:p_confidence IS NOT NULL AND (:p_confidence < 0 OR :p_confidence > 1)) THEN
        RETURN 'ERROR: confidence must be between 0 and 1';
    END IF;
    
    INSERT INTO dimension_scores (id, assessment_id, dimension, score, weight, confidence, evidence_count)
    VALUES (:p_id, :p_assessment_id, :p_dimension, :p_score, :p_weight, :p_confidence, :p_evidence_count);
    
    RETURN 'SUCCESS: Dimension score inserted';
END;
$$;


-- =============================================================================
-- UPDATE DIMENSION SCORE
-- Validates same as INSERT (for non-null values)
-- =============================================================================

CREATE OR REPLACE PROCEDURE update_dimension_score(
    p_id VARCHAR,
    p_dimension VARCHAR,
    p_score DECIMAL,
    p_weight DECIMAL,
    p_confidence DECIMAL,
    p_evidence_count INT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    IF (:p_dimension IS NOT NULL AND :p_dimension NOT IN ('data_infrastructure', 'ai_governance', 'technology_stack', 'talent_skills', 'leadership_vision', 'use_case_portfolio', 'culture_change')) THEN
        RETURN 'ERROR: dimension must be one of: data_infrastructure, ai_governance, technology_stack, talent_skills, leadership_vision, use_case_portfolio, culture_change';
    END IF;
    
    IF (:p_score IS NOT NULL AND (:p_score < 0 OR :p_score > 100)) THEN
        RETURN 'ERROR: score must be between 0 and 100';
    END IF;
    
    IF (:p_weight IS NOT NULL AND (:p_weight < 0 OR :p_weight > 1)) THEN
        RETURN 'ERROR: weight must be between 0 and 1';
    END IF;
    
    IF (:p_confidence IS NOT NULL AND (:p_confidence < 0 OR :p_confidence > 1)) THEN
        RETURN 'ERROR: confidence must be between 0 and 1';
    END IF;
    
    UPDATE dimension_scores
    SET 
        dimension = COALESCE(:p_dimension, dimension),
        score = COALESCE(:p_score, score),
        weight = COALESCE(:p_weight, weight),
        confidence = COALESCE(:p_confidence, confidence),
        evidence_count = COALESCE(:p_evidence_count, evidence_count)
    WHERE id = :p_id;
    
    RETURN 'SUCCESS: Dimension score updated';
END;
$$;

-- =============================================================================
-- SIGNAL SCORE SCHEMA
-- Stores company signal scores with upsert strategy:
-- - If ticker exists: replace the row
-- - If ticker is new: insert new row
-- =============================================================================

CREATE TABLE IF NOT EXISTS SIGNAL_SCORES (
    -- Primary identifiers
    ticker VARCHAR(20) NOT NULL,               -- Upsert key (PRIMARY KEY)
    company_id VARCHAR(36) NOT NULL,           -- UUID from companies table
    company_name VARCHAR(255) NOT NULL,

    -- Signal Scores (0-100 scale)
    hiring_score FLOAT,                        -- Job market/hiring signal
    innovation_score FLOAT,                    -- Patent/innovation signal
    tech_stack_score FLOAT,                    -- Tech stack signal
    leadership_score FLOAT,                    -- Leadership signal (NULL for now)
    composite_score FLOAT,                     -- Weighted average of available scores

    -- Metrics
    total_jobs INTEGER DEFAULT 0,
    ai_jobs INTEGER DEFAULT 0,
    total_patents INTEGER DEFAULT 0,
    ai_patents INTEGER DEFAULT 0,

    -- Tech stack keywords (stored as JSON array)
    techstack_keywords VARIANT,

    -- S3 references
    s3_jobs_key VARCHAR(500),
    s3_patents_key VARCHAR(500),

    -- Timestamps
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Primary key constraint
    PRIMARY KEY (ticker)
);

COMMENT ON TABLE SIGNAL_SCORES IS 'Company signal scores from job market, patents, and tech stack analysis. Upsert by ticker.';
COMMENT ON COLUMN SIGNAL_SCORES.ticker IS 'Company ticker symbol (upsert key)';
COMMENT ON COLUMN SIGNAL_SCORES.hiring_score IS 'Job market/hiring signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.innovation_score IS 'Patent/innovation signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.tech_stack_score IS 'Tech stack signal score (0-100)';
COMMENT ON COLUMN SIGNAL_SCORES.leadership_score IS 'Leadership signal score (0-100) - currently unused';
COMMENT ON COLUMN SIGNAL_SCORES.composite_score IS 'Average of available scores';

