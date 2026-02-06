
-- Seed Data: Sample Documents for Pipeline 1
-- app/database/seed-documents.sql


USE WAREHOUSE PE_ORGAIR_WH;

USE DATABASE PE_ORGAIR_DB;

USE SCHEMA PLATFORM;


-- First, add the 10 target companies (if they don't exist)
-- Using MERGE to avoid duplicates


-- CAT - Caterpillar
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-cat-001', 'Caterpillar Inc.', 'CAT', '550e8400-e29b-41d4-a716-446655440002', 0.15
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'CAT');

-- DE - Deere
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-de-002', 'Deere & Company', 'DE', '550e8400-e29b-41d4-a716-446655440002', 0.12
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'DE');

-- UNH - UnitedHealth
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-unh-003', 'UnitedHealth Group', 'UNH', '550e8400-e29b-41d4-a716-446655440003', 0.20
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'UNH');

-- HCA - HCA Healthcare
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-hca-004', 'HCA Healthcare', 'HCA', '550e8400-e29b-41d4-a716-446655440003', 0.08
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'HCA');

-- ADP - Automatic Data Processing
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-adp-005', 'Automatic Data Processing', 'ADP', '550e8400-e29b-41d4-a716-446655440001', 0.25
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'ADP');

-- PAYX - Paychex
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-payx-006', 'Paychex Inc.', 'PAYX', '550e8400-e29b-41d4-a716-446655440001', 0.10
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'PAYX');

-- WMT - Walmart
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-wmt-007', 'Walmart Inc.', 'WMT', '550e8400-e29b-41d4-a716-446655440004', 0.30
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'WMT');

-- TGT - Target
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-tgt-008', 'Target Corporation', 'TGT', '550e8400-e29b-41d4-a716-446655440004', 0.18
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'TGT');

-- JPM - JPMorgan
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-jpm-009', 'JPMorgan Chase', 'JPM', '550e8400-e29b-41d4-a716-446655440005', 0.35
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'JPM');

-- GS - Goldman Sachs
INSERT INTO companies (id, name, ticker, industry_id, position_factor)
SELECT 'comp-gs-010', 'Goldman Sachs', 'GS', '550e8400-e29b-41d4-a716-446655440005', 0.28
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE ticker = 'GS');


-- SAMPLE DOCUMENTS


-- Caterpillar 10-K
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-cat-10k-2024', 'comp-cat-001', 'CAT', '10-K', '2024-02-15', '0000018230-24-000012',
    'https://www.sec.gov/Archives/edgar/data/18230/000001823024000012/cat-20231231.htm',
    'data/raw/sec/CAT/10-K/0000018230-24-000012.txt',
    'raw/sec/CAT/10-K/0000018230-24-000012.txt',
    'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef12345678',
    45230, 52, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-cat-10k-2024');

-- Caterpillar 10-Q
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-cat-10q-2024q1', 'comp-cat-001', 'CAT', '10-Q', '2024-05-02', '0000018230-24-000045',
    'https://www.sec.gov/Archives/edgar/data/18230/000001823024000045/cat-20240331.htm',
    'data/raw/sec/CAT/10-Q/0000018230-24-000045.txt',
    'raw/sec/CAT/10-Q/0000018230-24-000045.txt',
    'b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456789a',
    18500, 22, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-cat-10q-2024q1');

-- JPMorgan 10-K
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-jpm-10k-2024', 'comp-jpm-009', 'JPM', '10-K', '2024-02-20', '0000019617-24-000089',
    'https://www.sec.gov/Archives/edgar/data/19617/000001961724000089/jpm-20231231.htm',
    'data/raw/sec/JPM/10-K/0000019617-24-000089.txt',
    'raw/sec/JPM/10-K/0000019617-24-000089.txt',
    'c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456789ab2',
    125000, 145, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-jpm-10k-2024');

-- JPMorgan 8-K
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-jpm-8k-2024', 'comp-jpm-009', 'JPM', '8-K', '2024-03-15', '0000019617-24-000112',
    'https://www.sec.gov/Archives/edgar/data/19617/000001961724000112/jpm-8k-20240315.htm',
    'data/raw/sec/JPM/8-K/0000019617-24-000112.txt',
    'raw/sec/JPM/8-K/0000019617-24-000112.txt',
    'd4e5f6789012345678901234567890abcdef1234567890abcdef123456789ab2c3',
    3200, 5, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-jpm-8k-2024');

-- Walmart 10-K
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-wmt-10k-2024', 'comp-wmt-007', 'WMT', '10-K', '2024-03-28', '0000104169-24-000034',
    'https://www.sec.gov/Archives/edgar/data/104169/000010416924000034/wmt-20240131.htm',
    'data/raw/sec/WMT/10-K/0000104169-24-000034.txt',
    'raw/sec/WMT/10-K/0000104169-24-000034.txt',
    'e5f6789012345678901234567890abcdef1234567890abcdef123456789ab2c3d4',
    98500, 112, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-wmt-10k-2024');

-- UnitedHealth 10-K
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-unh-10k-2024', 'comp-unh-003', 'UNH', '10-K', '2024-02-22', '0000731766-24-000015',
    'https://www.sec.gov/Archives/edgar/data/731766/000073176624000015/unh-20231231.htm',
    'data/raw/sec/UNH/10-K/0000731766-24-000015.txt',
    'raw/sec/UNH/10-K/0000731766-24-000015.txt',
    'f6789012345678901234567890abcdef1234567890abcdef123456789ab2c3d4e5',
    78000, 89, 'indexed'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-unh-10k-2024');

-- Goldman Sachs 10-K (downloaded status - in progress)
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status)
SELECT 'doc-gs-10k-2024', 'comp-gs-010', 'GS', '10-K', '2024-02-23', '0000886982-24-000078',
    'https://www.sec.gov/Archives/edgar/data/886982/000088698224000078/gs-20231231.htm',
    'data/raw/sec/GS/10-K/0000886982-24-000078.txt',
    NULL, NULL, NULL, NULL, 'downloaded'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-gs-10k-2024');

-- Deere 10-K (failed status - error example)
INSERT INTO documents (id, company_id, ticker, filing_type, filing_date, accession_number, source_url, local_path, s3_key, content_hash, word_count, chunk_count, status, error_message)
SELECT 'doc-de-10k-2024', 'comp-de-002', 'DE', '10-K', '2024-12-15', '0000315189-24-000098',
    'https://www.sec.gov/Archives/edgar/data/315189/000031518924000098/de-20241031.htm',
    'data/raw/sec/DE/10-K/0000315189-24-000098.txt',
    NULL, NULL, NULL, NULL, 'failed', 'Connection timeout during download'
WHERE NOT EXISTS (SELECT 1 FROM documents WHERE id = 'doc-de-10k-2024');


-- SAMPLE DOCUMENT CHUNKS (for Caterpillar 10-K)


-- Chunk 0: Item 1 - Business
INSERT INTO document_chunks (id, document_id, chunk_index, section, content, start_char, end_char, word_count)
SELECT 'chunk-cat-10k-001', 'doc-cat-10k-2024', 0, 'item_1',
    'Item 1. Business. Caterpillar Inc. is the worlds leading manufacturer of construction and mining equipment, off-highway diesel and natural gas engines, industrial gas turbines, and diesel-electric locomotives. The company principally operates through three primary segments: Construction Industries, Resource Industries, and Energy and Transportation.',
    0, 350, 55
WHERE NOT EXISTS (SELECT 1 FROM document_chunks WHERE id = 'chunk-cat-10k-001');

-- Chunk 1: Item 1 continued - AI investments
INSERT INTO document_chunks (id, document_id, chunk_index, section, content, start_char, end_char, word_count)
SELECT 'chunk-cat-10k-002', 'doc-cat-10k-2024', 1, 'item_1',
    'We are investing significantly in artificial intelligence and machine learning technologies to enhance our autonomous and semi-autonomous machine offerings. Our autonomous mining trucks and drilling systems leverage advanced AI algorithms for navigation, obstacle detection, and operational optimization.',
    351, 650, 45
WHERE NOT EXISTS (SELECT 1 FROM document_chunks WHERE id = 'chunk-cat-10k-002');

-- Chunk 2: Item 1A - Risk Factors
INSERT INTO document_chunks (id, document_id, chunk_index, section, content, start_char, end_char, word_count)
SELECT 'chunk-cat-10k-003', 'doc-cat-10k-2024', 2, 'item_1a',
    'Item 1A. Risk Factors. Our business involves various risks and uncertainties, including risks related to our technology investments. The development and deployment of artificial intelligence and autonomous technologies involve substantial uncertainties.',
    651, 900, 40
WHERE NOT EXISTS (SELECT 1 FROM document_chunks WHERE id = 'chunk-cat-10k-003');

-- Chunk 3: Item 7 - MD&A
INSERT INTO document_chunks (id, document_id, chunk_index, section, content, start_char, end_char, word_count)
SELECT 'chunk-cat-10k-004', 'doc-cat-10k-2024', 3, 'item_7',
    'Item 7. Managements Discussion and Analysis. During fiscal year 2023, we continued to advance our digital and technology initiatives. Our investments in AI-powered analytics and predictive maintenance solutions have contributed to improved customer outcomes.',
    901, 1150, 42