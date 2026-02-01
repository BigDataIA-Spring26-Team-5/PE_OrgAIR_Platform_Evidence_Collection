-- =============================================================================
-- INSERT COMPANIES DATA USING STORED PROCEDURE
-- =============================================================================
-- Uses insert_company() procedure for validation:
-- position_factor must be between -1.0 and 1.0
-- Requires industries to be seeded first (run seed-industries.sql)
-- =============================================================================

-- Manufacturing Companies (industry: 550e8400-e29b-41d4-a716-446655440001)
CALL insert_company('a1000000-0000-0000-0000-000000000001', 'Caterpillar Inc.', 'CAT', '550e8400-e29b-41d4-a716-446655440001', 0.00);
CALL insert_company('a1000000-0000-0000-0000-000000000002', 'Deere & Company', 'DE', '550e8400-e29b-41d4-a716-446655440001', 0.00);

-- Healthcare Services Companies (industry: 550e8400-e29b-41d4-a716-446655440002)
CALL insert_company('a2000000-0000-0000-0000-000000000001', 'UnitedHealth Group', 'UNH', '550e8400-e29b-41d4-a716-446655440002', 0.00);
CALL insert_company('a2000000-0000-0000-0000-000000000002', 'HCA Healthcare', 'HCA', '550e8400-e29b-41d4-a716-446655440002', 0.00);

-- Business Services Companies (industry: 550e8400-e29b-41d4-a716-446655440003)
CALL insert_company('a3000000-0000-0000-0000-000000000001', 'Automatic Data Processing', 'ADP', '550e8400-e29b-41d4-a716-446655440003', 0.00);
CALL insert_company('a3000000-0000-0000-0000-000000000002', 'Paychex Inc.', 'PAYX', '550e8400-e29b-41d4-a716-446655440003', 0.00);

-- Retail Companies (industry: 550e8400-e29b-41d4-a716-446655440004)
CALL insert_company('a4000000-0000-0000-0000-000000000001', 'Walmart Inc.', 'WMT', '550e8400-e29b-41d4-a716-446655440004', 0.00);
CALL insert_company('a4000000-0000-0000-0000-000000000002', 'Target Corporation', 'TGT', '550e8400-e29b-41d4-a716-446655440004', 0.00);

-- Financial Services Companies (industry: 550e8400-e29b-41d4-a716-446655440005)
CALL insert_company('a5000000-0000-0000-0000-000000000001', 'JPMorgan Chase', 'JPM', '550e8400-e29b-41d4-a716-446655440005', 0.00);
CALL insert_company('a5000000-0000-0000-0000-000000000002', 'Goldman Sachs', 'GS', '550e8400-e29b-41d4-a716-446655440005', 0.00);

-- Verify inserted data
SELECT * FROM COMPANIES ORDER BY industry_id, name;
