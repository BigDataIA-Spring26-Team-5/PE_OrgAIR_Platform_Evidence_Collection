-- =============================================================================
-- INSERT COMPANIES DATA USING STORED PROCEDURE
-- =============================================================================
-- Uses insert_company() procedure for validation:
-- position_factor must be between -1.0 and 1.0
-- Requires industries to be seeded first (run seed-industries.sql)
-- =============================================================================

-- Manufacturing Companies (industry: 550e8400-e29b-41d4-a716-446655440001)
CALL insert_company('a1000000-0000-0000-0000-000000000001', 'Apex Manufacturing Inc', 'APEX', '550e8400-e29b-41d4-a716-446655440001', 0.25);
CALL insert_company('a1000000-0000-0000-0000-000000000002', 'Precision Parts Corp', 'PPC', '550e8400-e29b-41d4-a716-446655440001', -0.15);
CALL insert_company('a1000000-0000-0000-0000-000000000003', 'Industrial Dynamics LLC', NULL, '550e8400-e29b-41d4-a716-446655440001', 0.50);

-- Healthcare Services Companies (industry: 550e8400-e29b-41d4-a716-446655440002)
CALL insert_company('a2000000-0000-0000-0000-000000000001', 'MedTech Solutions', 'MDTK', '550e8400-e29b-41d4-a716-446655440002', 0.75);
CALL insert_company('a2000000-0000-0000-0000-000000000002', 'HealthFirst Partners', 'HFP', '550e8400-e29b-41d4-a716-446655440002', 0.10);

-- Business Services Companies (industry: 550e8400-e29b-41d4-a716-446655440003)
CALL insert_company('a3000000-0000-0000-0000-000000000001', 'Strategic Consulting Group', 'SCG', '550e8400-e29b-41d4-a716-446655440003', 0.35);
CALL insert_company('a3000000-0000-0000-0000-000000000002', 'Enterprise Solutions Inc', 'ESI', '550e8400-e29b-41d4-a716-446655440003', -0.20);
CALL insert_company('a3000000-0000-0000-0000-000000000003', 'Global Staffing Partners', 'GSP', '550e8400-e29b-41d4-a716-446655440003', 0.00);

-- Retail Companies (industry: 550e8400-e29b-41d4-a716-446655440004)
CALL insert_company('a4000000-0000-0000-0000-000000000001', 'Urban Retail Brands', 'URB', '550e8400-e29b-41d4-a716-446655440004', -0.30);
CALL insert_company('a4000000-0000-0000-0000-000000000002', 'NextGen Commerce', 'NGC', '550e8400-e29b-41d4-a716-446655440004', 0.60);

-- Financial Services Companies (industry: 550e8400-e29b-41d4-a716-446655440005)
CALL insert_company('a5000000-0000-0000-0000-000000000001', 'Capital Ventures LLC', 'CVLL', '550e8400-e29b-41d4-a716-446655440005', 0.45);
CALL insert_company('a5000000-0000-0000-0000-000000000002', 'Fintech Innovations', 'FINI', '550e8400-e29b-41d4-a716-446655440005', 0.80);

-- Verify inserted data
SELECT * FROM COMPANIES ORDER BY industry_id, name;
