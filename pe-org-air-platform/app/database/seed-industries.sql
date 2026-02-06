
-- INSERT INDUSTRIES DATA USING STORED PROCEDURE

-- Uses insert_industry() procedure for validation:
-- h_r_base must be between 0 and 100


CALL insert_industry('550e8400-e29b-41d4-a716-446655440001', 'Manufacturing', 'Industrials', 72);
CALL insert_industry('550e8400-e29b-41d4-a716-446655440002', 'Healthcare Services', 'Healthcare', 78);
CALL insert_industry('550e8400-e29b-41d4-a716-446655440003', 'Business Services', 'Services', 75);
CALL insert_industry('550e8400-e29b-41d4-a716-446655440004', 'Retail', 'Consumer', 70);
CALL insert_industry('550e8400-e29b-41d4-a716-446655440005', 'Financial Services', 'Financial', 80);

SELECT * FROM INDUSTRIES;