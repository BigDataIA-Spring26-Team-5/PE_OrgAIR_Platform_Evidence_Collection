
-- INSERT DIMENSION SCORES DATA USING STORED PROCEDURE

-- Uses insert_dimension_score() procedure for validation:
-- dimension: data_infrastructure, ai_governance, technology_stack,
--            talent_skills, leadership_vision, use_case_portfolio, culture_change
-- score: between 0 and 100
-- weight: between 0 and 1
-- confidence: between 0 and 1
-- Requires assessments to be seeded first (run seed-assessments.sql)


-- Dimension scores for Apex Manufacturing Screening (b1000000-0000-0000-0000-000000000001)
-- Complete set of all 7 dimensions
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000001', 'b1000000-0000-0000-0000-000000000001', 'data_infrastructure', 70.0, 0.25, 0.85, 5);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000002', 'b1000000-0000-0000-0000-000000000001', 'ai_governance', 65.0, 0.20, 0.80, 4);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000003', 'b1000000-0000-0000-0000-000000000001', 'technology_stack', 75.0, 0.15, 0.90, 6);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000004', 'b1000000-0000-0000-0000-000000000001', 'talent_skills', 72.0, 0.15, 0.82, 3);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000005', 'b1000000-0000-0000-0000-000000000001', 'leadership_vision', 78.0, 0.10, 0.88, 4);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000006', 'b1000000-0000-0000-0000-000000000001', 'use_case_portfolio', 68.0, 0.10, 0.75, 5);
CALL insert_dimension_score('c1000000-0000-0000-0000-000000000007', 'b1000000-0000-0000-0000-000000000001', 'culture_change', 80.0, 0.05, 0.78, 2);

-- Dimension scores for MedTech Solutions Screening (b2000000-0000-0000-0000-000000000001)
-- Complete set of all 7 dimensions
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000001', 'b2000000-0000-0000-0000-000000000001', 'data_infrastructure', 88.0, 0.25, 0.92, 8);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000002', 'b2000000-0000-0000-0000-000000000001', 'ai_governance', 82.0, 0.20, 0.88, 6);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000003', 'b2000000-0000-0000-0000-000000000001', 'technology_stack', 90.0, 0.15, 0.95, 7);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000004', 'b2000000-0000-0000-0000-000000000001', 'talent_skills', 85.0, 0.15, 0.90, 5);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000005', 'b2000000-0000-0000-0000-000000000001', 'leadership_vision', 80.0, 0.10, 0.85, 4);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000006', 'b2000000-0000-0000-0000-000000000001', 'use_case_portfolio', 78.0, 0.10, 0.82, 6);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000007', 'b2000000-0000-0000-0000-000000000001', 'culture_change', 75.0, 0.05, 0.80, 3);

-- Dimension scores for MedTech Solutions Due Diligence (b2000000-0000-0000-0000-000000000002)
-- Partial set (assessment in progress)
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000008', 'b2000000-0000-0000-0000-000000000002', 'data_infrastructure', 85.0, 0.25, 0.90, 10);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000009', 'b2000000-0000-0000-0000-000000000002', 'ai_governance', 80.0, 0.20, 0.85, 8);
CALL insert_dimension_score('c2000000-0000-0000-0000-000000000010', 'b2000000-0000-0000-0000-000000000002', 'technology_stack', 88.0, 0.15, 0.92, 9);

-- Dimension scores for Strategic Consulting Screening (b3000000-0000-0000-0000-000000000001)
-- Complete set
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000001', 'b3000000-0000-0000-0000-000000000001', 'data_infrastructure', 72.0, 0.25, 0.82, 4);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000002', 'b3000000-0000-0000-0000-000000000001', 'ai_governance', 78.0, 0.20, 0.85, 5);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000003', 'b3000000-0000-0000-0000-000000000001', 'technology_stack', 80.0, 0.15, 0.88, 6);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000004', 'b3000000-0000-0000-0000-000000000001', 'talent_skills', 85.0, 0.15, 0.90, 7);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000005', 'b3000000-0000-0000-0000-000000000001', 'leadership_vision', 82.0, 0.10, 0.88, 5);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000006', 'b3000000-0000-0000-0000-000000000001', 'use_case_portfolio', 70.0, 0.10, 0.78, 4);
CALL insert_dimension_score('c3000000-0000-0000-0000-000000000007', 'b3000000-0000-0000-0000-000000000001', 'culture_change', 75.0, 0.05, 0.80, 3);

-- Dimension scores for Capital Ventures Screening (b5000000-0000-0000-0000-000000000001)
-- Complete set with high scores (financial services)
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000001', 'b5000000-0000-0000-0000-000000000001', 'data_infrastructure', 92.0, 0.25, 0.95, 12);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000002', 'b5000000-0000-0000-0000-000000000001', 'ai_governance', 88.0, 0.20, 0.92, 10);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000003', 'b5000000-0000-0000-0000-000000000001', 'technology_stack', 90.0, 0.15, 0.93, 9);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000004', 'b5000000-0000-0000-0000-000000000001', 'talent_skills', 85.0, 0.15, 0.88, 7);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000005', 'b5000000-0000-0000-0000-000000000001', 'leadership_vision', 90.0, 0.10, 0.92, 6);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000006', 'b5000000-0000-0000-0000-000000000001', 'use_case_portfolio', 82.0, 0.10, 0.85, 8);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000007', 'b5000000-0000-0000-0000-000000000001', 'culture_change', 78.0, 0.05, 0.82, 4);

-- Dimension scores for Capital Ventures Due Diligence (b5000000-0000-0000-0000-000000000002)
-- Complete set
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000008', 'b5000000-0000-0000-0000-000000000002', 'data_infrastructure', 94.0, 0.25, 0.96, 15);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000009', 'b5000000-0000-0000-0000-000000000002', 'ai_governance', 90.0, 0.20, 0.94, 12);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000010', 'b5000000-0000-0000-0000-000000000002', 'technology_stack', 92.0, 0.15, 0.95, 11);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000011', 'b5000000-0000-0000-0000-000000000002', 'talent_skills', 88.0, 0.15, 0.90, 9);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000012', 'b5000000-0000-0000-0000-000000000002', 'leadership_vision', 92.0, 0.10, 0.94, 8);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000013', 'b5000000-0000-0000-0000-000000000002', 'use_case_portfolio', 85.0, 0.10, 0.88, 10);
CALL insert_dimension_score('c5000000-0000-0000-0000-000000000014', 'b5000000-0000-0000-0000-000000000002', 'culture_change', 80.0, 0.05, 0.85, 5);

-- Verify inserted data
SELECT
    ds.id,
    ds.assessment_id,
    a.assessment_type,
    c.name as company_name,
    ds.dimension,
    ds.score,
    ds.weight,
    ds.confidence,
    ds.evidence_count
FROM DIMENSION_SCORES ds
JOIN ASSESSMENTS a ON ds.assessment_id = a.id
JOIN COMPANIES c ON a.company_id = c.id
ORDER BY c.name, a.assessment_date, ds.dimension;
