
-- INSERT ASSESSMENTS DATA USING STORED PROCEDURE

-- Uses insert_assessment() procedure for validation:
-- assessment_type: screening, due_diligence, quarterly, exit_prep
-- status: draft, in_progress, submitted, approved, superseded
-- v_r_score: between 0 and 100
-- Requires companies to be seeded first (run seed-companies.sql)


-- Assessments for Apex Manufacturing Inc (a1000000-0000-0000-0000-000000000001)
CALL insert_assessment('b1000000-0000-0000-0000-000000000001', 'a1000000-0000-0000-0000-000000000001', 'screening', '2025-01-15', 'approved', 'John Smith', 'Jane Doe', 72.5);
CALL insert_assessment('b1000000-0000-0000-0000-000000000002', 'a1000000-0000-0000-0000-000000000001', 'due_diligence', '2025-02-20', 'in_progress', 'John Smith', NULL, NULL);

-- Assessments for MedTech Solutions (a2000000-0000-0000-0000-000000000001)
CALL insert_assessment('b2000000-0000-0000-0000-000000000001', 'a2000000-0000-0000-0000-000000000001', 'screening', '2025-01-10', 'approved', 'Sarah Johnson', 'Mike Brown', 85.0);
CALL insert_assessment('b2000000-0000-0000-0000-000000000002', 'a2000000-0000-0000-0000-000000000001', 'due_diligence', '2025-02-15', 'submitted', 'Sarah Johnson', 'Emily Chen', 82.5);
CALL insert_assessment('b2000000-0000-0000-0000-000000000003', 'a2000000-0000-0000-0000-000000000001', 'quarterly', '2025-03-31', 'draft', 'Mike Brown', NULL, NULL);

-- Assessments for Strategic Consulting Group (a3000000-0000-0000-0000-000000000001)
CALL insert_assessment('b3000000-0000-0000-0000-000000000001', 'a3000000-0000-0000-0000-000000000001', 'screening', '2025-01-05', 'approved', 'David Lee', 'Anna Wilson', 78.0);

-- Assessments for Urban Retail Brands (a4000000-0000-0000-0000-000000000001)
CALL insert_assessment('b4000000-0000-0000-0000-000000000001', 'a4000000-0000-0000-0000-000000000001', 'screening', '2025-02-01', 'submitted', 'Tom Garcia', 'Lisa Park', 65.5);

-- Assessments for Capital Ventures LLC (a5000000-0000-0000-0000-000000000001)
CALL insert_assessment('b5000000-0000-0000-0000-000000000001', 'a5000000-0000-0000-0000-000000000001', 'screening', '2025-01-20', 'approved', 'Robert Chen', 'Maria Santos', 88.0);
CALL insert_assessment('b5000000-0000-0000-0000-000000000002', 'a5000000-0000-0000-0000-000000000001', 'due_diligence', '2025-03-01', 'approved', 'Robert Chen', 'James Kim', 90.5);
CALL insert_assessment('b5000000-0000-0000-0000-000000000003', 'a5000000-0000-0000-0000-000000000001', 'exit_prep', '2025-04-15', 'draft', 'Maria Santos', NULL, NULL);

-- Assessments for Fintech Innovations (a5000000-0000-0000-0000-000000000002)
CALL insert_assessment('b5000000-0000-0000-0000-000000000004', 'a5000000-0000-0000-0000-000000000002', 'screening', '2025-02-10', 'in_progress', 'Emily Chen', 'David Lee', NULL);

-- Verify inserted data
SELECT * FROM ASSESSMENTS ORDER BY company_id, assessment_date;
