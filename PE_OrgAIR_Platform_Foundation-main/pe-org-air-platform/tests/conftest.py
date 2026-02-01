# tests/conftest.py

"""
Pytest Fixtures - Shared test configurations and data for all models and APIs

SEED DATA ID REFERENCE:
- Industries: 550e8400-e29b-41d4-a716-446655440001 to 446655440005
- Companies:  a1000000-..., a2000000-..., a3000000-..., a4000000-..., a5000000-...
- Assessments: b1000000-..., b2000000-..., b3000000-..., b4000000-..., b5000000-...
- Dimension Scores: c1000000-..., c2000000-..., c3000000-..., c5000000-...
"""

import pytest
from uuid import uuid4, UUID
from datetime import datetime, date, timezone
from fastapi.testclient import TestClient

from app.main import app
from app.models.enumerations import Dimension, AssessmentType, AssessmentStatus


# =============================================================================
# FASTAPI TEST CLIENT FIXTURE
# =============================================================================

@pytest.fixture(scope="module")
def client():
    """Create a TestClient for FastAPI application."""
    with TestClient(app) as test_client:
        yield test_client


# =============================================================================
# SAMPLE UUID FIXTURES - MATCHING SEED DATA
# =============================================================================

@pytest.fixture
def sample_uuid():
    """Generate a random UUID for testing."""
    return str(uuid4())


@pytest.fixture
def sample_industry_id():
    """Manufacturing industry from seed-industries.sql."""
    return "550e8400-e29b-41d4-a716-446655440001"


@pytest.fixture
def sample_company_id():
    """Apex Manufacturing Inc from seed-companies.sql."""
    return "a1000000-0000-0000-0000-000000000001"


@pytest.fixture
def sample_assessment_id():
    """Apex Manufacturing Screening (approved) from seed-assessments.sql."""
    return "b1000000-0000-0000-0000-000000000001"


@pytest.fixture
def sample_dimension_score_id():
    """First dimension score from seed-dimension-scores.sql."""
    return "c1000000-0000-0000-0000-000000000001"


@pytest.fixture
def valid_company_industry_id(sample_industry_id):
    """Valid industry ID that exists in the database."""
    return sample_industry_id


# =============================================================================
# DIMENSION SCORE FIXTURES
# =============================================================================

@pytest.fixture
def valid_dimension_score_data(sample_assessment_id):
    """Valid dimension score data for testing."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "data_infrastructure",
        "score": 85.5,
        "weight": 0.25,
        "confidence": 0.92,
        "evidence_count": 5
    }


@pytest.fixture
def valid_dimension_score_minimal(sample_assessment_id):
    """Minimal valid dimension score (only required fields)."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "ai_governance",
        "score": 72.0
    }


@pytest.fixture
def invalid_dimension_score_high_score(sample_assessment_id):
    """Invalid dimension score - score > 100."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "data_infrastructure",
        "score": 150.0,
        "confidence": 0.8
    }


@pytest.fixture
def invalid_dimension_score_negative_score(sample_assessment_id):
    """Invalid dimension score - score < 0."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "data_infrastructure",
        "score": -10.0,
        "confidence": 0.8
    }


@pytest.fixture
def invalid_dimension_score_bad_weight(sample_assessment_id):
    """Invalid dimension score - weight > 1."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "data_infrastructure",
        "score": 85.0,
        "weight": 1.5
    }


@pytest.fixture
def invalid_dimension_score_bad_confidence(sample_assessment_id):
    """Invalid dimension score - confidence > 1."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "data_infrastructure",
        "score": 85.0,
        "confidence": 1.5
    }


@pytest.fixture
def invalid_dimension_score_bad_dimension(sample_assessment_id):
    """Invalid dimension score - invalid dimension value."""
    return {
        "assessment_id": sample_assessment_id,
        "dimension": "invalid_dimension",
        "score": 85.0
    }


@pytest.fixture
def valid_dimension_score_update():
    """Valid update data for dimension score."""
    return {
        "score": 90.0,
        "confidence": 0.95,
        "evidence_count": 10
    }


@pytest.fixture
def invalid_dimension_score_update():
    """Invalid update data - score > 100."""
    return {"score": 150.0}


@pytest.fixture
def expected_dimension_weights():
    """Expected dimension weights for validation."""
    return {
        "data_infrastructure": 0.25,
        "ai_governance": 0.20,
        "technology_stack": 0.15,
        "talent_skills": 0.15,
        "leadership_vision": 0.10,
        "use_case_portfolio": 0.10,
        "culture_change": 0.05
    }


# =============================================================================
# INDUSTRY FIXTURES
# =============================================================================

@pytest.fixture
def valid_industry_data():
    """Valid industry data for testing."""
    return {
        "name": "Software & Technology",
        "sector": "Technology",
        "h_r_base": 75.0
    }


@pytest.fixture
def invalid_industry_high_hr():
    """Invalid industry - h_r_base > 100."""
    return {
        "name": "Test Industry",
        "sector": "Test",
        "h_r_base": 150.0
    }


@pytest.fixture
def invalid_industry_negative_hr():
    """Invalid industry - h_r_base < 0."""
    return {
        "name": "Test Industry",
        "sector": "Test",
        "h_r_base": -10.0
    }


@pytest.fixture
def invalid_industry_empty_name():
    """Invalid industry - empty name."""
    return {
        "name": "",
        "sector": "Test",
        "h_r_base": 50.0
    }


# =============================================================================
# COMPANY FIXTURES
# =============================================================================

@pytest.fixture
def valid_company_data(sample_industry_id):
    """Valid company data using existing industry from seed data."""
    return {
        "name": f"Test Company {uuid4().hex[:8]}",  # Unique name
        "ticker": "TEST",
        "industry_id": sample_industry_id,
        "position_factor": 0.25
    }


@pytest.fixture
def valid_company_minimal(sample_industry_id):
    """Minimal valid company data."""
    return {
        "name": f"Minimal Company {uuid4().hex[:8]}",
        "industry_id": sample_industry_id
    }


@pytest.fixture
def invalid_company_high_position(sample_industry_id):
    """Invalid company - position_factor > 1."""
    return {
        "name": "Test Company",
        "industry_id": sample_industry_id,
        "position_factor": 1.5
    }


@pytest.fixture
def invalid_company_low_position(sample_industry_id):
    """Invalid company - position_factor < -1."""
    return {
        "name": "Test Company",
        "industry_id": sample_industry_id,
        "position_factor": -1.5
    }


@pytest.fixture
def invalid_company_empty_name(sample_industry_id):
    """Invalid company - empty name."""
    return {
        "name": "",
        "industry_id": sample_industry_id
    }


@pytest.fixture
def valid_company_update():
    """Valid company update data."""
    return {
        "name": "Updated Company Name",
        "position_factor": 0.5
    }


# =============================================================================
# ASSESSMENT FIXTURES
# =============================================================================

@pytest.fixture
def valid_assessment_data(sample_company_id):
    """Valid assessment data using existing company from seed data."""
    return {
        "company_id": sample_company_id,
        "assessment_type": "screening",
        "assessment_date": "2025-06-15",  # Future date to avoid conflicts
        "primary_assessor": "John Doe",
        "secondary_assessor": "Jane Smith"
    }


@pytest.fixture
def valid_assessment_minimal(sample_company_id):
    """Minimal valid assessment data."""
    return {
        "company_id": sample_company_id,
        "assessment_type": "exit_prep",  # Different type
        "assessment_date": "2025-07-15"
    }


@pytest.fixture
def invalid_assessment_bad_type(sample_company_id):
    """Invalid assessment - bad assessment_type."""
    return {
        "company_id": sample_company_id,
        "assessment_type": "invalid_type",
        "assessment_date": "2024-01-15"
    }


@pytest.fixture
def invalid_assessment_bad_date(sample_company_id):
    """Invalid assessment - bad date format."""
    return {
        "company_id": sample_company_id,
        "assessment_type": "screening",
        "assessment_date": "not-a-date"
    }


@pytest.fixture
def valid_status_update():
    """Valid status update data."""
    return {"status": "in_progress"}


@pytest.fixture
def invalid_status_update():
    """Invalid status update - bad status."""
    return {"status": "invalid_status"}