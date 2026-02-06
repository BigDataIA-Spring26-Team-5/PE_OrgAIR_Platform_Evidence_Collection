# tests/test_models.py

"""
Model Validation Tests - Tests for all Pydantic model validations
"""

import pytest
from uuid import uuid4, UUID
from datetime import date, datetime
from pydantic import ValidationError

from app.models.enumerations import Dimension, AssessmentType, AssessmentStatus
from app.models.dimension import (
    DimensionScoreBase,
    DimensionScoreCreate,
    DimensionScoreUpdate,
    DimensionScoreResponse,
    DIMENSION_WEIGHTS
)
from app.models.industry import IndustryBase, IndustryCreate, IndustryResponse
from app.models.company import CompanyBase, CompanyCreate, CompanyUpdate, CompanyResponse
from app.models.assessment import (
    AssessmentBase,
    AssessmentCreate,
    AssessmentResponse,
    StatusUpdate
)



# ENUMERATION TESTS


class TestDimensionEnum:
    """Tests for Dimension enumeration."""
    
    def test_all_dimensions_exist(self):
        """Test that all 7 dimensions are defined."""
        expected = [
            "data_infrastructure", "ai_governance", "technology_stack",
            "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"
        ]
        actual = [d.value for d in Dimension]
        assert actual == expected
    
    def test_dimension_count(self):
        """Test that exactly 7 dimensions exist."""
        assert len(Dimension) == 7


class TestAssessmentTypeEnum:
    """Tests for AssessmentType enumeration."""
    
    def test_all_assessment_types_exist(self):
        """Test that all assessment types are defined."""
        expected = ["screening", "due_diligence", "quarterly", "exit_prep"]
        actual = [t.value for t in AssessmentType]
        assert actual == expected
    
    def test_assessment_type_count(self):
        """Test that exactly 4 assessment types exist."""
        assert len(AssessmentType) == 4


class TestAssessmentStatusEnum:
    """Tests for AssessmentStatus enumeration."""
    
    def test_all_statuses_exist(self):
        """Test that all statuses are defined."""
        expected = ["draft", "in_progress", "submitted", "approved", "superseded"]
        actual = [s.value for s in AssessmentStatus]
        assert actual == expected
    
    def test_status_count(self):
        """Test that exactly 5 statuses exist."""
        assert len(AssessmentStatus) == 5



# DIMENSION WEIGHTS TESTS


class TestDimensionWeights:
    """Tests for dimension weights configuration."""
    
    def test_weights_sum_to_one(self):
        """Test that all dimension weights sum to 1.0."""
        total = sum(DIMENSION_WEIGHTS.values())
        assert 0.999 <= total <= 1.001
    
    def test_all_dimensions_have_weights(self):
        """Test that every dimension has a weight assigned."""
        for dimension in Dimension:
            assert dimension in DIMENSION_WEIGHTS
    
    def test_weights_are_valid_range(self):
        """Test that all weights are between 0 and 1."""
        for dimension, weight in DIMENSION_WEIGHTS.items():
            assert 0 <= weight <= 1
    
    def test_expected_weight_values(self, expected_dimension_weights):
        """Test that weights match expected values."""
        for dimension in Dimension:
            expected = expected_dimension_weights[dimension.value]
            actual = DIMENSION_WEIGHTS[dimension]
            assert actual == expected



# DIMENSION SCORE MODEL TESTS


class TestDimensionScoreCreate:
    """Tests for DimensionScoreCreate model validation."""
    
    def test_valid_dimension_score(self, valid_dimension_score_data):
        """Test creating a valid dimension score."""
        score = DimensionScoreCreate(**valid_dimension_score_data)
        assert score.score == 85.5
        assert score.dimension == Dimension.DATA_INFRASTRUCTURE
        assert score.confidence == 0.92
    
    def test_valid_minimal_dimension_score(self, valid_dimension_score_minimal):
        """Test creating dimension score with only required fields."""
        score = DimensionScoreCreate(**valid_dimension_score_minimal)
        assert score.score == 72.0
        assert score.confidence == 0.8  # Default
        assert score.evidence_count == 0  # Default
    
    def test_auto_weight_assignment(self, sample_assessment_id):
        """Test that weight is auto-assigned based on dimension."""
        data = {"assessment_id": sample_assessment_id, "dimension": "data_infrastructure", "score": 80.0}
        score = DimensionScoreCreate(**data)
        assert score.weight == 0.25
    
    def test_invalid_score_too_high(self, invalid_dimension_score_high_score):
        """Test that score > 100 raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreCreate(**invalid_dimension_score_high_score)
    
    def test_invalid_score_negative(self, invalid_dimension_score_negative_score):
        """Test that score < 0 raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreCreate(**invalid_dimension_score_negative_score)
    
    def test_invalid_weight_too_high(self, invalid_dimension_score_bad_weight):
        """Test that weight > 1 raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreCreate(**invalid_dimension_score_bad_weight)
    
    def test_invalid_confidence_too_high(self, invalid_dimension_score_bad_confidence):
        """Test that confidence > 1 raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreCreate(**invalid_dimension_score_bad_confidence)
    
    def test_invalid_dimension_value(self, invalid_dimension_score_bad_dimension):
        """Test that invalid dimension raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreCreate(**invalid_dimension_score_bad_dimension)
    
    def test_score_boundary_zero(self, sample_assessment_id):
        """Test that score = 0 is valid."""
        data = {"assessment_id": sample_assessment_id, "dimension": "data_infrastructure", "score": 0.0}
        score = DimensionScoreCreate(**data)
        assert score.score == 0.0
    
    def test_score_boundary_hundred(self, sample_assessment_id):
        """Test that score = 100 is valid."""
        data = {"assessment_id": sample_assessment_id, "dimension": "data_infrastructure", "score": 100.0}
        score = DimensionScoreCreate(**data)
        assert score.score == 100.0


class TestDimensionScoreUpdate:
    """Tests for DimensionScoreUpdate model validation."""
    
    def test_valid_partial_update(self):
        """Test valid partial update."""
        update = DimensionScoreUpdate(score=90.0)
        assert update.score == 90.0
        assert update.dimension is None
    
    def test_invalid_update_score_too_high(self):
        """Test that score > 100 raises validation error."""
        with pytest.raises(ValidationError):
            DimensionScoreUpdate(score=150.0)
    
    def test_empty_update_allowed(self):
        """Test that empty update is allowed."""
        update = DimensionScoreUpdate()
        assert update.score is None


class TestAllDimensionTypes:
    """Test dimension score creation for all dimension types."""
    
    @pytest.mark.parametrize("dimension,expected_weight", [
        ("data_infrastructure", 0.25),
        ("ai_governance", 0.20),
        ("technology_stack", 0.15),
        ("talent_skills", 0.15),
        ("leadership_vision", 0.10),
        ("use_case_portfolio", 0.10),
        ("culture_change", 0.05),
    ])
    def test_each_dimension_auto_weight(self, sample_assessment_id, dimension, expected_weight):
        """Test auto-weight assignment for each dimension type."""
        data = {"assessment_id": sample_assessment_id, "dimension": dimension, "score": 75.0}
        score = DimensionScoreCreate(**data)
        assert score.weight == expected_weight



# INDUSTRY MODEL TESTS


class TestIndustryCreate:
    """Tests for IndustryCreate model validation."""
    
    def test_valid_industry(self, valid_industry_data):
        """Test creating a valid industry."""
        industry = IndustryCreate(**valid_industry_data)
        assert industry.name == "Software & Technology"
        assert industry.sector == "Technology"
        assert industry.h_r_base == 75.0
    
    def test_invalid_h_r_base_too_high(self, invalid_industry_high_hr):
        """Test that h_r_base > 100 raises validation error."""
        with pytest.raises(ValidationError):
            IndustryCreate(**invalid_industry_high_hr)
    
    def test_invalid_h_r_base_negative(self, invalid_industry_negative_hr):
        """Test that h_r_base < 0 raises validation error."""
        with pytest.raises(ValidationError):
            IndustryCreate(**invalid_industry_negative_hr)
    
    def test_invalid_empty_name(self, invalid_industry_empty_name):
        """Test that empty name raises validation error."""
        with pytest.raises(ValidationError):
            IndustryCreate(**invalid_industry_empty_name)
    
    def test_h_r_base_boundary_zero(self):
        """Test that h_r_base = 0 is valid."""
        data = {"name": "Test", "sector": "Test", "h_r_base": 0.0}
        industry = IndustryCreate(**data)
        assert industry.h_r_base == 0.0
    
    def test_h_r_base_boundary_hundred(self):
        """Test that h_r_base = 100 is valid."""
        data = {"name": "Test", "sector": "Test", "h_r_base": 100.0}
        industry = IndustryCreate(**data)
        assert industry.h_r_base == 100.0
    
    def test_name_max_length(self):
        """Test that name respects max length."""
        long_name = "A" * 256
        with pytest.raises(ValidationError):
            IndustryCreate(name=long_name, sector="Test", h_r_base=50.0)


class TestIndustryResponse:
    """Tests for IndustryResponse model."""
    
    def test_response_includes_id_and_timestamp(self, valid_industry_data):
        """Test that response includes id and created_at."""
        response = IndustryResponse(**valid_industry_data)
        assert response.id is not None
        assert response.created_at is not None



# COMPANY MODEL TESTS


class TestCompanyCreate:
    """Tests for CompanyCreate model validation."""
    
    def test_valid_company(self):
        """Test creating a valid company."""
        data = {
            "name": "TechCorp",
            "ticker_symbol": "tech",
            "industry_id": "550e8400-e29b-41d4-a716-446655440001",
            "position_factor": 0.5
        }
        company = CompanyCreate(**data)
        assert company.name == "TechCorp"
        assert company.ticker_symbol == "TECH"  # Should be uppercased
        assert company.position_factor == 0.5
    
    def test_ticker_uppercase_conversion(self):
        """Test that ticker is converted to uppercase."""
        data = {
            "name": "Test",
            "ticker_symbol": "test",
            "industry_id": "550e8400-e29b-41d4-a716-446655440001"
        }
        company = CompanyCreate(**data)
        assert company.ticker_symbol == "TEST"
    
    def test_invalid_position_factor_too_high(self, invalid_company_high_position):
        """Test that position_factor > 1 raises validation error."""
        with pytest.raises(ValidationError):
            CompanyCreate(**invalid_company_high_position)
    
    def test_invalid_position_factor_too_low(self, invalid_company_low_position):
        """Test that position_factor < -1 raises validation error."""
        with pytest.raises(ValidationError):
            CompanyCreate(**invalid_company_low_position)
    
    def test_position_factor_boundary_positive(self):
        """Test that position_factor = 1.0 is valid."""
        data = {"name": "Test", "industry_id": "550e8400-e29b-41d4-a716-446655440001", "position_factor": 1.0}
        company = CompanyCreate(**data)
        assert company.position_factor == 1.0
    
    def test_position_factor_boundary_negative(self):
        """Test that position_factor = -1.0 is valid."""
        data = {"name": "Test", "industry_id": "550e8400-e29b-41d4-a716-446655440001", "position_factor": -1.0}
        company = CompanyCreate(**data)
        assert company.position_factor == -1.0
    
    def test_default_position_factor(self):
        """Test default position_factor is 0.0."""
        data = {"name": "Test", "industry_id": "550e8400-e29b-41d4-a716-446655440001"}
        company = CompanyCreate(**data)
        assert company.position_factor == 0.0
    
    def test_ticker_max_length(self):
        """Test that ticker respects max length."""
        data = {"name": "Test", "ticker_symbol": "A" * 11, "industry_id": "550e8400-e29b-41d4-a716-446655440001"}
        with pytest.raises(ValidationError):
            CompanyCreate(**data)



# ASSESSMENT MODEL TESTS


class TestAssessmentCreate:
    """Tests for AssessmentCreate model validation."""
    
    def test_valid_assessment(self, valid_assessment_data):
        """Test creating a valid assessment."""
        assessment = AssessmentCreate(**valid_assessment_data)
        assert assessment.assessment_type == AssessmentType.SCREENING
        assert assessment.primary_assessor == "John Doe"
    
    def test_valid_minimal_assessment(self, valid_assessment_minimal):
        """Test creating assessment with only required fields."""
        assessment = AssessmentCreate(**valid_assessment_minimal)
        assert assessment.primary_assessor is None
        assert assessment.secondary_assessor is None
    
    def test_invalid_assessment_type(self, invalid_assessment_bad_type):
        """Test that invalid assessment_type raises validation error."""
        with pytest.raises(ValidationError):
            AssessmentCreate(**invalid_assessment_bad_type)
    
    def test_invalid_date_format(self, invalid_assessment_bad_date):
        """Test that invalid date format raises validation error."""
        with pytest.raises(ValidationError):
            AssessmentCreate(**invalid_assessment_bad_date)
    
    @pytest.mark.parametrize("assessment_type", [
        "screening", "due_diligence", "quarterly", "exit_prep"
    ])
    def test_all_assessment_types_valid(self, assessment_type):
        """Test that all assessment types are valid."""
        data = {
            "company_id": "550e8400-e29b-41d4-a716-446655440003",
            "assessment_type": assessment_type,
            "assessment_date": "2024-01-15"
        }
        assessment = AssessmentCreate(**data)
        assert assessment.assessment_type.value == assessment_type


class TestAssessmentResponse:
    """Tests for AssessmentResponse model."""
    
    def test_response_defaults(self, valid_assessment_data):
        """Test that response has correct defaults."""
        response = AssessmentResponse(**valid_assessment_data)
        assert response.status == AssessmentStatus.DRAFT
        assert response.v_r_score is None
        assert response.id is not None
        assert response.created_at is not None
    
    def test_confidence_interval_validation_valid(self, valid_assessment_data):
        """Test valid confidence interval."""
        data = {**valid_assessment_data, "confidence_lower": 70.0, "confidence_upper": 90.0}
        response = AssessmentResponse(**data)
        assert response.confidence_lower == 70.0
        assert response.confidence_upper == 90.0
    
    def test_confidence_interval_validation_invalid(self, valid_assessment_data):
        """Test that confidence_upper < confidence_lower raises error."""
        data = {**valid_assessment_data, "confidence_lower": 90.0, "confidence_upper": 70.0}
        with pytest.raises(ValidationError):
            AssessmentResponse(**data)
    
    def test_v_r_score_boundary(self, valid_assessment_data):
        """Test v_r_score boundaries."""
        data = {**valid_assessment_data, "v_r_score": 100.0}
        response = AssessmentResponse(**data)
        assert response.v_r_score == 100.0
    
    def test_v_r_score_invalid_too_high(self, valid_assessment_data):
        """Test that v_r_score > 100 raises error."""
        data = {**valid_assessment_data, "v_r_score": 101.0}
        with pytest.raises(ValidationError):
            AssessmentResponse(**data)


class TestStatusUpdate:
    """Tests for StatusUpdate model."""
    
    def test_valid_status_update(self, valid_status_update):
        """Test valid status update."""
        update = StatusUpdate(**valid_status_update)
        assert update.status == AssessmentStatus.IN_PROGRESS
    
    def test_invalid_status(self, invalid_status_update):
        """Test that invalid status raises error."""
        with pytest.raises(ValidationError):
            StatusUpdate(**invalid_status_update)
    
    @pytest.mark.parametrize("status", [
        "draft", "in_progress", "submitted", "approved", "superseded"
    ])
    def test_all_statuses_valid(self, status):
        """Test that all statuses are valid."""
        update = StatusUpdate(status=status)
        assert update.status.value == status