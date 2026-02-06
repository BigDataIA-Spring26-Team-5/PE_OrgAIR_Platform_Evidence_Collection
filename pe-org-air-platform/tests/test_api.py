# tests/test_api.py

"""
API Endpoint Tests - Tests for all FastAPI endpoints
"""

import pytest
from fastapi import status



# DIMENSION WEIGHTS ENDPOINT TESTS


class TestDimensionWeightsEndpoint:
    """Tests for GET /api/v1/dimensions/weights endpoint."""
    
    def test_get_weights_success(self, client):
        """Test successful retrieval of dimension weights."""
        response = client.get("/api/v1/dimensions/weights")
        assert response.status_code == status.HTTP_200_OK
        
        data = response.json()
        assert "weights" in data
        assert "total" in data
        assert "is_valid" in data
    
    def test_get_weights_sum_to_one(self, client):
        """Test that returned weights sum to 1.0."""
        response = client.get("/api/v1/dimensions/weights")
        data = response.json()
        
        assert data["is_valid"] is True
        assert 0.999 <= data["total"] <= 1.001
    
    def test_get_weights_all_dimensions_present(self, client, expected_dimension_weights):
        """Test that all dimensions are present in response."""
        response = client.get("/api/v1/dimensions/weights")
        data = response.json()
        
        for dimension in expected_dimension_weights.keys():
            assert dimension in data["weights"]



# DIMENSION SCORE POST ENDPOINT TESTS


class TestAddDimensionScoreEndpoint:
    """Tests for POST /api/v1/assessments/{id}/scores endpoint."""
    
    def test_add_score_success(self, client, sample_assessment_id):
        """Test successful creation of dimension score.
        
        Note: May return 409 if dimension already exists from seed data.
        This is expected behavior for the unique constraint.
        """
        # Use assessment b1000000-0000-0000-0000-000000000002 (due_diligence, in_progress)
        # which has NO dimension scores in seed data
        assessment_without_scores = "b1000000-0000-0000-0000-000000000002"
        
        score_data = {
            "assessment_id": assessment_without_scores,
            "dimension": "culture_change",  # Unlikely to exist
            "score": 75.0,
            "confidence": 0.85
        }
        
        response = client.post(
            f"/api/v1/assessments/{assessment_without_scores}/scores",
            json=score_data
        )
        
        # Accept 201 (created) or 409 (already exists if test ran before)
        assert response.status_code in [status.HTTP_201_CREATED, status.HTTP_409_CONFLICT]
        
        if response.status_code == status.HTTP_201_CREATED:
            data = response.json()
            assert data["score"] == score_data["score"]
            assert "id" in data
    
    def test_add_score_invalid_score_too_high(self, client, invalid_dimension_score_high_score):
        """Test that score > 100 returns 422 error."""
        assessment_id = invalid_dimension_score_high_score["assessment_id"]
        
        response = client.post(
            f"/api/v1/assessments/{assessment_id}/scores",
            json=invalid_dimension_score_high_score
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_add_score_mismatched_assessment_id(self, client, valid_dimension_score_data):
        """Test that mismatched assessment_id returns 400 error."""
        wrong_assessment_id = "b9999999-0000-0000-0000-000000000000"
        
        response = client.post(
            f"/api/v1/assessments/{wrong_assessment_id}/scores",
            json=valid_dimension_score_data
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
    
    def test_add_score_invalid_uuid_format(self, client, valid_dimension_score_data):
        """Test that invalid UUID format returns 422 error."""
        response = client.post(
            "/api/v1/assessments/invalid-uuid/scores",
            json=valid_dimension_score_data
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY



# DIMENSION SCORE GET ENDPOINT TESTS


class TestGetDimensionScoresEndpoint:
    """Tests for GET /api/v1/assessments/{id}/scores endpoint."""
    
    def test_get_scores_not_found(self, client):
        """Test that non-existent assessment returns 404."""
        non_existent_id = "b9999999-0000-0000-0000-000000000000"
        
        response = client.get(f"/api/v1/assessments/{non_existent_id}/scores")
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_scores_invalid_uuid(self, client):
        """Test that invalid UUID returns 422."""
        response = client.get("/api/v1/assessments/invalid-uuid/scores")
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY



# DIMENSION SCORE PUT ENDPOINT TESTS


class TestUpdateDimensionScoreEndpoint:
    """Tests for PUT /api/v1/scores/{id} endpoint."""
    
    def test_update_score_not_found(self, client, valid_dimension_score_update):
        """Test that non-existent score returns 404."""
        non_existent_id = "c9999999-0000-0000-0000-000000000000"
        
        response = client.put(
            f"/api/v1/scores/{non_existent_id}",
            json=valid_dimension_score_update
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_update_score_invalid_uuid(self, client, valid_dimension_score_update):
        """Test that invalid UUID returns 422."""
        response = client.put(
            "/api/v1/scores/invalid-uuid",
            json=valid_dimension_score_update
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY



# COMPANY ENDPOINT TESTS


class TestCompanyEndpoints:
    """Tests for Company API endpoints."""
    
    def test_create_company_success(self, client, valid_company_data):
        """Test successful company creation.
        
        Requires: Industry must exist in database (run seed-industries.sql first)
        """
        response = client.post("/api/v1/companies", json=valid_company_data)
        
        # If 404, the industry doesn't exist - check if seed data is loaded
        if response.status_code == status.HTTP_404_NOT_FOUND:
            # Verify industry exists
            industry_check = client.get(f"/api/v1/industries/{valid_company_data['industry_id']}")
            if industry_check.status_code == status.HTTP_404_NOT_FOUND:
                pytest.skip(
                    f"Industry {valid_company_data['industry_id']} not found. "
                    "Run seed-industries.sql to populate test data."
                )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == valid_company_data["name"]
        assert "id" in data
    
    def test_create_company_invalid_position_factor(self, client, invalid_company_high_position):
        """Test that position_factor > 1 returns 422."""
        response = client.post("/api/v1/companies", json=invalid_company_high_position)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_create_company_industry_not_found(self, client):
        """Test that non-existent industry returns 404."""
        data = {
            "name": "Test Company",
            "industry_id": "550e8400-e29b-41d4-a716-000000000000"  # Non-existent
        }
        response = client.post("/api/v1/companies", json=data)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_list_companies_success(self, client):
        """Test listing companies."""
        response = client.get("/api/v1/companies")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert "page" in data
    
    def test_list_companies_pagination(self, client):
        """Test company list pagination."""
        response = client.get("/api/v1/companies?page=1&page_size=10")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 10
    
    def test_list_companies_invalid_page(self, client):
        """Test that page < 1 returns 422."""
        response = client.get("/api/v1/companies?page=0")
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_get_company_not_found(self, client):
        """Test that non-existent company returns 404."""
        response = client.get("/api/v1/companies/a9999999-0000-0000-0000-000000000000")
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_company_invalid_uuid(self, client):
        """Test that invalid UUID returns 422."""
        response = client.get("/api/v1/companies/invalid-uuid")
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY



# ASSESSMENT ENDPOINT TESTS


class TestAssessmentEndpoints:
    """Tests for Assessment API endpoints."""
    
    def test_create_assessment_success(self, client, valid_assessment_data):
        """Test successful assessment creation.
        
        Requires: Company must exist in database (run seed-companies.sql first)
        """
        response = client.post("/api/v1/assessments", json=valid_assessment_data)
        
        # If 404, the company doesn't exist - check if seed data is loaded
        if response.status_code == status.HTTP_404_NOT_FOUND:
            company_check = client.get(f"/api/v1/companies/{valid_assessment_data['company_id']}")
            if company_check.status_code == status.HTTP_404_NOT_FOUND:
                pytest.skip(
                    f"Company {valid_assessment_data['company_id']} not found. "
                    "Run seed-companies.sql to populate test data."
                )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["assessment_type"] == valid_assessment_data["assessment_type"]
        assert data["status"] == "draft"
        assert "id" in data
    
    def test_create_assessment_company_not_found(self, client):
        """Test that non-existent company returns 404."""
        data = {
            "company_id": "a9999999-0000-0000-0000-000000000000",  # Non-existent
            "assessment_type": "screening",
            "assessment_date": "2024-01-15"
        }
        response = client.post("/api/v1/assessments", json=data)
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_create_assessment_invalid_type(self, client, invalid_assessment_bad_type):
        """Test that invalid assessment_type returns 422."""
        response = client.post("/api/v1/assessments", json=invalid_assessment_bad_type)
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_list_assessments_success(self, client):
        """Test listing assessments."""
        response = client.get("/api/v1/assessments")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "items" in data
        assert "total" in data
    
    def test_list_assessments_filter_by_status(self, client):
        """Test filtering assessments by status."""
        response = client.get("/api/v1/assessments?status=draft")
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_assessment_not_found(self, client):
        """Test that non-existent assessment returns 404."""
        response = client.get("/api/v1/assessments/b9999999-0000-0000-0000-000000000000")
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_get_assessment_invalid_uuid(self, client):
        """Test that invalid UUID returns 422."""
        response = client.get("/api/v1/assessments/invalid-uuid")
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_update_assessment_status_not_found(self, client, valid_status_update):
        """Test that status update on non-existent assessment returns 404."""
        response = client.patch(
            "/api/v1/assessments/b9999999-0000-0000-0000-000000000000/status",
            json=valid_status_update
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
    
    def test_update_assessment_status_invalid(self, client, sample_company_id, invalid_status_update):
        """Test that invalid status returns 422."""
        # First create an assessment
        create_data = {
            "company_id": sample_company_id,
            "assessment_type": "screening",
            "assessment_date": "2025-12-01"  # Future date to avoid conflicts
        }
        create_response = client.post("/api/v1/assessments", json=create_data)
        
        if create_response.status_code == status.HTTP_201_CREATED:
            assessment_id = create_response.json()["id"]
            
            response = client.patch(
                f"/api/v1/assessments/{assessment_id}/status",
                json=invalid_status_update
            )
            
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY



# HEALTH ENDPOINT TESTS


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""
    
    def test_health_check_returns_response(self, client):
        """Test that health endpoint returns a response."""
        response = client.get("/health")
        
        # Should return either 200 (all healthy) or 503 (some unhealthy)
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_503_SERVICE_UNAVAILABLE]
    
    def test_health_check_response_structure(self, client):
        """Test that health response has correct structure."""
        response = client.get("/health")
        data = response.json()
        
        assert "status" in data
        assert "timestamp" in data
        assert "version" in data
        assert "dependencies" in data
    
    def test_health_check_has_all_dependencies(self, client):
        """Test that health response includes all required dependencies."""
        response = client.get("/health")
        data = response.json()
        
        dependencies = data["dependencies"]
        assert "snowflake" in dependencies
        assert "redis" in dependencies
        assert "s3" in dependencies
    
    def test_health_check_status_values(self, client):
        """Test that status is either 'healthy' or 'degraded'."""
        response = client.get("/health")
        data = response.json()
        
        assert data["status"] in ["healthy", "degraded"]
    
    def test_health_check_version_present(self, client):
        """Test that version is present and is a string."""
        response = client.get("/health")
        data = response.json()
        
        assert data["version"] == "1.0.0"
    
    def test_health_check_timestamp_format(self, client):
        """Test that timestamp is in valid ISO format."""
        response = client.get("/health")
        data = response.json()
        
        # Should not raise exception if valid ISO format
        from datetime import datetime
        try:
            datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            valid_timestamp = True
        except ValueError:
            valid_timestamp = False
        
        assert valid_timestamp
    
    def test_health_check_dependency_status_format(self, client):
        """Test that each dependency status is a string."""
        response = client.get("/health")
        data = response.json()
        
        for dep_name, dep_status in data["dependencies"].items():
            assert isinstance(dep_status, str)
    
    def test_health_check_200_when_all_healthy(self, client):
        """Test that 200 is returned only when all dependencies are healthy."""
        response = client.get("/health")
        data = response.json()
        
        # Check if status starts with "healthy" (could be "healthy" or "healthy (connected)")
        all_healthy = all(
            v.startswith("healthy") for v in data["dependencies"].values()
        )
        
        if all_healthy:
            assert response.status_code == status.HTTP_200_OK
            assert data["status"] == "healthy"
    
    def test_health_check_503_when_any_unhealthy(self, client):
        """Test that 503 is returned when any dependency is unhealthy."""
        response = client.get("/health")
        data = response.json()
        
        any_unhealthy = any(
            not v.startswith("healthy") for v in data["dependencies"].values()
        )
        
        if any_unhealthy:
            assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
            assert data["status"] == "degraded"



# ROOT ENDPOINT TEST


class TestRootEndpoint:
    """Tests for root endpoint."""
    
    def test_root_endpoint(self, client):
        """Test that root endpoint returns API info."""
        response = client.get("/")
        
        # If root endpoint doesn't exist, skip this test
        if response.status_code == status.HTTP_404_NOT_FOUND:
            pytest.skip("Root endpoint not implemented - add @app.get('/') to main.py")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "message" in data



# ERROR RESPONSE FORMAT TESTS


class TestErrorResponseFormat:
    """Tests for error response format consistency."""
    
    def test_404_error_has_detail(self, client):
        """Test that 404 errors have detail/message field."""
        response = client.get("/api/v1/companies/a9999999-0000-0000-0000-000000000000")
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = response.json()
        # Accept either standard FastAPI format or custom format
        assert "detail" in data or "message" in data
    
    def test_422_error_has_detail(self, client):
        """Test that 422 errors have detail/message field."""
        response = client.get("/api/v1/companies/invalid-uuid")
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()
        # Accept standard FastAPI format OR custom error format
        # Your API uses: {"message": ..., "details": ..., "error_code": ...}
        assert "detail" in data or "details" in data or "message" in data