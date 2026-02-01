"""
Dependencies - PE Org-AI-R Platform
app/dependencies.py

FastAPI dependency injection for repositories.
"""

from functools import lru_cache

from app.repositories.industry_repository import IndustryRepository
from app.repositories.company_repository import CompanyRepository
from app.repositories.assessment_repository import AssessmentRepository
from app.repositories.dimension_score_repository import DimensionScoreRepository


@lru_cache()
def get_industry_repository() -> IndustryRepository:
    """Get cached IndustryRepository instance."""
    return IndustryRepository()


@lru_cache()
def get_company_repository() -> CompanyRepository:
    """Get cached CompanyRepository instance."""
    return CompanyRepository()


@lru_cache()
def get_assessment_repository() -> AssessmentRepository:
    """Get cached AssessmentRepository instance."""
    return AssessmentRepository()


@lru_cache()
def get_dimension_score_repository() -> DimensionScoreRepository:
    """Get cached DimensionScoreRepository instance."""
    return DimensionScoreRepository()
