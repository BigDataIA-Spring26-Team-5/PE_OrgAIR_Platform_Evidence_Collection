"""
Repositories Package - PE Org-AI-R Platform
app/repositories/__init__.py

Data access layer for Snowflake database operations.
"""

from app.repositories.base import BaseRepository
from app.repositories.assessment_repository import AssessmentRepository
from app.repositories.company_repository import CompanyRepository
from app.repositories.dimension_score_repository import DimensionScoreRepository
from app.repositories.industry_repository import IndustryRepository

__all__ = [
    "BaseRepository",
    "AssessmentRepository",
    "CompanyRepository",
    "DimensionScoreRepository",
    "IndustryRepository",
]
