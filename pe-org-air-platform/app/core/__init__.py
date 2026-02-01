"""
Core Package - PE Org-AI-R Platform
app/core/__init__.py

Core infrastructure: dependencies, exceptions.
"""

from app.core.dependencies import (
    get_assessment_repository,
    get_company_repository,
    get_dimension_score_repository,
    get_industry_repository,
)
from app.core.exceptions import (
    DatabaseConnectionException,
    DuplicateEntityException,
    EntityDeletedException,
    EntityNotFoundException,
    ForeignKeyViolationException,
    RepositoryException,
)

__all__ = [
    # Dependencies
    "get_assessment_repository",
    "get_company_repository",
    "get_dimension_score_repository",
    "get_industry_repository",
    # Exceptions
    "DatabaseConnectionException",
    "DuplicateEntityException",
    "EntityDeletedException",
    "EntityNotFoundException",
    "ForeignKeyViolationException",
    "RepositoryException",
]
