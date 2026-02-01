"""
Custom Exceptions - PE Org-AI-R Platform
app/exceptions.py

Custom exception classes for repository operations.
"""


class RepositoryException(Exception):
    """Base exception for repository operations."""

    pass


class EntityNotFoundException(RepositoryException):
    """Entity not found in database."""

    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} with ID {entity_id} not found")


class EntityDeletedException(RepositoryException):
    """Entity has been soft-deleted."""

    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} with ID {entity_id} has been deleted")


class DuplicateEntityException(RepositoryException):
    """Duplicate entity violation."""

    def __init__(self, message: str = "Entity already exists"):
        self.message = message
        super().__init__(message)


class DatabaseConnectionException(RepositoryException):
    """Database connection failure."""

    def __init__(self, message: str = "Database connection failed"):
        self.message = message
        super().__init__(message)


class ForeignKeyViolationException(RepositoryException):
    """Foreign key constraint violation."""

    def __init__(self, message: str = "Foreign key constraint violation"):
        self.message = message
        super().__init__(message)
