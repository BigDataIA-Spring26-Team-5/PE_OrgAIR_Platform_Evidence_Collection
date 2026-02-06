"""
Services module for the PE OrgAIR Platform.
"""

from app.services.cache import get_cache
from app.services.document_chunking_service import get_document_chunking_service
from app.services.document_collector import get_document_collector_service
from app.services.document_parsing_service import get_document_parsing_service
from app.services.leadership_service import get_leadership_service
from app.services.redis_cache import get_redis_cache
from app.services.cache import get_cache
from app.services.redis_cache import RedisCache
from app.services.s3_storage import get_s3_service
from app.services.snowflake import get_snowflake_connection, SnowflakeService


def get_document_collector_service():
    """Lazy import to avoid circular dependency."""
    from app.services.document_collector import get_document_collector_service as _get
    return _get()


def get_document_chunking_service():
    """Lazy import to avoid circular dependency."""
    from app.services.document_chunking_service import get_document_chunking_service as _get
    return _get()


def get_document_parsing_service():
    """Lazy import to avoid circular dependency."""
    from app.services.document_parsing_service import get_document_parsing_service as _get
    return _get()


def get_leadership_service():
    """Lazy import to avoid circular dependency."""
    from app.services.leadership_service import get_leadership_service as _get
    return _get()


def get_job_data_service():
    """Lazy import to avoid circular dependency."""
    from app.services.job_data_service import get_job_data_service as _get
    return _get()


def get_job_signal_service():
    """Lazy import to avoid circular dependency."""
    from app.services.job_signal_service import get_job_signal_service as _get
    return _get()


def get_tech_signal_service():
    """Lazy import to avoid circular dependency."""
    from app.services.tech_signal_service import get_tech_signal_service as _get
    return _get()


def get_patent_signal_service():
    """Lazy import to avoid circular dependency."""
    from app.services.patent_signal_service import get_patent_signal_service as _get
    return _get()


# Lazy import for SignalsStorage classes
def _get_signals_storage_classes():
    from app.services.signals_storage import SignalsStorage, S3SignalsStorage
    return SignalsStorage, S3SignalsStorage


__all__ = [
    # Core services
    "get_cache",
    "get_cache",
    "get_document_chunking_service",
    "get_document_collector_service",
    "get_document_parsing_service",
    "get_leadership_service",
    "RedisCache",
    "get_s3_service",
    "get_snowflake_connection",
    "SnowflakeService",

    # Data services
    "get_job_data_service",

    # Signal services
    "get_job_signal_service",
    "get_tech_signal_service",
    "get_patent_signal_service",
]
