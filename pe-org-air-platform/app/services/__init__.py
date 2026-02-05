"""
Services module for the PE OrgAIR Platform.
"""

from app.services.cache import get_cache_service
from app.services.document_chunking_service import get_document_chunking_service
from app.services.document_collector import get_document_collector
from app.services.document_parsing_service import get_document_parsing_service
from app.services.leadership_service import get_leadership_service
from app.services.redis_cache import get_redis_cache
from app.services.s3_storage import get_s3_service
from app.services.signals_storage import get_signals_storage_service
from app.services.snowflake import get_snowflake_connection, SnowflakeService

# New signal services
from app.services.job_signal_service import get_job_signal_service
from app.services.tech_signal_service import get_tech_signal_service
from app.services.patent_signal_service import get_patent_signal_service

__all__ = [
    # Core services
    "get_cache_service",
    "get_document_chunking_service",
    "get_document_collector",
    "get_document_parsing_service",
    "get_leadership_service",
    "get_redis_cache",
    "get_s3_service",
    "get_signals_storage_service",
    "get_snowflake_connection",
    "SnowflakeService",
    
    # Signal services
    "get_job_signal_service",
    "get_tech_signal_service",
    "get_patent_signal_service",
]