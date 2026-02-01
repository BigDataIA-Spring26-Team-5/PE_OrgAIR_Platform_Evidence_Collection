"""
Cache Service Singleton - PE Org-AI-R Platform
app/services/cache.py

Provides a singleton Redis cache instance with TTL constants.
Gracefully handles Redis unavailability.
"""
import redis
from typing import Optional
from app.services.redis_cache import RedisCache
from app.config import settings

# TTL constants from requirements (in seconds)
TTL_COMPANY = 300              # 5 minutes
TTL_ASSESSMENT = 120           # 2 minutes
TTL_INDUSTRY = 3600            # 1 hour
TTL_DIMENSION_WEIGHTS = 86400  # 24 hours

# Singleton instance
_cache: Optional[RedisCache] = None


def get_cache() -> Optional[RedisCache]:
    """
    Get or create Redis cache instance.

    Returns:
        RedisCache instance if Redis is available, None otherwise.

    Note:
        Returns None if Redis is unavailable, allowing the application
        to continue functioning without caching (graceful degradation).
    """
    global _cache
    if _cache is None:
        try:
            _cache = RedisCache()
            _cache.client.ping()  # Test connection
        except (redis.RedisError, ConnectionError):
            _cache = None
    return _cache


def reset_cache() -> None:
    """
    Reset the cache singleton.

    Useful for testing or when Redis connection needs to be re-established.
    """
    global _cache
    _cache = None
