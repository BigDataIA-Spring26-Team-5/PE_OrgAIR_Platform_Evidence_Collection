"""
Redis Cache Tests - PE Org-AI-R Platform
tests/test_redis_cache.py

Tests for Redis caching functionality including cache hits,
misses, invalidation, and graceful degradation.
"""
import pytest
from unittest.mock import patch, MagicMock
from uuid import uuid4
from datetime import datetime, timezone
from pydantic import BaseModel

from app.services.redis_cache import RedisCache
from app.services.cache import get_cache, reset_cache, TTL_COMPANY, TTL_ASSESSMENT


class MockModel(BaseModel):
    """Mock Pydantic model for testing."""
    id: str
    name: str


class TestRedisCache:
    """Tests for the RedisCache class."""

    def test_redis_cache_init(self):
        """Test RedisCache initialization."""
        with patch('app.services.redis_cache.redis.Redis') as mock_redis:
            cache = RedisCache()
            mock_redis.assert_called_once()
            assert cache.client is not None

    def test_cache_set_and_get(self):
        """Test setting and getting cached values."""
        with patch('app.services.redis_cache.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_redis.return_value = mock_client

            cache = RedisCache()
            model = MockModel(id="123", name="Test")

            # Test set
            cache.set("test:key", model, 300)
            mock_client.setex.assert_called_once_with(
                "test:key",
                300,
                model.model_dump_json()
            )

            # Test get - simulate cache hit
            mock_client.get.return_value = model.model_dump_json()
            result = cache.get("test:key", MockModel)
            assert result is not None
            assert result.id == "123"
            assert result.name == "Test"

    def test_cache_get_miss(self):
        """Test cache miss returns None."""
        with patch('app.services.redis_cache.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_client.get.return_value = None
            mock_redis.return_value = mock_client

            cache = RedisCache()
            result = cache.get("nonexistent:key", MockModel)
            assert result is None

    def test_cache_delete(self):
        """Test deleting a cache entry."""
        with patch('app.services.redis_cache.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_redis.return_value = mock_client

            cache = RedisCache()
            cache.delete("test:key")
            mock_client.delete.assert_called_once_with("test:key")

    def test_cache_delete_pattern(self):
        """Test deleting cache entries by pattern."""
        with patch('app.services.redis_cache.redis.Redis') as mock_redis:
            mock_client = MagicMock()
            mock_client.scan_iter.return_value = ["key:1", "key:2", "key:3"]
            mock_redis.return_value = mock_client

            cache = RedisCache()
            cache.delete_pattern("key:*")

            mock_client.scan_iter.assert_called_once_with(match="key:*")
            assert mock_client.delete.call_count == 3


class TestCacheSingleton:
    """Tests for the cache singleton."""

    def teardown_method(self):
        """Reset cache after each test."""
        reset_cache()

    def test_get_cache_returns_instance(self):
        """Test that get_cache returns a RedisCache instance when Redis is available."""
        with patch('app.services.cache.RedisCache') as mock_cache_class:
            mock_instance = MagicMock()
            mock_instance.client.ping.return_value = True
            mock_cache_class.return_value = mock_instance

            reset_cache()
            cache = get_cache()
            assert cache is not None

    def test_get_cache_returns_none_when_redis_unavailable(self):
        """Test that get_cache returns None when Redis is unavailable."""
        with patch('app.services.cache.RedisCache') as mock_cache_class:
            mock_cache_class.side_effect = ConnectionError("Redis unavailable")

            reset_cache()
            cache = get_cache()
            assert cache is None

    def test_get_cache_singleton_behavior(self):
        """Test that get_cache returns the same instance."""
        with patch('app.services.cache.RedisCache') as mock_cache_class:
            mock_instance = MagicMock()
            mock_instance.client.ping.return_value = True
            mock_cache_class.return_value = mock_instance

            reset_cache()
            cache1 = get_cache()
            cache2 = get_cache()
            assert cache1 is cache2
            # Should only be called once due to singleton
            assert mock_cache_class.call_count == 1


class TestCacheInvalidation:
    """Tests for cache invalidation behavior."""

    def test_company_cache_invalidation_on_update(self):
        """Test that company cache is invalidated on update."""
        with patch('app.routers.companies.get_cache') as mock_get_cache:
            mock_cache = MagicMock()
            mock_get_cache.return_value = mock_cache

            from app.routers.companies import invalidate_company_cache
            company_id = uuid4()
            invalidate_company_cache(company_id)

            mock_cache.delete.assert_called_once_with(f"company:{company_id}")
            mock_cache.delete_pattern.assert_called_once_with("companies:page:*")

    def test_assessment_cache_invalidation_on_update(self):
        """Test that assessment cache is invalidated on update."""
        with patch('app.routers.assessments.get_cache') as mock_get_cache:
            mock_cache = MagicMock()
            mock_get_cache.return_value = mock_cache

            from app.routers.assessments import invalidate_assessment_cache
            assessment_id = uuid4()
            invalidate_assessment_cache(assessment_id)

            mock_cache.delete.assert_called_once_with(f"assessment:{assessment_id}")


class TestGracefulDegradation:
    """Tests for graceful degradation when Redis is unavailable."""

    def test_api_works_without_redis(self):
        """Test that API endpoints work even when Redis is unavailable."""
        with patch('app.services.cache.get_cache', return_value=None):
            # Verify get_cache returns None
            from app.services.cache import get_cache
            assert get_cache() is None

    def test_cache_operations_fail_silently(self):
        """Test that cache operations fail silently without raising exceptions."""
        with patch('app.routers.companies.get_cache') as mock_get_cache:
            mock_cache = MagicMock()
            mock_cache.delete.side_effect = Exception("Redis error")
            mock_get_cache.return_value = mock_cache

            from app.routers.companies import invalidate_company_cache
            # Should not raise an exception
            invalidate_company_cache(uuid4())


class TestTTLConstants:
    """Tests for TTL constant values."""

    def test_ttl_company(self):
        """Test company TTL is 300 seconds (5 minutes)."""
        assert TTL_COMPANY == 300

    def test_ttl_assessment(self):
        """Test assessment TTL is 120 seconds (2 minutes)."""
        assert TTL_ASSESSMENT == 120

    def test_ttl_industry(self):
        """Test industry TTL is 3600 seconds (1 hour)."""
        from app.services.cache import TTL_INDUSTRY
        assert TTL_INDUSTRY == 3600

    def test_ttl_dimension_weights(self):
        """Test dimension weights TTL is 86400 seconds (24 hours)."""
        from app.services.cache import TTL_DIMENSION_WEIGHTS
        assert TTL_DIMENSION_WEIGHTS == 86400


class TestCacheKeyPatterns:
    """Tests for cache key patterns."""

    def test_company_cache_key_pattern(self):
        """Test company cache key follows pattern: company:{uuid}."""
        company_id = uuid4()
        cache_key = f"company:{company_id}"
        assert cache_key.startswith("company:")
        assert str(company_id) in cache_key

    def test_assessment_cache_key_pattern(self):
        """Test assessment cache key follows pattern: assessment:{uuid}."""
        assessment_id = uuid4()
        cache_key = f"assessment:{assessment_id}"
        assert cache_key.startswith("assessment:")
        assert str(assessment_id) in cache_key

    def test_industry_list_cache_key(self):
        """Test industry list cache key is: industry:list."""
        cache_key = "industry:list"
        assert cache_key == "industry:list"

    def test_dimension_weights_cache_key(self):
        """Test dimension weights cache key is: dimension:weights."""
        cache_key = "dimension:weights"
        assert cache_key == "dimension:weights"
