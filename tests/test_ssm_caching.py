"""
Test SSMClientCachingWrapper to verify cachetools compatibility.
This test must pass on cachetools >= 5.5.1 (any version).
"""
import time
from unittest.mock import MagicMock

import pytest
from cachetools import TTLCache

from modular_sdk.services.ssm_service import (
    AbstractSSMClient,
    SSMClientCachingWrapper,
    StorageType,
)


# ---------------------------------------------------------------------------
# Sanity check: cachetools API hasn't changed
# ---------------------------------------------------------------------------

def test_cachetools_ttlcache_basic_api():
    """Verify TTLCache supports the operations modular-sdk depends on."""
    cache = TTLCache(maxsize=50, ttl=60)

    cache['key1'] = 'value1'
    assert 'key1' in cache
    assert 'missing' not in cache
    assert cache['key1'] == 'value1'
    assert cache.pop('key1', None) == 'value1'
    assert cache.pop('missing', None) is None
    assert 'key1' not in cache


def test_cachetools_ttl_expiration():
    """Verify TTL actually expires entries."""
    cache = TTLCache(maxsize=10, ttl=0.5)
    cache['x'] = 'y'
    assert 'x' in cache
    time.sleep(0.6)
    assert 'x' not in cache


# ---------------------------------------------------------------------------
# Tests for SSMClientCachingWrapper
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_inner_client():
    client = MagicMock(spec=AbstractSSMClient)
    client.storage_type = StorageType.SSM
    client.get_parameter.return_value = 'secret-value'
    # By default, the inner client returns the same name it received
    client.put_parameter.side_effect = lambda name, value, _type='SecureString': name
    client.delete_parameter.return_value = True
    return client


@pytest.fixture
def mock_env_service():
    env = MagicMock()
    env.inner_cache_ttl_seconds.return_value = 60
    return env


@pytest.fixture
def wrapper(mock_inner_client, mock_env_service):
    return SSMClientCachingWrapper(
        client=mock_inner_client,
        environment_service=mock_env_service,
    )


def test_storage_type_passthrough(wrapper, mock_inner_client):
    assert wrapper.storage_type == StorageType.SSM


def test_get_parameter_caches_result(wrapper, mock_inner_client):
    # First call - hits inner client
    result1 = wrapper.get_parameter('my-secret')
    assert result1 == 'secret-value'
    assert mock_inner_client.get_parameter.call_count == 1

    # Second call - served from cache
    result2 = wrapper.get_parameter('my-secret')
    assert result2 == 'secret-value'
    assert mock_inner_client.get_parameter.call_count == 1


def test_get_parameter_does_not_cache_none(wrapper, mock_inner_client):
    mock_inner_client.get_parameter.return_value = None

    wrapper.get_parameter('missing')
    wrapper.get_parameter('missing')
    assert mock_inner_client.get_parameter.call_count == 2


def test_put_parameter_updates_cache(wrapper, mock_inner_client):
    """
    put_parameter caches under the name returned by the inner client.
    The inner client may transform the name (e.g. SSM sanitization),
    so the cache key is whatever the inner client returns.
    """
    # By default fixture returns the same name back
    returned_name = wrapper.put_parameter('my-secret', 'new-value')
    assert returned_name == 'my-secret'

    # Subsequent get returns cached value (no inner call)
    result = wrapper.get_parameter('my-secret')
    assert result == 'new-value'
    mock_inner_client.get_parameter.assert_not_called()


def test_put_parameter_caches_under_returned_name(wrapper, mock_inner_client):
    """
    When inner client transforms the name (e.g. sanitization),
    the cache is populated under the TRANSFORMED name, not the original.
    """
    # Inner client transforms the name
    mock_inner_client.put_parameter.side_effect = None
    mock_inner_client.put_parameter.return_value = 'sanitized-name'

    returned_name = wrapper.put_parameter('original/name', 'value')
    assert returned_name == 'sanitized-name'

    # Cache hit on the returned (sanitized) name
    result = wrapper.get_parameter('sanitized-name')
    assert result == 'value'
    mock_inner_client.get_parameter.assert_not_called()


@pytest.mark.skip(
    reason="Known issue: cache misses when inner client transforms name. "
           "See TODO in SSMClientCachingWrapper.put_parameter"
)
def test_put_parameter_caches_under_original_name_too(
        wrapper, mock_inner_client,
):
    """
    When the inner client returns a different name than the one given,
    the cache should still serve subsequent reads under the ORIGINAL name.
    """
    mock_inner_client.put_parameter.side_effect = None
    mock_inner_client.put_parameter.return_value = 'sanitized-name'

    wrapper.put_parameter('original/name', 'value')

    # Reading with the ORIGINAL name should hit the cache
    result = wrapper.get_parameter('original/name')
    assert result == 'value'
    mock_inner_client.get_parameter.assert_not_called()


def test_put_parameter_returning_none_does_not_cache(
        wrapper, mock_inner_client,
):
    """If inner put_parameter fails (returns None), nothing is cached."""
    mock_inner_client.put_parameter.side_effect = None
    mock_inner_client.put_parameter.return_value = None

    result = wrapper.put_parameter('my-secret', 'value')
    assert result is None

    # Cache stays empty - subsequent get hits inner client
    wrapper.get_parameter('my-secret')
    assert mock_inner_client.get_parameter.call_count == 1


def test_delete_parameter_clears_cache(wrapper, mock_inner_client):
    wrapper.get_parameter('my-secret')
    assert mock_inner_client.get_parameter.call_count == 1

    assert wrapper.delete_parameter('my-secret') is True

    wrapper.get_parameter('my-secret')
    assert mock_inner_client.get_parameter.call_count == 2


def test_delete_nonexistent_key_does_not_raise(wrapper, mock_inner_client):
    result = wrapper.delete_parameter('never-cached')
    assert result is True


def test_cache_respects_maxsize(mock_inner_client, mock_env_service):
    """Smoke test: cache eviction works (maxsize=50 hardcoded in wrapper)."""
    wrapper = SSMClientCachingWrapper(
        client=mock_inner_client,
        environment_service=mock_env_service,
    )

    for i in range(60):
        mock_inner_client.get_parameter.return_value = f'value-{i}'
        wrapper.get_parameter(f'key-{i}')

    assert len(wrapper._cache) <= 50
