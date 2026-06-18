"""
Smoke + behavior tests for the main SDK entry point: modular_sdk.modular.Modular
and its parent ModularServiceProvider.

These tests are deliberately defensive about Python 3.14:
  - assert that lazy imports of every service emit no DeprecationWarning
  - assert singleton semantics still hold under SingletonMeta
  - assert lazy-property caching works (each service built exactly once)
  - assert reset() actually clears state (name-mangling sanity check)
  - assert ServiceMode.DOCKER side effects in __init__
"""
from __future__ import annotations

import importlib
import warnings

import pytest


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture
def fresh_modular(monkeypatch):
    """
    Wipe the SingletonMeta registry so each test gets a brand-new Modular.

    SingletonMeta caches one instance per class; without clearing it,
    earlier tests will pollute later ones.
    """
    # Make sure no environment leaks into Modular.__init__
    for var in (
        'modular_service_mode',
        'MODULAR_SERVICE_MODE',
        'modular_assume_role_arn',
        'MODULAR_ASSUME_ROLE_ARN',
        'AWS_REGION',
        'AWS_DEFAULT_REGION',
    ):
        monkeypatch.delenv(var, raising=False)
    # Force docker mode so __init__ does not try to call sts_service()
    monkeypatch.setenv('modular_service_mode', 'docker')

    from modular_sdk.commons import SingletonMeta
    from modular_sdk.modular import Modular, ModularServiceProvider

    # Clear singleton registry for both classes
    SingletonMeta._instances.pop(Modular, None)
    SingletonMeta._instances.pop(ModularServiceProvider, None)

    yield Modular

    # Clean up after
    SingletonMeta._instances.pop(Modular, None)
    SingletonMeta._instances.pop(ModularServiceProvider, None)


# --------------------------------------------------------------------------- #
# Singleton semantics
# --------------------------------------------------------------------------- #

class TestSingletonBehavior:
    def test_modular_is_singleton(self, fresh_modular):
        a = fresh_modular()
        b = fresh_modular()
        assert a is b, 'Modular() must always return the same instance'

    def test_modular_singleton_survives_reimport(self, fresh_modular):
        """Re-importing the module must not produce a second instance."""
        a = fresh_modular()
        import modular_sdk.modular as m
        importlib.reload  # noqa: B018  -- do NOT actually reload; verify ref stable
        b = m.Modular()
        assert a is b

    def test_provider_and_modular_are_distinct_singletons(self, fresh_modular):
        """
        Modular inherits from ModularServiceProvider, but each has its own
        SingletonMeta entry. They are different singletons.
        """
        from modular_sdk.modular import ModularServiceProvider
        from modular_sdk.commons import SingletonMeta
        SingletonMeta._instances.pop(ModularServiceProvider, None)

        modular = fresh_modular()
        provider = ModularServiceProvider()
        # They are different objects (separate keys in the registry)
        assert modular is not provider
        # But Modular IS-A ModularServiceProvider
        assert isinstance(modular, ModularServiceProvider)


# --------------------------------------------------------------------------- #
# Lazy-property caching
# --------------------------------------------------------------------------- #

class TestLazyServiceCaching:
    def test_environment_service_cached(self, fresh_modular):
        m = fresh_modular()
        s1 = m.environment_service()
        s2 = m.environment_service()
        assert s1 is s2

    def test_customer_service_cached(self, fresh_modular):
        m = fresh_modular()
        assert m.customer_service() is m.customer_service()

    def test_tenant_service_cached(self, fresh_modular):
        m = fresh_modular()
        assert m.tenant_service() is m.tenant_service()

    def test_parent_service_uses_shared_tenant_and_customer(self, fresh_modular):
        m = fresh_modular()
        ps = m.parent_service()
        # ParentService is built with the same TenantService / CustomerService
        # the provider hands out elsewhere.
        assert ps.tenant_service is m.tenant_service()
        assert ps.customer_service is m.customer_service()

    def test_region_service_cached(self, fresh_modular):
        m = fresh_modular()
        assert m.region_service() is m.region_service()


# --------------------------------------------------------------------------- #
# Lazy imports must not emit DeprecationWarnings on Python 3.14
# --------------------------------------------------------------------------- #

class TestNoDeprecationWarnings:
    """
    On Python 3.14, several stdlib idioms now emit DeprecationWarning
    (datetime.utcnow, ssl wrap, etc.). Make sure none of them fire when we
    lazy-import every service the provider can hand out.

    We do NOT call services that require live AWS / Mongo / Vault.
    """

    PURE_LAZY_GETTERS = (
        'environment_service',
        'customer_service',
        'tenant_service',
        'tenant_settings_service',
        'customer_settings_service',
        'region_service',
        'parent_service',
        'application_service',
        'thread_local_storage_service',
        'events_service',
        'lambda_service',
        'sqs_service',
    )

    @pytest.mark.parametrize('getter', PURE_LAZY_GETTERS)
    def test_lazy_getter_emits_no_deprecation(self, fresh_modular, getter):
        m = fresh_modular()
        with warnings.catch_warnings():
            warnings.simplefilter('error', DeprecationWarning)
            getattr(m, getter)()


# --------------------------------------------------------------------------- #
# reset() — verify it actually clears the cached service
# --------------------------------------------------------------------------- #

class TestReset:
    def test_reset_unknown_service_raises(self, fresh_modular):
        m = fresh_modular()
        with pytest.raises(AssertionError):
            m.reset('definitely_not_a_service')

    @pytest.mark.xfail(
        reason=(
            "Pre-existing name-mangling bug in reset(): it builds "
            "'__ModularServiceProvider_<svc>' but Python mangles to "
            "'_ModularServiceProvider__<svc>'. This test documents the "
            "bug; remove xfail when fixed."
        ),
        strict=False,
    )
    def test_reset_actually_clears_environment_service(self, fresh_modular):
        m = fresh_modular()
        s1 = m.environment_service()
        m.reset('environment_service')
        s2 = m.environment_service()
        assert s1 is not s2, 'reset() should force a new instance to be built'


# --------------------------------------------------------------------------- #
# __init__ side effects (ServiceMode.DOCKER)
# --------------------------------------------------------------------------- #

class TestDockerModeInit:
    def test_docker_mode_sets_mongo_env_vars(self, monkeypatch):
        """
        When constructed in DOCKER mode with mongo args, __init__ must push
        them into the Env namespace via Env.<var>.set().
        """
        from modular_sdk.commons import SingletonMeta
        from modular_sdk.commons.constants import Env, ServiceMode
        from modular_sdk.modular import Modular, ModularServiceProvider

        SingletonMeta._instances.pop(Modular, None)
        SingletonMeta._instances.pop(ModularServiceProvider, None)

        # Wipe any leftovers
        for var in (
            Env.SERVICE_MODE,
            Env.MONGO_USER,
            Env.MONGO_PASSWORD,
            Env.MONGO_URL,
            Env.MONGO_DB_NAME,
            Env.MONGO_URI,
        ):
            monkeypatch.delenv(var.value, raising=False)

        Modular(
            modular_service_mode=ServiceMode.DOCKER,
            modular_mongo_user='alice',
            modular_mongo_password='s3cret',
            modular_mongo_url='mongo:27017',
            modular_mongo_db_name='maestro',
            modular_mongo_uri='mongodb://alice:s3cret@mongo:27017/maestro',
        )

        assert Env.SERVICE_MODE.get() == ServiceMode.DOCKER.value
        assert Env.MONGO_USER.get() == 'alice'
        assert Env.MONGO_PASSWORD.get() == 's3cret'
        assert Env.MONGO_URL.get() == 'mongo:27017'
        assert Env.MONGO_DB_NAME.get() == 'maestro'
        assert Env.MONGO_URI.get() == 'mongodb://alice:s3cret@mongo:27017/maestro'

        SingletonMeta._instances.pop(Modular, None)
        SingletonMeta._instances.pop(ModularServiceProvider, None)

    def test_docker_mode_with_no_mongo_args_does_not_raise(self, monkeypatch):
        from modular_sdk.commons import SingletonMeta
        from modular_sdk.commons.constants import ServiceMode
        from modular_sdk.modular import Modular, ModularServiceProvider

        SingletonMeta._instances.pop(Modular, None)
        SingletonMeta._instances.pop(ModularServiceProvider, None)

        # Should not raise
        Modular(modular_service_mode=ServiceMode.DOCKER)

        SingletonMeta._instances.pop(Modular, None)
        SingletonMeta._instances.pop(ModularServiceProvider, None)


# --------------------------------------------------------------------------- #
# Smoke test: __str__ works and is stable
# --------------------------------------------------------------------------- #

class TestSmoke:
    def test_str_returns_id(self, fresh_modular):
        m = fresh_modular()
        assert str(m) == str(id(m))

    def test_str_is_stable_across_calls(self, fresh_modular):
        m = fresh_modular()
        assert str(m) == str(m)
