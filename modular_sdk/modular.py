from typing import TYPE_CHECKING

from modular_sdk.commons import SingletonMeta
from modular_sdk.commons.constants import (
    ASSUMES_ROLE_SESSION_NAME,
    Env,
    SecretsBackend,
    ServiceMode,
)
from modular_sdk.commons.time_helper import utc_iso

if TYPE_CHECKING:
    from modular_sdk.connections.rabbit_connection import RabbitMqConnection
    from modular_sdk.services.application_service import ApplicationService
    from modular_sdk.services.customer_service import CustomerService
    from modular_sdk.services.customer_settings_service import (
        CustomerSettingsService,
    )
    from modular_sdk.services.environment_service import EnvironmentService
    from modular_sdk.services.events_service import EventsService
    from modular_sdk.services.impl.maestro_credentials_service import (
        MaestroCredentialsService,
    )
    from modular_sdk.services.impl.maestro_http_transport_service import (
        MaestroHTTPConfig,
        MaestroHTTPTransport,
    )
    from modular_sdk.services.impl.maestro_rabbit_transport_service import (
        MaestroRabbitMQTransport,
    )
    from modular_sdk.services.lambda_service import LambdaService
    from modular_sdk.services.parent_service import ParentService
    from modular_sdk.services.region_service import RegionService
    from modular_sdk.services.settings_management_service import (
        SettingsManagementService,
    )
    from modular_sdk.services.sqs_service import SQSService
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper
    from modular_sdk.services.sts_service import StsService
    from modular_sdk.services.tenant_service import TenantService
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )
    from modular_sdk.services.thread_local_storage_service import (
        ThreadLocalStorageService,
    )


class ModularServiceProvider(metaclass=SingletonMeta):
    # services
    __rabbit_conn = None
    __environment_service = None
    __customer_service = None
    __application_service = None
    __parent_service = None
    __region_service = None
    __tenant_service = None
    __tenant_settings_service = None
    __customer_settings_service = None
    __sts_service = None
    __sqs_service = None
    __lambda_service = None
    __events_service = None
    __rabbit_transport_service = None
    __http_transport_service = None
    __settings_service = None
    __instantiated_setting_group = []
    __credentials_service = None
    __thread_local_storage_service = None

    __ssm_service = None
    __assume_role_ssm_service = None

    def __str__(self):
        return str(id(self))

    def environment_service(self) -> 'EnvironmentService':
        if not self.__environment_service:
            from modular_sdk.services.environment_service import (
                EnvironmentService,
            )

            self.__environment_service = EnvironmentService()
        return self.__environment_service

    def application_service(self) -> 'ApplicationService':
        if not self.__application_service:
            from modular_sdk.services.application_service import (
                ApplicationService,
            )

            self.__application_service = ApplicationService(
                customer_service=self.customer_service()
            )
        return self.__application_service

    def customer_service(self) -> 'CustomerService':
        if not self.__customer_service:
            from modular_sdk.services.customer_service import CustomerService

            self.__customer_service = CustomerService()
        return self.__customer_service

    def parent_service(self) -> 'ParentService':
        if not self.__parent_service:
            from modular_sdk.services.parent_service import ParentService

            self.__parent_service = ParentService(
                tenant_service=self.tenant_service(),
                customer_service=self.customer_service(),
            )
        return self.__parent_service

    def region_service(self) -> 'RegionService':
        if not self.__region_service:
            from modular_sdk.services.region_service import RegionService

            self.__region_service = RegionService(
                tenant_service=self.tenant_service()
            )
        return self.__region_service

    def tenant_service(self) -> 'TenantService':
        if not self.__tenant_service:
            from modular_sdk.services.tenant_service import TenantService

            self.__tenant_service = TenantService()
        return self.__tenant_service

    def tenant_settings_service(self) -> 'TenantSettingsService':
        if not self.__tenant_settings_service:
            from modular_sdk.services.tenant_settings_service import (
                TenantSettingsService,
            )

            self.__tenant_settings_service = TenantSettingsService()
        return self.__tenant_settings_service

    def customer_settings_service(self) -> 'CustomerSettingsService':
        if not self.__customer_settings_service:
            from modular_sdk.services.customer_settings_service import (
                CustomerSettingsService,
            )

            self.__customer_settings_service = CustomerSettingsService()
        return self.__customer_settings_service

    def sts_service(self) -> 'StsService':
        if not self.__sts_service:
            from modular_sdk.services.sts_service import StsService

            self.__sts_service = StsService(
                environment_service=self.environment_service(),
                aws_region=self.environment_service().aws_region(),
            )
        return self.__sts_service

    def sqs_service(self) -> 'SQSService':
        if not self.__sqs_service:
            from modular_sdk.services.sqs_service import SQSService

            self.__sqs_service = SQSService(
                aws_region=self.environment_service().aws_region(),
                environment_service=self.environment_service(),
            )
        return self.__sqs_service

    def lambda_service(self) -> 'LambdaService':
        if not self.__lambda_service:
            from modular_sdk.services.lambda_service import LambdaService

            self.__lambda_service = LambdaService(
                aws_region=self.environment_service().aws_region()
            )
        return self.__lambda_service

    def events_service(self) -> 'EventsService':
        if not self.__events_service:
            from modular_sdk.services.events_service import EventsService

            self.__events_service = EventsService(
                aws_region=self.environment_service().aws_region()
            )
        return self.__events_service

    def rabbit(
        self, connection_url, timeout=None, refresh=False
    ) -> 'RabbitMqConnection':
        if not self.__rabbit_conn or refresh:
            from modular_sdk.connections.rabbit_connection import (
                RabbitMqConnection,
            )

            self.__rabbit_conn = RabbitMqConnection(
                connection_url=connection_url, timeout=timeout
            )
        return self.__rabbit_conn

    def rabbit_transport_service(
        self, connection_url, config, timeout=None
    ) -> 'MaestroRabbitMQTransport':
        if not self.__rabbit_transport_service:
            from modular_sdk.services.impl.maestro_rabbit_transport_service import (
                MaestroRabbitMQTransport,
            )

            rabbit_connection = self.rabbit(
                connection_url=connection_url, timeout=timeout, refresh=True
            )
            self.__rabbit_transport_service = MaestroRabbitMQTransport(
                rabbit_connection=rabbit_connection, config=config
            )
        return self.__rabbit_transport_service

    def http_transport_service(
        self,
        api_link: str,
        config: 'MaestroHTTPConfig',
        timeout: int | None = None,
    ) -> 'MaestroHTTPTransport':
        if not self.__http_transport_service:
            from modular_sdk.services.impl.maestro_http_transport_service import (
                MaestroHTTPTransport,
            )

            self.__http_transport_service = MaestroHTTPTransport(
                config=config, api_link=api_link, timeout=timeout
            )
        return self.__http_transport_service

    def settings_service(self, group_name) -> 'SettingsManagementService':
        if (
            not self.__settings_service
            or group_name not in self.__instantiated_setting_group
        ):
            from modular_sdk.services.settings_management_service import (
                SettingsManagementService,
            )

            self.__settings_service = SettingsManagementService(
                group_name=group_name
            )
        return self.__settings_service

    def ssm_service(self) -> 'SSMClientCachingWrapper':
        if not self.__ssm_service:
            from modular_sdk.services.ssm_service import (
                SSMClientCachingWrapper,
                SSMService,
                VaultSSMClient,
            )

            backend = Env.SECRETS_BACKEND.get()

            if backend == SecretsBackend.VAULT:
                self.__ssm_service = VaultSSMClient()
            elif backend == SecretsBackend.SSM:
                self.__ssm_service = SSMService(
                    aws_region=self.environment_service().aws_region()
                )
            else:
                raise RuntimeError(f'Unknown secrets backend type: {backend}')
            self.__ssm_service = SSMClientCachingWrapper(
                client=self.__ssm_service,
                environment_service=self.environment_service(),
            )
        return self.__ssm_service

    def assume_role_ssm_service(self) -> 'SSMClientCachingWrapper':
        if not self.__assume_role_ssm_service:
            from modular_sdk.services.ssm_service import (
                ModularAssumeRoleSSMService,
                SSMClientCachingWrapper,
                VaultSSMClient,
            )

            backend = Env.SECRETS_BACKEND.get()

            if backend == SecretsBackend.VAULT:
                self.__assume_role_ssm_service = VaultSSMClient()
            elif backend == SecretsBackend.SSM:
                self.__assume_role_ssm_service = ModularAssumeRoleSSMService()
            else:
                raise RuntimeError(f'Unknown secrets backend type: {backend}')
            self.__assume_role_ssm_service = SSMClientCachingWrapper(
                client=self.__assume_role_ssm_service,
                environment_service=self.environment_service(),
            )
        return self.__assume_role_ssm_service

    def maestro_credentials_service(self) -> 'MaestroCredentialsService':
        if not self.__credentials_service:
            from modular_sdk.services.impl.maestro_credentials_service import (
                MaestroCredentialsService,
            )

            self.__credentials_service = MaestroCredentialsService.build()
        return self.__credentials_service

    def thread_local_storage_service(self) -> 'ThreadLocalStorageService':
        if not self.__thread_local_storage_service:
            from modular_sdk.services.thread_local_storage_service import (
                ThreadLocalStorageService,
            )

            self.__thread_local_storage_service = ThreadLocalStorageService()
        return self.__thread_local_storage_service

    def reset(self, service: str):
        """
        Removes the saved instance of the service. It is useful,
        for example, in case of gitlab service - when we want to use
        different rule-sources configurations
        """
        private_service_name = f'__ModularServiceProvider_{service}'
        if not hasattr(self, private_service_name):
            raise AssertionError(
                f'In case you are using this method, make sure your '
                f'service {private_service_name} exists amongst the '
                f'private attributes'
            )
        setattr(self, private_service_name, None)


class Modular(ModularServiceProvider, metaclass=SingletonMeta):
    def __init__(
        self,
        *,
        modular_service_mode: str | ServiceMode | None = None,
        modular_mongo_user: str | None = None,
        modular_mongo_password: str | None = None,
        modular_mongo_url: str | None = None,
        modular_mongo_srv: bool = False,
        modular_mongo_db_name: str | None = None,
        modular_assume_role_arn: str | list[str] | None = None,
        modular_mongo_uri: str | None = None,
        **kwargs,
    ):
        if isinstance(modular_service_mode, ServiceMode):
            modular_service_mode = modular_service_mode.value
        if modular_service_mode is None:
            modular_service_mode = Env.SERVICE_MODE.get()
        if modular_assume_role_arn is None:
            modular_service_mode = Env.ASSUME_ROLE_ARN.get()

        if (
            modular_service_mode == ServiceMode.SAAS
            and modular_assume_role_arn
        ):
            if isinstance(modular_assume_role_arn, str):
                modular_assume_role_arn = modular_assume_role_arn.split(',')
            sts_service = self.sts_service()
            assumed_credentials = sts_service.assume_roles_chain(
                list(
                    sts_service.assume_roles_default_payloads(
                        modular_assume_role_arn, ASSUMES_ROLE_SESSION_NAME
                    )
                )
            )
            Env.INNER_AWS_ACCESS_KEY_ID.set(
                assumed_credentials['aws_access_key_id']
            )
            Env.INNER_AWS_SECRET_ACCESS_KEY.set(
                assumed_credentials['aws_secret_access_key']
            )
            Env.INNER_AWS_SESSION_TOKEN.set(
                assumed_credentials['aws_session_token']
            )
            Env.INNER_AWS_CREDENTIALS_EXPIRATION.set(
                utc_iso(assumed_credentials['expiration'])
            )
            Env.ASSUME_ROLE_ARN.set(','.join(modular_assume_role_arn))
        elif modular_service_mode == ServiceMode.DOCKER:
            Env.SERVICE_MODE.set(modular_service_mode)
            if modular_mongo_user is not None:
                Env.MONGO_USER.set(modular_mongo_user)
            if modular_mongo_password is not None:
                Env.MONGO_PASSWORD.set(modular_mongo_password)
            if modular_mongo_url is not None:
                Env.MONGO_URL.set(modular_mongo_url)
            if modular_mongo_srv:
                Env.MONGO_SRV.set(str(modular_mongo_srv))
            if modular_mongo_db_name is not None:
                Env.MONGO_DB_NAME.set(modular_mongo_db_name)
            if modular_mongo_uri is not None:
                Env.MONGO_URI.set(modular_mongo_uri)
