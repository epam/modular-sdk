from typing import Optional

from pynamodb.pagination import ResultIterator

from modular_sdk.commons import RESPONSE_BAD_REQUEST_CODE
from modular_sdk.commons.exception import ModularException
from modular_sdk.models.tenant_settings import TenantSettings

RESOURCE_QUOTA = 'RESOURCE_QUOTA'


class TenantSettingsService:

    @staticmethod
    def create(tenant_name: str, key: str, value: Optional[dict] = None
               ) -> TenantSettings:
        return TenantSettings(
            tenant_name=tenant_name, key=key, value=value or dict()
        )

    @staticmethod
    def get(tenant_name: str, key: str) -> Optional[TenantSettings]:
        return TenantSettings.get_nullable(
            hash_key=tenant_name,
            range_key=key
        )

    @staticmethod
    def delete(entity: TenantSettings):
        entity.delete()

    @staticmethod
    def get_all_tenants(tenant) -> list:
        tenants = TenantSettings.query(hash_key=tenant)
        return list(tenants)

    @staticmethod
    def save(tenant_setting: TenantSettings):
        tenant_setting.save()

    @staticmethod
    def update(tenant_setting: TenantSettings, actions: list) -> None:
        tenant_setting.update(actions=actions)

    @staticmethod
    def get_tenant_by_name(tenant):
        tenants = TenantSettingsService.get_all_tenants(tenant=tenant)
        tenant_item = list(
            filter(lambda item:
                   item.attribute_values.get('key') == RESOURCE_QUOTA,
                   tenants))

        if not tenant_item:
            raise ModularException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Tenant with name {tenant} is not found'
            )

        return tenant_item[0]

    @staticmethod
    def i_get_by_tenant(tenant: str, key: Optional[str] = None,
                        limit: Optional[int] = None
                        ) -> ResultIterator[TenantSettings]:
        return TenantSettings.query(
            hash_key=tenant,
            limit=limit,
            range_key_condition=(TenantSettings.key == key) if key else None
        )

    @staticmethod
    def i_get_by_key(key: str, tenant: Optional[str] = None,
                     limit: Optional[int] = None
                     ) -> ResultIterator[TenantSettings]:
        fc = None
        if tenant:
            fc = (TenantSettings.tenant_name == tenant)
        return TenantSettings.key_tenant_name_index.query(
            hash_key=key,
            limit=limit,
            filter_condition=fc
        )
