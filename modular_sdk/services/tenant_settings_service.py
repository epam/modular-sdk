from typing import Optional, Iterator

from modular_sdk.commons import RESPONSE_BAD_REQUEST_CODE
from modular_sdk.commons.exception import ModularException
from modular_sdk.models.tenant_settings import TenantSettings

RESOURCE_QUOTA = 'RESOURCE_QUOTA'


class TenantSettingsService:

    @staticmethod
    def create(tenant_name: str, key: str, value: Optional[dict] = None):
        return TenantSettings(
            tenant_name=tenant_name, key=key, value=value or dict()
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
    def i_get_by_tenant(tenant: str, key: Optional[str] = None) ->\
            Iterator[TenantSettings]:

        return TenantSettings.query(
            hash_key=tenant,
            range_key_condition=(TenantSettings.key == key) if key else None
        )

    @staticmethod
    def i_get_by_key(key: str, tenant: Optional[str] = None) ->\
            Iterator[TenantSettings]:
        # TODO use range_key_condition instead of filter_condition if
        #  key_tenant_name_index has range_key tenant_name
        return TenantSettings.key_tenant_name_index.query(
            hash_key=key, filter_condition=(
                TenantSettings.tenant_name == tenant
            ) if tenant else None
        )

