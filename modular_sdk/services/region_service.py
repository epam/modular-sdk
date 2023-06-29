from typing import Optional
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.region import RegionModel, RegionAttr
from modular_sdk.services.tenant_service import TenantService

REGION_TABLE_NAME = 'Regions'
REGION_TABLE_HASH_KEY = 'r'
REGION_FIELDS_KEY = 'f'
REGION_CLOUD_KEY = 'c'
REGION_VIRT_PROFILES = 'vp'
REGION_SHAPE_MAPPING = 'shapeMapping'
REGION_NATIVE_NAME = 'nn'
REGION_ID = 'rId'

VIRT_PROFILE_ERROR_PATTERN = 'There is no virt profiles in region {0}'
SHAPE_MAPPING_ERROR_PATTERN = 'There is no shape mapping in region {0}'

_LOG = get_logger('modular_sdk-region-service')


def _extract_region_fields(region_item):
    _LOG = get_logger('_extract_region_fields')
    region_fields = region_item.fields
    if not region_fields:
        _LOG.error('There are no fields in region item')
        return dict()
    return region_fields


class RegionService:
    def __init__(self, tenant_service: TenantService):
        self.tenant_service = tenant_service

    @staticmethod
    def get_all_regions(only_active=False):
        regions = list(RegionModel.scan())
        if only_active:
            return list(filter(lambda region: region.is_active, regions))
        return regions

    @staticmethod
    def get_region(region_name):
        return RegionModel.get_nullable(hash_key=region_name)

    @staticmethod
    def get_region_by_native_name(native_name: str,
                                  cloud: Optional[str] = None
                                  ) -> Optional[RegionModel]:
        """
        Always returns one region or None. But still cloud should be specified
        """
        condition = None
        if cloud:
            condition = RegionModel.cloud == cloud
        return next(RegionModel.native_name_cloud_index.query(
            hash_key=native_name, range_key_condition=condition
        ), None)

    @staticmethod
    def get_regions(region_names):
        return RegionModel.batch_get(list(set(region_names)))

    @staticmethod
    def check_region_is_not_activated(region_to_add, tenant_regions):
        region_name = region_to_add.maestro_name
        for tenant_region in tenant_regions:
            if region_name == tenant_region.maestro_name:
                return False
        return True

    @staticmethod
    def get_dto(region):
        if isinstance(region, RegionModel):
            return region.get_json()
        if isinstance(region, RegionAttr):
            return region.as_dict()

    @staticmethod
    def region_model_to_attr(region: RegionModel):
        return RegionAttr(**region.get_json())
