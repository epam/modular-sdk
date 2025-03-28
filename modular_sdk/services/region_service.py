from typing import Optional
from modular_sdk.models.region import RegionModel, RegionAttr
from modular_sdk.services.tenant_service import TenantService
from modular_sdk.models.pynamongo.convertors import instance_as_json_dict, instance_as_dict

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
            return instance_as_json_dict(region)
        if isinstance(region, RegionAttr):
            return region.as_dict()

    @staticmethod
    def region_model_to_attr(region: RegionModel):
        return RegionAttr(**instance_as_dict(region))
