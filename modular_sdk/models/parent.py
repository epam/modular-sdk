from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    BooleanAttribute, NumberAttribute

from modular_sdk.models.pynamodb_extension.base_role_access_model import \
    BaseRoleAccessModel

from pynamodb.indexes import AllProjection

from modular_sdk.models.pynamodb_extension.base_model import BaseGSI
from modular_sdk.models.base_meta import BaseMeta, TABLES_PREFIX

PARENT_ID = 'pid'
CUSTOMER_ID = 'cid'
APPLICATION_ID = 'aid'
TYPE = 't'
DESCRIPTION = 'descr'
META = 'meta'
IS_DELETED = 'd'
DELETION_DATE = 'dd'
CREATION_TIMESTAMP = 'ct'
UPDATE_TIMESTAMP = 'ut'
DELETION_TIMESTAMP = 'dt'

MODULAR_PARENTS_TABLE_NAME = 'Parents'


class CustomerIdTypeIndex(BaseGSI):
    class Meta(BaseMeta):
        index_name = f"{CUSTOMER_ID}-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_id = UnicodeAttribute(hash_key=True, attr_name=CUSTOMER_ID)
    # type = UnicodeAttribute(range_key=True, attr_name=TYPE)


class Parent(BaseRoleAccessModel):
    class Meta(BaseMeta):
        table_name = f'{TABLES_PREFIX}{MODULAR_PARENTS_TABLE_NAME}'

    parent_id = UnicodeAttribute(hash_key=True, attr_name=PARENT_ID)
    customer_id = UnicodeAttribute(attr_name=CUSTOMER_ID)
    application_id = UnicodeAttribute(attr_name=APPLICATION_ID)
    type = UnicodeAttribute(attr_name=TYPE)
    description = UnicodeAttribute(attr_name=DESCRIPTION, null=True)
    meta = MapAttribute(attr_name=META, default=dict)
    is_deleted = BooleanAttribute(attr_name=IS_DELETED)
    deletion_date = UnicodeAttribute(attr_name=DELETION_DATE, null=True)
    creation_timestamp = NumberAttribute(attr_name=CREATION_TIMESTAMP, null=True)
    update_timestamp = NumberAttribute(attr_name=UPDATE_TIMESTAMP, null=True)
    deletion_timestamp = NumberAttribute(attr_name=DELETION_TIMESTAMP, null=True)

    customer_id_type_index = CustomerIdTypeIndex()
