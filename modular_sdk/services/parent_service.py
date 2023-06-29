from typing import Optional, Iterator, Union, List

from modular_sdk.commons import RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE
from modular_sdk.commons import generate_id
from modular_sdk.commons.constants import ALL_PARENT_TYPES
from modular_sdk.commons.exception import ModularException
from modular_sdk.commons.log_helper import get_logger
from pynamodb.expressions.condition import Condition

from modular_sdk.commons.time_helper import utc_iso
from modular_sdk.models.parent import Parent
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.tenant_service import TenantService

_LOG = get_logger('modular_sdk-parent-service')


class ParentService:

    def __init__(self, tenant_service: TenantService,
                 customer_service: CustomerService):
        self.customer_service = customer_service
        self.tenant_service = tenant_service

    @staticmethod
    def get_parent_by_id(parent_id):
        return Parent.get_nullable(hash_key=parent_id)

    @staticmethod
    def list():
        return list(Parent.scan())

    @staticmethod
    def list_application_parents(application_id, only_active=True):
        return list(ParentService.i_list_application_parents(
            application_id=application_id, only_active=only_active
        ))

    @staticmethod
    def i_list_application_parents(application_id: str, only_active=True,
                                   limit: Optional[int] = None,
                                   last_evaluated_key: Optional[dict] = None
                                   ) -> Iterator[Parent]:
        condition = Parent.application_id == application_id
        if only_active:
            condition &= (Parent.is_deleted == False)
        return Parent.scan(filter_condition=condition, limit=limit,
                           last_evaluated_key=last_evaluated_key)

    @staticmethod
    def i_get_parent_by_customer(
            customer_id: str, 
            parent_type: Optional[Union[str, List[str]]] = None,
            is_deleted: Optional[bool] = None,
            meta_conditions: Optional[Condition] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[dict] = None
    ) -> Iterator[Parent]:
        """
        Meta conditions can be used like this:
        parent = next(parent_service.i_get_parent_by_customer(
            customer_id='EPAM Systems',
            parent_type='CUSTODIAN',
            is_deleted=False,
            meta_conditions=(Parent.meta['scope'] == 'ALL') & Parent.meta['clouds'].contains('AWS'),
            limit=1
        ), None)
        :param customer_id:
        :param parent_type:
        :param is_deleted:
        :param meta_conditions:
        :param limit:
        :param last_evaluated_key:
        :return:
        """
        condition = meta_conditions
        if parent_type:  # list or str
            types = parent_type if isinstance(parent_type, list) else [parent_type]
            condition &= (Parent.type.is_in(*types))
        if isinstance(is_deleted, bool):
            condition &= (Parent.is_deleted == is_deleted)
        return Parent.customer_id_type_index.query(
            hash_key=customer_id,
            filter_condition=condition,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def create(self, application_id, customer_id, parent_type: str,
               is_deleted=False, description=None, meta=None):
        if parent_type not in ALL_PARENT_TYPES:
            _LOG.error(f'Invalid parent type specified \'{parent_type}\'. '
                       f'Available options: {ALL_PARENT_TYPES}')
            raise ModularException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Invalid parent type specified \'{parent_type}\'. '
                        f'Available options are: {ALL_PARENT_TYPES} or '
                        f'$APPLICATION:$ENTITY.'
            )
        customer = self.customer_service.get(name=customer_id)
        if not customer:
            _LOG.error(f'Customer with name \'{customer_id}\' does not exist')
            raise ModularException(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Customer with name \'{customer_id}\' does not exist'
            )
        parent = Parent(
            parent_id=generate_id(),
            customer_id=customer_id,
            type=parent_type,
            application_id=application_id,
            is_deleted=is_deleted,
        )
        if description:
            parent.description = description
        if meta and isinstance(meta, dict):
            parent.meta = meta
        return parent

    @staticmethod
    def get_dto(parent: Parent):
        return parent.get_json()

    @staticmethod
    def save(parent: Parent):
        parent.save()

    def mark_deleted(self, parent: Parent):
        if parent.is_deleted:
            _LOG.warning(f'Parent with id \'{parent.parent_id}\' '
                         f'already deleted.')
            raise ModularException(
                code=RESPONSE_OK_CODE,
                content=f'Parent with id \'{parent.parent_id}\' '
                        f'already deleted.'
            )
        _LOG.debug(f'Searching for parent tenants')
        parent_tenant = next(self.tenant_service.i_get_tenant_by_parent_id(
            parent_id=parent.parent_id,
            active=True,
            limit=1
        ), None)
        if parent_tenant:
            raise ModularException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'There are active tenants associated '
                        f'with parent: {parent_tenant.name}'
            )
        _LOG.debug(f'Deleting parent')
        parent.is_deleted = True
        parent.deletion_date = utc_iso()
