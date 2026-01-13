from typing import Optional, List, Any
from enum import Enum
from http import HTTPStatus

from modular_sdk.commons import generate_id
from modular_sdk.commons.exception import ModularException
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.commons.time_helper import utc_datetime
from modular_sdk.models.application import Application
from modular_sdk.models.pynamongo import ResultIterator
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.models.pynamongo.convertors import instance_as_json_dict, convert_condition_expression

_LOG = get_logger(__name__)


class ApplicationService:

    def __init__(self, customer_service: CustomerService):
        self.customer_service = customer_service

    def build(self, customer_id: str, type: str, description: str,
              created_by: str, application_id: Optional[str] = None,
              is_deleted=False, meta: Optional[dict] = None,
              secret: Optional[str] = None) -> Application:
        application_id = application_id or generate_id()
        if not self.customer_service.get(name=customer_id):
            _LOG.error(f'Customer with name \'{customer_id}\' does not exist.')
            raise ModularException(
                code=HTTPStatus.NOT_FOUND.value,
                content=f'Customer with name \'{customer_id}\' does not exist.'
            )
        return Application(
            application_id=application_id,
            customer_id=customer_id,
            type=type.value if isinstance(type, Enum) else type,
            description=description,
            is_deleted=is_deleted,
            meta=meta,
            secret=secret,
            created_by=created_by
        )

    @staticmethod
    def get_application_by_id(application_id) -> Optional[Application]:
        return Application.find_one(
            filter={'aid': application_id}
        )

    @staticmethod
    def i_get_application_by_customer(
            customer_id: str, application_type: Optional[str] = None,
            deleted: Optional[bool] = None
    ) -> ResultIterator[Application]:
        filter_dict: dict[str, Any] = {'cid': customer_id}
        if application_type:
            filter_dict['t'] = application_type
        if deleted is not None:
            filter_dict['d'] = deleted

        return Application.find(filter=filter_dict)

    @staticmethod
    def query_by_customer(customer: str, 
                          range_key_condition: Optional = None,
                          filter_condition: Optional = None,
                          limit: Optional[int] = None,
                          last_evaluated_key: Optional[dict | int] = None,
                          rate_limit: Optional[int] = None
                          ) -> ResultIterator[Application]:
        """
        Allows to specify flexible conditions
        """
        filter_dict: dict[str, Any] = {'cid': customer}
        
        if range_key_condition is not None:
            range_condition_dict = convert_condition_expression(range_key_condition)
            filter_dict.update(range_condition_dict)
        
        if filter_condition is not None:
            filter_condition_dict = convert_condition_expression(filter_condition)
            if len(filter_dict) > 1 or len(filter_condition_dict) > 1:
                # Merge conditions using $and if we have multiple conditions
                filter_dict = {'$and': [filter_dict, filter_condition_dict]}
            else:
                filter_dict.update(filter_condition_dict)
        
        skip = last_evaluated_key if isinstance(last_evaluated_key, int) else 0
        
        return Application.find(
            filter=filter_dict,
            limit=limit,
            skip=skip,
            batch_size=rate_limit
        )

    @staticmethod
    def list(customer: Optional[str] = None, _type: Optional[str] = None,
             deleted: Optional[bool] = None,
             limit: Optional[int] = None,
             last_evaluated_key: dict | int | None = None
             ) -> ResultIterator[Application]:
        filter_dict: dict[str, Any] = {}
        
        if customer:
            filter_dict['cid'] = customer
        if isinstance(_type, str):
            filter_dict['t'] = _type
        if isinstance(deleted, bool):
            filter_dict['d'] = deleted
        
        skip = last_evaluated_key if isinstance(last_evaluated_key, int) else 0
        
        return Application.find(
            filter=filter_dict,
            limit=limit,
            skip=skip
        )

    @staticmethod
    def save(application: Application):
        application.save()

    def update_meta(self, application: Application, updated_by: str):
        _LOG.debug(f'Going to update application {application.application_id}'
                   f'meta')

        self.update(
            application=application,
            attributes=[
                Application.meta
            ],
            updated_by=updated_by
        )
        _LOG.debug('Application meta was updated')

    @staticmethod
    def update(application: Application, attributes: List, updated_by: str):
        updatable_attributes = [
            Application.description,
            Application.meta,
            Application.secret,
            Application.updated_by,
            Application.is_deleted
        ]

        actions = []

        for attribute in attributes:
            # Use identity check instead of 'in' to avoid boolean evaluation
            # issues with pynamodb Comparison objects
            if not any(attr.attr_name == attribute.attr_name for attr in updatable_attributes):
                _LOG.warning(f'Attribute {attribute.attr_name} '
                             f'can\'t be updated.')
                continue
            python_attr_name = Application._dynamo_to_python_attr(
                attribute.attr_name)
            update_value = getattr(application, python_attr_name)
            actions.append(attribute.set(update_value))

        actions.append(Application.updated_by.set(updated_by))
        actions.append(Application.update_timestamp.set(
            int(utc_datetime().timestamp() * 1e3)))

        application.update(actions=actions)

    @staticmethod
    def get_dto(application: Application) -> dict:
        return instance_as_json_dict(application)

    @staticmethod
    def mark_deleted(application: Application):
        _LOG.debug(f'Going to mark the application '
                   f'{application.application_id} as deleted')
        if application.is_deleted:
            _LOG.warning(f'Application \'{application.application_id}\' '
                         f'is already deleted.')
            return
        application.update(actions=[
            Application.is_deleted.set(True),
            Application.deletion_timestamp.set(
                int(utc_datetime().timestamp() * 1e3))
        ])
        _LOG.debug('Application was marked as deleted')

    @staticmethod
    def force_delete(application: Application):
        _LOG.debug(f'Going to delete application {application.application_id}')
        application.delete()
        _LOG.debug('Application has been deleted')
