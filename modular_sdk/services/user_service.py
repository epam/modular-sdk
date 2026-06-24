from typing import List, Optional

from http import HTTPStatus

from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator

from modular_sdk.commons.exception import ModularException
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.commons.time_helper import java_timestamp
from modular_sdk.models.pynamongo.convertors import instance_as_json_dict
from modular_sdk.models.user import TenantMapping, User
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.tenant_service import TenantService

_LOG = get_logger(__name__)


class UserService:

    def __init__(
        self,
        customer_service: CustomerService,
        tenant_service: TenantService,
    ):
        self.customer_service = customer_service
        self.tenant_service = tenant_service

    def create(
        self,
        email: str,
        customer_id: str,
        user_name: str,
        display_email: str,
        type: str,
        role: Optional[str] = None,
        identity_id: Optional[str] = None,
        unique_user_id: Optional[str] = None,
        allowed_tenants: Optional[list] = None,
        tenant_mappings: Optional[list] = None,
    ) -> User:
        if not self.customer_service.get(name=customer_id):
            _LOG.error(
                f'Customer with name \'{customer_id}\' does not exist.'
            )
            raise ModularException(
                code=HTTPStatus.NOT_FOUND.value,
                content=(
                    f'Customer with name \'{customer_id}\' does not exist.'
                ),
            )
        timestamp = int(java_timestamp())
        return User(
            email=email,
            customer_id=customer_id,
            user_name=user_name,
            display_email=display_email,
            role=role,
            type=type,
            identity_id=identity_id,
            unique_user_id=unique_user_id,
            allowed_tenants=allowed_tenants or [],
            tenant_mappings=tenant_mappings or [],
            last_update_date=timestamp,
        )

    @staticmethod
    def get(
        email: str,
        attributes_to_get: Optional[list] = None,
    ) -> Optional[User]:
        return User.get_nullable(
            hash_key=email,
            attributes_to_get=attributes_to_get,
        )

    @staticmethod
    def list() -> List[User]:
        return list(User.scan())

    @staticmethod
    def i_list(
        customer_id: Optional[str] = None,
        email: Optional[str] = None,
        blocked: Optional[bool] = None,
        identity_id: Optional[str] = None,
        unique_user_id: Optional[str] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[dict] = None,
        rate_limit: Optional[int] = None,
        filter_condition: Optional[Condition] = None,
    ) -> ResultIterator[User]:
        condition = filter_condition
        if condition is not None and customer_id:
            condition &= User.customer_id == customer_id
        elif customer_id:
            condition = User.customer_id == customer_id
        if condition is not None and email:
            condition &= User.email == email
        elif email:
            condition = User.email == email
        if condition is not None and isinstance(blocked, bool):
            condition &= User.blocked == blocked
        elif isinstance(blocked, bool):
            condition = User.blocked == blocked
        if condition is not None and identity_id:
            condition &= User.identity_id == identity_id
        elif identity_id:
            condition = User.identity_id == identity_id
        if condition is not None and unique_user_id:
            condition &= User.unique_user_id == unique_user_id
        elif unique_user_id:
            condition = User.unique_user_id == unique_user_id
        return User.scan(
            filter_condition=condition,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            rate_limit=rate_limit,
        )

    @staticmethod
    def save(user: User):
        user.save()

    def add_tenant_mapping(
        self,
        user: User,
        tenant: str,
        positions: Optional[list] = None,
        expiration: Optional[int] = None,
        tenant_approval: Optional[dict] = None,
    ) -> User:
        if not self.tenant_service.does_exist(tenant_name=tenant):
            _LOG.warning(f'Tenant \'{tenant}\' does not exist.')
            raise ModularException(
                code=HTTPStatus.NOT_FOUND.value,
                content=f'Tenant \'{tenant}\' does not exist.',
            )
        for mapping in user.tenant_mappings:
            if mapping.tenant == tenant:
                _LOG.warning(
                    f'User \'{user.email}\' already has mapping '
                    f'for tenant \'{tenant}\'.'
                )
                raise ModularException(
                    code=HTTPStatus.BAD_REQUEST.value,
                    content=(
                        f'User \'{user.email}\' already has mapping '
                        f'for tenant \'{tenant}\'.'
                    ),
                )
        tenant_mappings = list(user.tenant_mappings)
        tenant_mappings.append(
            TenantMapping(
                tenant=tenant,
                positions=positions or [],
                expiration=expiration,
                tenant_approval=tenant_approval or {},
            )
        )
        self._save_tenant_mappings(user, tenant_mappings)
        return user

    def update_tenant_mapping(
        self,
        user: User,
        tenant: str,
        positions: Optional[list] = None,
        expiration: Optional[int] = None,
        tenant_approval: Optional[dict] = None,
    ) -> User:
        tenant_mappings = list(user.tenant_mappings)
        for index, mapping in enumerate(tenant_mappings):
            if mapping.tenant != tenant:
                continue
            tenant_mappings[index] = TenantMapping(
                tenant=tenant,
                positions=(
                    positions
                    if positions is not None
                    else list(mapping.positions)
                ),
                expiration=(
                    expiration
                    if expiration is not None
                    else mapping.expiration
                ),
                tenant_approval=(
                    tenant_approval
                    if tenant_approval is not None
                    else (
                        mapping.tenant_approval.as_dict()
                        if mapping.tenant_approval is not None
                        else {}
                    )
                ),
            )
            self._save_tenant_mappings(user, tenant_mappings)
            return user
        _LOG.warning(
            f'User \'{user.email}\' has no mapping for tenant \'{tenant}\'.'
        )
        raise ModularException(
            code=HTTPStatus.NOT_FOUND.value,
            content=(
                f'User \'{user.email}\' has no mapping for tenant \'{tenant}\'.'
            ),
        )

    def remove_tenant_mapping(self, user: User, tenant: str) -> User:
        tenant_mappings = list(user.tenant_mappings)
        updated_mappings = [
            mapping for mapping in tenant_mappings
            if mapping.tenant != tenant
        ]
        if len(updated_mappings) == len(tenant_mappings):
            _LOG.warning(
                f'User \'{user.email}\' has no mapping for tenant \'{tenant}\'.'
            )
            raise ModularException(
                code=HTTPStatus.NOT_FOUND.value,
                content=(
                    f'User \'{user.email}\' has no mapping '
                    f'for tenant \'{tenant}\'.'
                ),
            )
        self._save_tenant_mappings(user, updated_mappings)
        return user

    @staticmethod
    def _save_tenant_mappings(user: User, tenant_mappings: list) -> None:
        user.update(actions=[
            User.tenant_mappings.set(tenant_mappings),
            User.last_update_date.set(int(java_timestamp())),
        ])

    @staticmethod
    def update(user: User, attributes: List) -> None:
        updatable_attributes = [
            User.customer_id,
            User.allowed_tenants,
            User.role,
            User.identity_id,
            User.type,
            User.didevice_info,
            User.unique_user_id,
            User.user_name,
            User.display_email,
            User.profile_photo_url,
            User.activation,
            User.expiration,
            User.last_login_date,
            User.blocked,
            User.user_additional_params,
            User.jmx,
            User.jmx_permissions,
            User.tenant_mappings,
            User.admin_secret_key,
            User.admin_for_customers,
            User.custom_placeholder,
            User.api_user,
            User.api_rate_limit,
            User.description,
            User.block_reason,
            User.block_initiator,
            User.block_expiration,
        ]

        actions = []
        for attribute in attributes:
            if attribute not in updatable_attributes:
                _LOG.warning(
                    f'Attribute {attribute.attr_name} can\'t be updated.'
                )
                continue
            python_attr_name = User._dynamo_to_python_attr(
                attribute.attr_name
            )
            update_value = getattr(user, python_attr_name)
            actions.append(attribute.set(update_value))

        actions.append(
            User.last_update_date.set(int(java_timestamp()))
        )
        user.update(actions=actions)

    @staticmethod
    def get_dto(user: User) -> dict:
        return instance_as_json_dict(user)
