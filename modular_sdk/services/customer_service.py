from typing import Optional, List, Iterable

from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.customer import Customer

_LOG = get_logger(__name__)


class CustomerService:
    @staticmethod
    def get(name: str) -> Optional[Customer]:
        return Customer.get_nullable(hash_key=name)

    @staticmethod
    def list() -> List[Customer]:
        return list(Customer.scan())

    @staticmethod
    def i_get_customer(attributes_to_get: Optional[List] = None,
                       limit: Optional[int] = None
                       ) -> Iterable[Customer]:
        return Customer.scan(
            attributes_to_get=attributes_to_get,
            limit=limit
        )

    @staticmethod
    def get_dto(customer: Customer) -> dict:
        return customer.get_json()
