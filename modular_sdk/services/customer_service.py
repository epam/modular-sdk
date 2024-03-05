from typing import List, Optional

from pynamodb.pagination import ResultIterator

from modular_sdk.models.customer import Customer


class CustomerService:
    @staticmethod
    def get(name: str) -> Optional[Customer]:
        return Customer.get_nullable(hash_key=name)

    @staticmethod
    def list() -> List[Customer]:
        return list(Customer.scan())

    @staticmethod
    def i_get_customer(attributes_to_get: Optional[List] = None,
                       is_active: Optional[bool] = None,
                       name: Optional[str] = None,
                       limit: Optional[int] = None,
                       last_evaluated_key: Optional[dict] = None,
                       rate_limit: Optional[int] = None
                       ) -> ResultIterator[Customer]:
        condition = None
        if isinstance(is_active, bool):
            condition &= (Customer.is_active == is_active)
        if name:
            condition &= (Customer.name == name)
        return Customer.scan(
            attributes_to_get=attributes_to_get,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            rate_limit=rate_limit,
            filter_condition=condition
        )

    @staticmethod
    def get_dto(customer: Customer) -> dict:
        return customer.get_json()
