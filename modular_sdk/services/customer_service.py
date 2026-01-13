from typing import List, Optional, Any

from modular_sdk.models.customer import Customer, NAME_KEY, ACTIVE
from modular_sdk.models.pynamongo import ResultIterator
from modular_sdk.models.pynamongo.convertors import instance_as_json_dict


class CustomerService:
    @staticmethod
    def get(name: str) -> Optional[Customer]:
        return Customer.find_one(
            filter={NAME_KEY: name}
        )

    @staticmethod
    def list() -> List[Customer]:
        return list(Customer.find({}))

    @staticmethod
    def i_get_customer(attributes_to_get: Optional[List] = None,
                       is_active: Optional[bool] = None,
                       name: Optional[str] = None,
                       limit: Optional[int] = None,
                       last_evaluated_key: Optional[dict] = None,
                       rate_limit: Optional[int] = None
                       ) -> ResultIterator[Customer]:
        filter_dict: dict[str, Any] = {}
        if isinstance(is_active, bool):
            filter_dict[ACTIVE] = is_active
        if name:
            filter_dict[NAME_KEY] = name

        skip = last_evaluated_key if isinstance(last_evaluated_key, int) else 0

        return Customer.find(
            filter=filter_dict,
            projection=attributes_to_get,
            limit=limit,
            skip=skip,
            batch_size=rate_limit
        )

    @staticmethod
    def get_dto(customer: Customer) -> dict:
        return instance_as_json_dict(customer)
