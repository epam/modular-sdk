from typing import Union, Optional, List
from pynamodb.expressions.update import Action
from pynamodb.expressions.condition import Condition

from modular_sdk.models.customer_settings import CustomerSettings, \
    CUSTOMER_NAME, KEY
from modular_sdk.models.pynamongo import ResultIterator


class CustomerSettingsService:

    @staticmethod
    def create(customer_name: str, key: str, 
               value: Union[list, dict, bool, str, int, float, None]
               ) -> CustomerSettings:
        return CustomerSettings(
            customer_name=customer_name,
            key=key,
            value=value
        )

    @staticmethod
    def get_nullable(customer_name: str, key: str
                     ) -> Optional[CustomerSettings]:
        filter_dict = {CUSTOMER_NAME: customer_name, KEY: key}
        return CustomerSettings.find_one(filter=filter_dict)

    @staticmethod
    def query_by_customer_name(customer_name: str, 
                               key_condition: Optional[Condition] = None,
                               limit: Optional[int] = None,
                               last_evaluated_key: Optional[Union[dict, int]] = None,
                               rate_limit: Optional[float] = None
                               ) -> ResultIterator[CustomerSettings]:
        return CustomerSettings.query(
            hash_key=customer_name,
            range_key_condition=key_condition,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            rate_limit=rate_limit
        )

    @staticmethod
    def delete(setting: CustomerSettings):
        setting.delete()

    @staticmethod
    def save(setting: CustomerSettings):
        setting.save()

    @staticmethod
    def update(setting: CustomerSettings, actions: List[Action]) -> None:
        if actions:
            setting.update(actions=actions)
