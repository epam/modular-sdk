import os
import pymongo
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Text,
    Union,
)

from pynamodb.attributes import Attribute, ListAttribute, MapAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.models import _T, BatchWrite, _KeyType
from pynamodb.models import Model as _Model

from modular_sdk.commons.constants import Env
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.pynamongo.adapter import MongoAdapter, ResultIterator

_LOG = get_logger(__name__)


class Model(_Model):
    """
    Base Model class that always uses MongoDB adapter.
    Inherits from PynamoDB Model for structure definition only.
    """
    @classmethod
    def mongo_adapter(cls) -> MongoAdapter:
        if hasattr(cls, '_mongo_adapter'):
            return getattr(cls, '_mongo_adapter')
        setattr(cls, '_mongo_adapter', MongoAdapter())
        return getattr(cls, '_mongo_adapter')

    @classmethod
    def batch_get(
        cls,
        items: Iterable[Union[_KeyType, Iterable[_KeyType]]],
        consistent_read: Optional[bool] = None,  # Ignored for MongoDB
        attributes_to_get: Optional[Sequence[str]] = None,
    ) -> Iterator[_T]:
        return cls.mongo_adapter().batch_get(
            model=cls, items=items, attributes_to_get=attributes_to_get
        )

    @classmethod
    def batch_write(
        cls,
        auto_commit: bool = True,
    ) -> BatchWrite[_T]:
        return cls.mongo_adapter().batch_write(model=cls)

    def delete(
        self,
        condition: Optional[Condition] = None,
        *,
        add_version_condition: bool = True,  # Ignored for MongoDB
    ) -> Any:
        return self.mongo_adapter().delete(instance=self, condition=condition)

    def update(
        self,
        actions: List[Action],
        condition: Optional[Condition] = None,
        *,
        add_version_condition: bool = True,  # Ignored for MongoDB
    ) -> Any:
        return self.mongo_adapter().update(instance=self, actions=actions, condition=condition)

    def save(
        self,
        condition: Optional[Condition] = None,
        *,
        add_version_condition: bool = True,  # Ignored for MongoDB
    ) -> Dict[str, Any]:
        return self.mongo_adapter().save(instance=self)

    def refresh(
        self,
        consistent_read: bool = False,  # Ignored for MongoDB
    ) -> None:
        return self.mongo_adapter().refresh(instance=self)

    @classmethod
    def get(
        cls,
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,  # Ignored for MongoDB
        attributes_to_get: Optional[Sequence[Text]] = None,
    ) -> _T:
        return cls.mongo_adapter().get(
            model=cls,
            hash_key=hash_key,
            range_key=range_key,
            attributes_to_get=attributes_to_get,
        )

    @classmethod
    def get_nullable(
        cls,
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,  # Ignored for MongoDB
        attributes_to_get: Optional[Sequence[Text]] = None,
    ) -> _T | None:
        try:
            return cls.get(
                hash_key=hash_key,
                range_key=range_key,
                consistent_read=consistent_read,
                attributes_to_get=attributes_to_get,
            )
        except DoesNotExist:
            return

    @classmethod
    def count(
        cls,
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,  # Ignored for MongoDB
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,  # Ignored for MongoDB
    ) -> int:
        return cls.mongo_adapter().count(
            model=cls,
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            limit=limit,
        )

    @classmethod
    def query(
        cls,
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,  # Ignored for MongoDB
        index_name: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[Iterable[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,  # Ignored for MongoDB
    ) -> ResultIterator[_T]:
        return cls.mongo_adapter().query(
            model=cls,
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
        )

    @classmethod
    def scan(
        cls,
        filter_condition: Optional[Condition] = None,
        segment: Optional[int] = None,  # Ignored for MongoDB
        total_segments: Optional[int] = None,  # Ignored for MongoDB
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        page_size: Optional[int] = None,
        consistent_read: Optional[bool] = None,  # Ignored for MongoDB
        index_name: Optional[str] = None,
        rate_limit: Optional[float] = None,  # Ignored for MongoDB
        attributes_to_get: Optional[Sequence[str]] = None,
    ) -> ResultIterator[_T]:
        return cls.mongo_adapter().scan(
            model=cls,
            filter_condition=filter_condition,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            index_name=index_name,
            attributes_to_get=attributes_to_get,
        )

    @classmethod
    def exists(cls) -> bool:
        return cls.mongo_adapter().exists(cls)

    @classmethod
    def delete_table(cls) -> Any:
        return cls.mongo_adapter().delete_table(cls)

    @classmethod
    def describe_table(cls) -> Any:
        raise NotImplementedError('Describe not implemented for mongo')

    @classmethod
    def create_table(
        cls,
        wait: bool = False,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        billing_mode: Optional[str] = None,
        ignore_update_ttl_errors: bool = False,
    ) -> Any:
        return cls.mongo_adapter().create_table(cls)


class SafeUpdateModel(Model):
    """
    Allows not to override existing attributes that are not specified
    in Models on item update.
    """

    _original_data_attr_name = '_original_data'

    @classmethod
    def _update_with_not_defined_attributes(
        cls,
        serialized: dict[str, dict[str, Any]],
        original: dict[str, dict[str, Any]],
        attributes: dict[str, Attribute],
    ) -> None:
        """
        Changes serialized dict in place
        """
        name_to_instance = {
            attr.attr_name: attr for attr in attributes.values()
        }
        for name, value in original.items():
            if name not in name_to_instance:  # not defined in model
                serialized[name] = value
                continue
            # attribute is defined, but maybe it's a nested mapping and
            # some nested attributes are not defined
            attr = name_to_instance[name]
            if isinstance(attr, MapAttribute) and not attr.is_raw():
                cls._update_with_not_defined_attributes(
                    serialized=serialized[name].setdefault(attr.attr_type, {}),
                    original=value.get(attr.attr_type, {}),
                    attributes=attr.get_attributes(),
                )
            elif (
                isinstance(attr, ListAttribute)
                and attr.element_type
                and issubclass(attr.element_type, MapAttribute)
                and not attr.element_type.is_raw()
            ):
                # here we definitely have list of mappings
                inner_attributes = attr.element_type.get_attributes()
                orig_items = value.get(attr.attr_type, ())
                for i, item in enumerate(
                    serialized[name].setdefault(attr.attr_type, [])
                ):
                    if i >= len(orig_items):
                        break
                    cls._update_with_not_defined_attributes(
                        serialized=item.setdefault(
                            attr.element_type.attr_type, {}
                        ),
                        original=orig_items[i].get(
                            attr.element_type.attr_type, {}
                        ),
                        attributes=inner_attributes,
                    )

    def serialize(self, null_check: bool = True) -> dict[str, dict[str, Any]]:
        """
        Used both by Mongo and PynamoDB serializers
        """
        serialized = super().serialize(null_check=null_check)
        self._update_with_not_defined_attributes(
            serialized=serialized,
            original=getattr(self, self._original_data_attr_name, {}),
            attributes=self.get_attributes(),
        )
        return serialized

    def deserialize(self, attribute_values: dict[str, dict[str, Any]]) -> None:
        """
        Used by Mongo deserializer
        """
        setattr(self, self._original_data_attr_name, attribute_values)
        return super().deserialize(attribute_values=attribute_values)

    @classmethod
    def _instantiate(cls, attribute_values: dict[str, dict[str, Any]]):
        """
        Used by PynamoDB deserializer
        """
        instance = super()._instantiate(attribute_values)
        setattr(instance, cls._original_data_attr_name, attribute_values)
        return instance


class MongoClientSingleton:
    _instance = None

    @staticmethod
    def _build_mongo_uri() -> str:
        if uri := Env.MONGO_URI.get():
            return uri
        user = Env.MONGO_USER.get()
        password = Env.MONGO_PASSWORD.get()
        url = Env.MONGO_URL.get()
        srv = bool(Env.MONGO_SRV.get())
        return f'mongodb{"+srv" if srv else ""}://{user}:{password}@{url}/'

    @classmethod
    def get_instance(cls) -> pymongo.MongoClient:
        if cls._instance is None:
            _LOG.debug(f'Creating MongoClient in {os.getpid()}')
            cls._instance = pymongo.MongoClient(cls._build_mongo_uri())
        return cls._instance


class BaseModel(SafeUpdateModel):
    """
    Base model for Modular SDK that always uses MongoDB.
    Provides mongo adapter configured from environment variables.
    """
    @classmethod
    def mongo_adapter(cls) -> MongoAdapter:
        if hasattr(cls, '_mongo_adapter'):
            return getattr(cls, '_mongo_adapter')
        client = MongoClientSingleton.get_instance()
        db = Env.MONGO_DB_NAME.get()
        setattr(
            cls,
            '_mongo_adapter',
            MongoAdapter(db=client.get_database(db)),
        )
        return getattr(cls, '_mongo_adapter')
