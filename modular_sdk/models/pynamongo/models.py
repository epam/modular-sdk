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

from pynamodb.exceptions import DoesNotExist
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.models import _T, BatchWrite, _KeyType
from pynamodb.models import Model as _Model
from pynamodb.pagination import ResultIterator
from pynamodb.settings import OperationSettings

from modular_sdk.models.pynamongo.adapter import PynamoDBToPymongoAdapter


class Model(_Model):
    mongo_adapter = PynamoDBToPymongoAdapter()

    @classmethod
    def is_mongo_model(cls) -> bool:
        return (
            getattr(cls.Meta, 'mongo_database', None) is not None
            or getattr(cls.Meta, 'mongo_collection', None) is not None
        )

    @classmethod
    def batch_get(
        cls,
        items: Iterable[Union[_KeyType, Iterable[_KeyType]]],
        consistent_read: Optional[bool] = None,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> Iterator[_T]:
        if cls.is_mongo_model():
            return cls.mongo_adapter.batch_get(
                model=cls, items=items, attributes_to_get=attributes_to_get
            )
        return super().batch_get(
            items=items,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
            settings=settings,
        )

    @classmethod
    def batch_write(
        cls,
        auto_commit: bool = True,
        settings: OperationSettings = OperationSettings.default,
    ) -> BatchWrite[_T]:
        if cls.is_mongo_model():
            return cls.mongo_adapter.batch_write(model=cls)
        return super().batch_write(auto_commit=auto_commit, settings=settings)

    def delete(
        self,
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
        *,
        add_version_condition: bool = True,
    ) -> Any:
        if self.is_mongo_model():
            return self.mongo_adapter.delete(instance=self)
        return super().delete(
            condition=condition,
            settings=settings,
            add_version_condition=add_version_condition,
        )

    def update(
        self,
        actions: List[Action],
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
        *,
        add_version_condition: bool = True,
    ) -> Any:
        if self.is_mongo_model():
            return self.mongo_adapter.update(instance=self, actions=actions)
        return super().update(
            actions=actions,
            condition=condition,
            settings=settings,
            add_version_condition=add_version_condition,
        )

    def save(
        self,
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
        *,
        add_version_condition: bool = True,
    ) -> Dict[str, Any]:
        if self.is_mongo_model():
            return self.mongo_adapter.save(instance=self)
        # TODO: correct result
        return super().save(
            condition=condition,
            settings=settings,
            add_version_condition=add_version_condition,
        )

    def refresh(
        self,
        consistent_read: bool = False,
        settings: OperationSettings = OperationSettings.default,
    ) -> None:
        if self.is_mongo_model():
            return self.mongo_adapter.refresh(instance=self)
        return super().refresh(
            consistent_read=consistent_read, settings=settings
        )

    @classmethod
    def get(
        cls,
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Sequence[Text]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> _T:
        if cls.is_mongo_model():
            return cls.mongo_adapter.get(
                model=cls,
                hash_key=hash_key,
                range_key=range_key,
                attributes_to_get=attributes_to_get,
            )
        return super().get(
            hash_key=hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
            settings=settings,
        )

    @classmethod
    def get_nullable(
        cls,
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        consistent_read: bool = False,
        attributes_to_get: Optional[Sequence[Text]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> _T | None:
        try:
            return cls.get(
                hash_key=hash_key,
                range_key=range_key,
                consistent_read=consistent_read,
                attributes_to_get=attributes_to_get,
                settings=settings,
            )
        except DoesNotExist:
            return

    @classmethod
    def count(
        cls,
        hash_key: Optional[_KeyType] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> int:
        if cls.is_mongo_model():
            return cls.mongo_adapter.count(
                model=cls,
                hash_key=hash_key,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                index_name=index_name,
                limit=limit,
            )
        return super().count(
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=index_name,
            limit=limit,
            rate_limit=rate_limit,
            settings=settings,
        )

    @classmethod
    def query(
        cls,
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[Iterable[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> ResultIterator[_T]:
        if cls.is_mongo_model():
            return cls.mongo_adapter.query(
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
        return super().query(
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            rate_limit=rate_limit,
            settings=settings,
        )

    @classmethod
    def scan(
        cls,
        filter_condition: Optional[Condition] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        page_size: Optional[int] = None,
        consistent_read: Optional[bool] = None,
        index_name: Optional[str] = None,
        rate_limit: Optional[float] = None,
        attributes_to_get: Optional[Sequence[str]] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> ResultIterator[_T]:
        if cls.is_mongo_model():
            return cls.mongo_adapter.scan(
                model=cls,
                filter_condition=filter_condition,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                page_size=page_size,
                index_name=index_name,
                attributes_to_get=attributes_to_get,
            )
        return super().scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=index_name,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get,
            settings=settings,
        )

    @classmethod
    def exists(cls) -> bool:
        if cls.is_mongo_model():
            return cls.mongo_adapter.exists(cls)
        return super().exists()

    @classmethod
    def delete_table(cls) -> Any:
        if cls.is_mongo_model():
            return cls.mongo_adapter.delete_table(cls)
        return super().delete_table()

    @classmethod
    def describe_table(cls) -> Any:
        if cls.is_mongo_model():
            raise NotImplementedError('Describe not implemented for mongo')
        return super().describe_table()

    @classmethod
    def create_table(
        cls,
        wait: bool = False,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        billing_mode: Optional[str] = None,
        ignore_update_ttl_errors: bool = False,
    ) -> Any:
        if cls.is_mongo_model():
            return cls.mongo_adapter.create_table(cls)
        return super().create_table(
            wait=wait,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            billing_mode=billing_mode,
            ignore_update_ttl_errors=ignore_update_ttl_errors,
        )
