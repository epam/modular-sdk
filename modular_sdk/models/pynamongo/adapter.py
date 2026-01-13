import base64
import binascii
import json
import math
from typing import TYPE_CHECKING, Any, Generator, Iterator, TypeVar, cast

from pymongo import ASCENDING, DESCENDING, DeleteOne, ReplaceOne
from pymongo.collection import ReturnDocument
from pynamodb.expressions.condition import Condition
from pynamodb.models import Model

from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.pynamongo.convertors import (
    PynamoDBModelToMongoDictSerializer,
    convert_attributes_to_get,
    convert_condition_expression,
    convert_update_expression,
    merge_update_expressions,
)

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.cursor import Cursor
    from pymongo.database import Database
    from pynamodb.expressions.update import Action

_MT = TypeVar('_MT', bound=Model)
_LOG = get_logger(__name__)


class ResultIterator(Iterator[_MT]):
    """
    Mocks ResultIterator from PynamoDB
    """

    __slots__ = '_cursor', '_model', '_serializer', '_skip', '_total'

    def __init__(
        self,
        cursor: 'Cursor',
        model: type[_MT],
        serializer: PynamoDBModelToMongoDictSerializer,
        skip: int = 0,
        total: int | float = math.inf,  # pyright: ignore
    ):
        self._cursor = cursor
        self._model = model
        self._serializer = serializer
        self._skip = skip
        self._total = total

    def __iter__(self) -> 'ResultIterator':
        return self

    def __next__(self) -> _MT:
        item = self._cursor.__next__()
        self._skip += 1
        return self._serializer.deserialize(self._model, item)

    def next(self) -> _MT:
        return self.__next__()

    @property
    def last_evaluated_key(self):
        if self._skip < self._total:
            return self._skip

    @property
    def total_count(self) -> int:
        return self._cursor.retrieved


class EmptyResultIterator(ResultIterator):
    __slots__ = '_lek'

    def __init__(self, last_evaluated_key=None):
        self._lek = last_evaluated_key

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration('Cannot iterate over empty result iterator')

    @property
    def last_evaluated_key(self):
        return self._lek

    @property
    def total_count(self):
        return 0


class BatchWrite:
    __slots__ = '_ser', '_collection', '_req'

    def __init__(
        self,
        serializer: PynamoDBModelToMongoDictSerializer,
        collection: 'Collection',
    ):
        self._ser = serializer
        self._collection = collection
        self._req = []

    def save(self, put_item: Model) -> None:
        if _id := self._ser.get_mongo_id(put_item):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(put_item)
        self._req.append(
            ReplaceOne(
                filter=q,
                replacement=self._ser.serialize(put_item),
                upsert=True,
            )
        )

    def delete(self, del_item: Model) -> None:
        if _id := self._ser.get_mongo_id(del_item):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(del_item)
        self._req.append(DeleteOne(q))

    def __enter__(self):
        self._req.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return self.commit()

    def commit(self) -> None:
        if not self._req:
            return
        self._collection.bulk_write(self._req)


class MongoAdapter:
    __slots__ = ('_db',)
    _ser = PynamoDBModelToMongoDictSerializer()

    def __init__(self, db: 'Database | None' = None):
        self._db = db

    @property
    def mongo_database(self) -> 'Database | None':
        return self._db

    def get_database(self, model: type[Model] | Model) -> 'Database':
        db = getattr(model.Meta, 'mongo_database', self._db)
        assert db is not None, (
            'Mongo database must be set either to MongoAdapter'
        )
        return db

    def get_collection(self, model: type[Model] | Model) -> 'Collection':
        collection = getattr(model.Meta, 'mongo_collection', None)
        if collection is not None:
            return collection

        db = self.get_database(model)
        col = db.get_collection(model.Meta.table_name)
        setattr(model.Meta, 'mongo_collection', col)
        return col

    @staticmethod
    def _normalize_projection(
        projection: dict | list[str] | tuple[str, ...] | None
    ) -> dict | None:
        """
        Normalize projection to MongoDB dict format.
        
        Args:
            projection: Projection as dict, list/tuple of field names, or None
            
        Returns:
            Normalized projection dict or None
        """
        if projection is None:
            return None
        
        if isinstance(projection, dict):
            return projection
        
        if isinstance(projection, (list, tuple)):
            # Convert list/tuple to dict format
            projection_dict = {field: 1 for field in projection}
            # Exclude _id if not explicitly included
            if '_id' not in projection:
                projection_dict['_id'] = 0
            return projection_dict
        
        return None

    def find_one(
        self,
        model: type[_MT],
        filter: dict,
        projection: dict | list[str] | tuple[str, ...] | None = None,
        raise_if_not_found: bool = False,
    ) -> _MT | None:
        """
        Native MongoDB find_one method.
        
        Args:
            model: Model class
            filter: MongoDB query filter dict
            projection: Optional projection dict, list, or tuple of field names
            raise_if_not_found: If True, raise model.DoesNotExist() when no document found
            
        Returns:
            Model instance or None if not found (unless raise_if_not_found=True)
        """
        collection = self.get_collection(model)
        normalized_projection = self._normalize_projection(projection)
        item = collection.find_one(filter, projection=normalized_projection)
        if not item:
            if raise_if_not_found:
                raise model.DoesNotExist()
            return None
        return self._ser.deserialize(model, item)

    def get(
        self,
        model: type[_MT],
        hash_key,
        range_key=None,
        attributes_to_get=None,
    ) -> _MT:
        """
        Deprecated: use find_one() instead
        """
        hash_key_name, range_key_name = self._ser.model_keys_names(model)
        hash_key, range_key = self._ser.serialize_keys(
            model, hash_key, range_key
        )
        query = {hash_key_name: hash_key}
        if range_key_name and range_key:
            query[range_key_name] = range_key

        projection = convert_attributes_to_get(attributes_to_get) if attributes_to_get else None
        # convert_attributes_to_get returns tuple[str, ...], which will be normalized by find_one
        return self.find_one(model, filter=query, projection=projection, raise_if_not_found=True)

    def save(self, instance: Model) -> dict:
        """
        Save model instance to MongoDB collection.
        
        Returns:
            dict with MongoDB operation result:
            - matched_count: Number of documents matched
            - modified_count: Number of documents modified
            - upserted_id: ID of upserted document (if any)
        """
        collection = self.get_collection(instance)
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        res = collection.replace_one(
            filter=q, replacement=self._ser.serialize(instance), upsert=True
        )
        if _id := res.upserted_id:
            self._ser.set_mongo_id(instance, _id)
        return {
            'matched_count': res.matched_count,
            'modified_count': res.modified_count,
            'upserted_id': res.upserted_id,
        }

    def update(
        self,
        instance: Model,
        actions: list['Action'],
        condition: Condition | None = None,
    ) -> dict:
        """
        Update model instance in MongoDB collection.
        
        Returns:
            dict with MongoDB operation result:
            - matched_count: Number of documents matched
            - modified_count: Number of documents modified
            - upserted_id: ID of upserted document (if any)
        """
        _update = merge_update_expressions(
            map(convert_update_expression, actions)
        )
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        if condition is not None:
            q = {'$and': [q, convert_condition_expression(condition)]}
        res = self.get_collection(instance).find_one_and_update(
            filter=q,
            update=_update,
            upsert=True if condition is None else False,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            self._ser.deserialize_to(instance, res)
        return {
            'matched_count': 1 if res else 0,
            'modified_count': 1 if res else 0,
            'upserted_id': res._id if res else None
        }

    def delete(
        self, instance: Model, condition: Condition | None = None
    ) -> dict:
        """
        Delete model instance from MongoDB collection.
        
        Returns:
            dict with MongoDB operation result:
            - deleted_count: Number of documents deleted
        """
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        if condition is not None:
            q = {'$and': [q, convert_condition_expression(condition)]}

        res = self.get_collection(instance).delete_one(q)
        return {
            'deleted_count': res.deleted_count,
        }

    def refresh(self, instance: Model) -> None:
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        item = self.get_collection(instance).find_one(q)
        if not item:
            raise instance.DoesNotExist()
        self._ser.deserialize_to(instance, item)

    def exists(self, model: type[Model]) -> bool:
        db = self.get_collection(model).database
        return model.Meta.table_name in db.list_collection_names()

    def delete_table(self, model: type[Model]) -> None:
        self.get_collection(model).drop()

    def create_table(self, model: type[Model]) -> None:
        """
        Mongo table is created automatically
        """
        # db = self._get_db(model)
        # db.create_collection()

    @staticmethod
    def _build_query(
        hash_key_name: str | None,
        hash_key: Any | None,
        range_key_name: str | None,
        range_key_condition: Condition | None,
        filter_condition: Condition | None,
    ) -> dict:
        """
        We would want to merge them but that seems difficult to get right,
        so we just make use of constraints imposed by DynamoDB.
        """
        if hash_key_name is None and hash_key is not None:
            raise ValueError('Hash key name must be provided with hash key')
        if range_key_name is None and range_key_condition is not None:
            raise ValueError(
                'Cannot use range key condition is there is no range key'
            )
        query = {}
        if hash_key is not None:
            query[cast(str, hash_key_name)] = {'$eq': hash_key}

        if range_key_condition is not None:
            range_key_name = cast(str, range_key_name)
            c = convert_condition_expression(range_key_condition)
            if range_key_name not in c:
                raise ValueError(
                    'Range key condition should be a simple '
                    'condition utilizing range key'
                )
            query.update(c)

        if filter_condition is not None:
            # should not use hash key and range key. By here filter query
            # contains only one or two simple conditions. Whatever this thing
            # returns it can be just expanded into query
            c = convert_condition_expression(filter_condition)
            if (hash_key_name and hash_key_name in c) or (
                range_key_name and range_key_name in c
            ):
                # can be checked inside as well but seems like it's enough
                raise ValueError(
                    'Filter condition should not use hash key or range key'
                )
            query.update(c)
        return query

    def count(
        self,
        model: type[Model],
        hash_key=None,
        range_key_condition=None,
        filter_condition=None,
        index_name=None,
        limit=None,
    ) -> int:
        if hash_key:
            if index_name:
                index = self._ser.model_indexes(model).get(index_name)
                assert index is not None, 'Index must exist'

                h_attr, r_attr = self._ser.index_keys(index)
                hash_key_name = h_attr.attr_name
                range_key_name = (
                    r_attr.attr_name if r_attr is not None else None
                )
                hash_key = h_attr.serialize(hash_key)
            else:
                hash_key_name, range_key_name = self._ser.model_keys_names(
                    model
                )
                hash_key, _ = self._ser.serialize_keys(model, hash_key, None)
        else:
            hash_key_name, range_key_name = None, None
        query = self._build_query(
            hash_key_name=hash_key_name,
            hash_key=hash_key,
            range_key_name=range_key_name,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
        )

        collection = self.get_collection(model)

        if limit:
            return collection.count_documents(query, limit=limit)
        return collection.count_documents(query)

    def find(
        self,
        model: type[_MT],
        filter: dict = None,
        projection: dict | list[str] | tuple[str, ...] | None = None,
        sort: list[tuple[str, int]] | None = None,
        skip: int = 0,
        limit: int | None = None,
        batch_size: int | None = None,
        count_total: bool = False
    ) -> ResultIterator[_MT]:
        """
        Native MongoDB find method.
        
        Args:
            model: Model class
            filter: MongoDB query filter dict
            projection: Optional projection dict, list, or tuple of field names
            sort: Optional list of (field, direction) tuples where direction is 1 (ASC) or -1 (DESC)
            skip: Number of documents to skip
            limit: Maximum number of documents to return
            batch_size: Number of documents to return per batch
            count_total: If True, count total documents matching filter (expensive operation)
            
        Returns:
            ResultIterator over model instances
        """
        if not filter:
            filter = {}

        collection = self.get_collection(model)
        normalized_projection = self._normalize_projection(projection)
        find_kwargs = {
            'filter': filter,
            'skip': skip,
        }
        if normalized_projection is not None:
            find_kwargs['projection'] = normalized_projection
        if limit is not None:
            find_kwargs['limit'] = limit
        if batch_size is not None:
            find_kwargs['batch_size'] = batch_size
        
        cursor = collection.find(**find_kwargs)
        if sort:
            cursor = cursor.sort(sort)
        
        # Only count documents if explicitly requested (expensive operation)
        total = collection.count_documents(filter) if count_total else math.inf
        return ResultIterator(
            cursor=cursor,
            model=model,
            serializer=self._ser,
            skip=skip,
            total=total,
        )

    def query(
        self,
        model: type[_MT],
        hash_key,
        range_key_condition=None,
        filter_condition=None,
        index_name: str | None = None,
        scan_index_forward=True,
        limit=None,
        last_evaluated_key=None,
        attributes_to_get=None,
        page_size=None,
    ) -> ResultIterator[_MT]:
        """
        Deprecated: use find() instead
        """
        if index_name:
            index = self._ser.model_indexes(model).get(index_name)
            assert index is not None, 'Index must exist'
            h_attr, r_attr = self._ser.index_keys(index)
            hash_key_name = h_attr.attr_name
            range_key_name = r_attr.attr_name if r_attr is not None else None
            hash_key = h_attr.serialize(hash_key)
        else:
            hash_key_name, range_key_name = self._ser.model_keys_names(model)
            hash_key, _ = self._ser.serialize_keys(model, hash_key, None)

        query = self._build_query(
            hash_key_name=hash_key_name,
            hash_key=hash_key,
            range_key_name=range_key_name,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
        )

        last_evaluated_key = last_evaluated_key or 0
        projection = convert_attributes_to_get(attributes_to_get) if attributes_to_get else None
        
        sort = None
        if range_key_name:
            sort = [(range_key_name, ASCENDING if scan_index_forward else DESCENDING)]
        
        return self.find(
            model=model,
            filter=query,
            projection=projection,
            sort=sort,
            skip=last_evaluated_key,
            limit=limit,
            batch_size=page_size,
        )

    def scan(
        self,
        model: type[_MT],
        filter_condition=None,
        limit=None,
        last_evaluated_key=None,
        page_size=None,
        index_name: str | None = None,
        attributes_to_get=None,
    ) -> ResultIterator[_MT]:
        """
        Deprecated: use find() instead
        """
        if filter_condition is not None:
            query = convert_condition_expression(filter_condition)
        else:
            query = {}
        last_evaluated_key = last_evaluated_key or 0
        projection = convert_attributes_to_get(attributes_to_get) if attributes_to_get else None

        # TODO: scan a specific index using mongo hint
        return self.find(
            model=model,
            filter=query,
            projection=projection,
            skip=last_evaluated_key,
            limit=limit,
            batch_size=page_size,
        )

    def batch_get(
        self,
        model: type[_MT],
        items: list[tuple[Any, Any]] | list[Any],
        attributes_to_get=None,
    ) -> Generator[_MT, None, None]:
        """
        Deprecated: use find() with `$in` operator instead
        """
        ors = []

        hash_key_name, range_key_name = self._ser.model_keys_names(model)

        for key in items:
            if range_key_name:  # Model has range key - enforce tuple
                if not isinstance(key, tuple) or len(key) != 2:
                    raise ValueError(
                        f'Item {key} must be a (hash, range) tuple'
                    )
                hash_key_val, range_key_val = key
            else:  # Model has no range key - single value (any type)
                hash_key_val = key
                range_key_val = None

            hash_key_ser, range_key_ser = self._ser.serialize_keys(
                model=model, hash_key=hash_key_val, range_key=range_key_val
            )

            query = {hash_key_name: hash_key_ser}
            if range_key_name and range_key_ser is not None:
                query[range_key_name] = range_key_ser
            ors.append(query)

        projection = convert_attributes_to_get(attributes_to_get) if attributes_to_get else None
        normalized_projection = self._normalize_projection(projection)
        cursor = self.get_collection(model).find(
            {'$or': ors},
            projection=normalized_projection,
        )
        for item in cursor:
            yield self._ser.deserialize(model, item)

    def batch_write(self, model: type[Model]) -> BatchWrite:
        return BatchWrite(
            serializer=self._ser, collection=self.get_collection(model)
        )


class LastEvaluatedKey:
    """
    Pagination token for MongoDB queries.
    Encapsulates offset/skip position for cursor-based pagination.
    """

    payload_key_name = 'key'

    def __init__(self, lek: dict | int | None = None):
        self._lek = lek

    def serialize(self) -> str:
        payload = {self.payload_key_name: self._lek}
        return base64.urlsafe_b64encode(
            json.dumps(payload, separators=(',', ':'), sort_keys=True).encode()
        ).decode()

    @classmethod
    def deserialize(cls, s: str | None = None) -> 'LastEvaluatedKey':
        if not s or not isinstance(s, str):
            return cls()
        _payload = {}
        try:
            decoded = base64.urlsafe_b64decode(s.encode()).decode()
            _payload = json.loads(decoded)
        except binascii.Error:
            _LOG.warning('Invalid base64 encoding in pagination token')
        except json.JSONDecodeError:
            _LOG.warning('Invalid json string within pagination token')
        except Exception as e:  # you never know :)
            _LOG.warning(
                'Some unexpected exception occurred while '
                f"deserializing pagination token : '{e}'"
            )
        return cls(_payload.get(cls.payload_key_name))

    @property
    def value(self) -> dict | int | None:
        return self._lek

    @value.setter
    def value(self, v: dict | int | None):
        self._lek = v

    def __bool__(self) -> bool:
        return bool(self._lek)
