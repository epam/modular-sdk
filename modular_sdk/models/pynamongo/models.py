from typing import TYPE_CHECKING, Generator, Iterator, TypeVar

from pymongo import ASCENDING, DESCENDING, DeleteOne, ReplaceOne
from pymongo.collection import ReturnDocument
from pynamodb.models import Model as _Model

from .convertors import (
    AttributesToGetToProjectionConvertor,
    ConditionConverter,
    PynamoDBModelToMongoDictSerializer,
    UpdateExpressionConverter,
)

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.cursor import Cursor
    from pymongo.database import Database
    from pynamodb.expressions.update import Action

_MT = TypeVar('_MT', bound=_Model)


class ResultIterator(Iterator[_MT]):
    __slots__ = '_cursor', '_model', '_serializer', '_skip'

    def __init__(
        self,
        cursor: 'Cursor',
        model: type[_MT],
        serializer: PynamoDBModelToMongoDictSerializer,
        skip: int = 0,
    ):
        self._cursor = cursor
        self._model = model
        self._serializer = serializer
        self._skip = skip

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
        return self._skip

    @property
    def total_count(self) -> int:
        return self._cursor.retrieved


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

    def save(self, put_item: _MT) -> None:
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

    def delete(self, del_item: _MT) -> None:
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


class PynamoDBToPymongoAdapter:
    __slots__ = '_db',
    _ser = PynamoDBModelToMongoDictSerializer()

    def __init__(self, db: 'Database | None' = None):
        self._db = db

    def _get_db(self, model: type[_MT] | _MT) -> 'Database':
        db = getattr(model.Meta, 'mongo_db', None)
        if db is not None:
            return db
        assert self._db is not None, \
            ('Adapter must own an instance of Mongo Database if you want to '
             'use models without their own DB')
        return self._db

    def _get_collection(self, model: type[_MT] | _MT) -> 'Collection':
        db = self._get_db(model)
        return db.get_collection(model.Meta.table_name)

    def get(
        self,
        model: type[_Model],
        hash_key,
        range_key=None,
        attributes_to_get=None,
    ):
        hash_key_name, range_key_name = self._ser.model_keys_names(model)
        hash_key, range_key = self._ser.serialize_keys(
            model, hash_key, range_key
        )
        query = {hash_key_name: hash_key}
        if range_key_name and range_key:
            query[range_key_name] = range_key

        item = self._get_collection(model).find_one(
            query,
            projection=AttributesToGetToProjectionConvertor.convert(
                attributes_to_get
            ),
        )
        if not item:
            raise model.DoesNotExist()
        return self._ser.deserialize(model, item)

    def save(self, instance: _Model):
        collection = self._get_collection(instance)
        # TODO: save by mongo_id if exists
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        collection.replace_one(
            filter=q,
            replacement=self._ser.serialize(instance),
            upsert=True,
        )

    def update(self, instance: _Model, actions: list['Action']):
        _update = {}
        for dct in map(UpdateExpressionConverter.convert, actions):
            for action, query in dct.items():
                _update.setdefault(action, {}).update(query)
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        res = self._get_collection(instance).find_one_and_update(
            filter=q,
            update=_update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            self._ser.deserialize_to(instance, res)

    def delete(self, instance: _Model):
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)

        self._get_collection(instance).delete_one(q)

    def refresh(self, instance: _Model):
        if _id := self._ser.get_mongo_id(instance):
            q = {'_id': _id}
        else:
            q = self._ser.instance_serialized_keys(instance)
        item = self._get_collection(instance).find_one(q)
        if not item:
            raise instance.DoesNotExist()
        self._ser.deserialize_to(instance, item)

    def exists(self, model: type[_Model]) -> bool:
        db = self._get_db(model)
        return model.Meta.table_name in db.list_collection_names()

    def delete_table(self, model: type[_Model]):
        db = self._get_db(model)
        db.drop_collection(model.Meta.table_name)

    def create_table(self, model: type[_Model]):
        """
        Mongo table is created automatically
        """
        # db = self._get_db(model)
        # db.create_collection()

    def count(
        self,
        model: type[_Model],
        hash_key=None,
        range_key_condition=None,
        filter_condition=None,
        index_name=None,
        limit=None,
    ) -> int:
        query = {}

        if hash_key:
            if index_name:
                index = self._ser.model_indexes(model).get(index_name)
                assert index is not None, 'Index must exist'

                h_attr, _ = self._ser.index_keys(index)
                query[h_attr.attr_name] = h_attr.serialize(hash_key)
            else:
                hash_key_name, _ = self._ser.model_keys_names(model)
                hash_key, _ = self._ser.serialize_keys(model, hash_key, None)
                query[hash_key_name] = hash_key

        if range_key_condition is not None:
            query.update(ConditionConverter.convert(range_key_condition))
        if filter_condition is not None:
            query.update(ConditionConverter.convert(filter_condition))
        collection = self._get_collection(model)

        if limit:
            return collection.count_documents(query, limit=limit)
        return collection.count_documents(query)

    def query(
        self,
        model: type[_Model],
        hash_key,
        range_key_condition=None,
        filter_condition=None,
        index_name: str | None = None,
        scan_index_forward=True,
        limit=None,
        last_evaluated_key=None,
        attributes_to_get=None,
        page_size=None,
    ):
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

        q = {hash_key_name: hash_key}
        if range_key_condition is not None:
            q.update(ConditionConverter.convert(range_key_condition))
        if filter_condition is not None:
            q.update(ConditionConverter.convert(filter_condition))
        last_evaluated_key = last_evaluated_key or 0

        cursor = self._get_collection(model).find(
            q,
            projection=AttributesToGetToProjectionConvertor.convert(
                attributes_to_get
            ),
            skip=last_evaluated_key,
            limit=limit or 0,
            batch_size=page_size or 0,
        )
        if range_key_name:
            cursor = cursor.sort(
                range_key_name, ASCENDING if scan_index_forward else DESCENDING
            )
        return ResultIterator(
            cursor=cursor,
            model=model,
            serializer=self._ser,
            skip=last_evaluated_key,
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
        query = {}
        if filter_condition is not None:
            query.update(ConditionConverter.convert(filter_condition))
        last_evaluated_key = last_evaluated_key or 0

        # TODO: scan a specific index
        cursor = self._get_collection(model).find(
            query,
            projection=AttributesToGetToProjectionConvertor.convert(
                attributes_to_get
            ),
            skip=last_evaluated_key,
            limit=limit or 0,
            batch_size=page_size or 0,
        )
        return ResultIterator(
            cursor=cursor,
            model=model,
            serializer=self._ser,
            skip=last_evaluated_key,
        )

    def batch_get(
        self, model: type[_MT], keys: list[tuple], attributes_to_get=None
    ) -> Generator[_MT, None, None]:
        """
        Seems like bulk read is not supported.
        Order not guaranteed
        """
        ors = []

        hash_key_name, range_key_name = self._ser.model_keys_names(model)
        for key in keys:
            hash_key, range_key = self._ser.serialize_keys(model, *key)
            q = {hash_key_name: hash_key}
            if range_key_name and range_key:
                q[range_key_name] = range_key
            ors.append(q)
        cursor = self._get_collection(model).find(
            {'$or': ors},
            projection=AttributesToGetToProjectionConvertor.convert(
                attributes_to_get
            ),
        )
        for item in cursor:
            yield self._ser.deserialize(model, item)

    def batch_write(self, model: type[_MT]) -> BatchWrite:
        return BatchWrite(
            serializer=self._ser, collection=self._get_collection(model)
        )


class Model(_Model):
    pass
