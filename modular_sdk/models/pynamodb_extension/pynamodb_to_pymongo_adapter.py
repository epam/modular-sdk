import json
import json
import re
import decimal
from itertools import islice
from typing import Optional, Dict, List, Union, TypeVar, Iterator

from pymongo import DeleteOne, ReplaceOne, DESCENDING, ASCENDING
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import BulkWriteError
from pynamodb import indexes
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Value, Path, _ListAppend
from pynamodb.expressions.update import SetAction, RemoveAction, Action
from pynamodb.models import Model
from pynamodb.settings import OperationSettings

from modular_sdk.commons import DynamoDBJsonSerializer
from modular_sdk.connections.mongodb_connection import MongoDBConnection

T = TypeVar('T')


class Result(Iterator[T]):
    def __init__(self, result: Iterator[T],
                 _evaluated_key: Optional[int] = None,
                 page_size: Optional[int] = None):
        self._result_it = result
        self._evaluated_key = _evaluated_key
        self._page_size = page_size

    @property
    def last_evaluated_key(self):
        _key = self._evaluated_key
        if _key is not None and _key < self._page_size:
            return _key

    def __iter__(self):
        return self

    def __next__(self) -> T:
        item = self._result_it.__next__()

        if self._evaluated_key is not None:
            self._evaluated_key += 1
        return item


class BatchWrite:
    def __init__(self, model, mongo_connection):
        self.collection_name = model.Meta.table_name
        self.mongo_connection = mongo_connection
        self.request = []

    def save(self, put_item):
        json_to_save = put_item.dynamodb_model()
        json_to_save.pop('mongo_id', None)

        encoded_document = self.mongo_connection.encode_keys(
            {
                key: value for key, value in json_to_save.items()
                if value is not None
            }
        )
        self.request.append(ReplaceOne(put_item.get_keys(),
                                       encoded_document, upsert=True))

    def delete(self, del_item):
        self.request.append(DeleteOne(del_item._get_keys()))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.commit()

    def commit(self):
        collection = self.mongo_connection.collection(
            collection_name=self.collection_name)

        if not self.request:
            return
        try:
            collection.bulk_write(self.request)
        except BulkWriteError:
            pass


class _PynamoDBExpressionsConverter:
    # Looks for [1], [2], [12], etc in a string
    index_regex: re.Pattern = re.compile('\[\d+\]')

    @staticmethod
    def _preprocess(val: T) -> T:
        """
        Convert some values that are not accepted by mongodb:
        - decimal.Decimal
        Changes the given collection in place but also returns it
        """
        if isinstance(val, dict):
            for k, v in val.items():
                val[k] = _PynamoDBExpressionsConverter._preprocess(v)
            return val
        if isinstance(val, list):
            for i, v in enumerate(val):
                val[i] = _PynamoDBExpressionsConverter._preprocess(v)
            return val
        if isinstance(val, decimal.Decimal):
            return float(val)
        return val

    @staticmethod
    def value_to_raw(value: Value) -> Union[str, dict, list, int, float]:
        """
        PynamoDB operand Value contains only one element in a list. This
        element is a dict: {'pynamo type': 'value'}
        :param value:
        :return:
        """
        val = DynamoDBJsonSerializer.deserializer.deserialize(value.value)
        # now we can return the val, BUT some its values (top-level and nested)
        # can contain decimal.Decimal which is not acceptable by MongoDB.
        # we should convert them to simple floats.
        return _PynamoDBExpressionsConverter._preprocess(val)

    @classmethod
    def path_to_raw(cls, path: Path) -> str:
        """
        You can query MongoDB by nested attributes (one.two.three) and by
        first nested lists (one.two.3). But not deeper, i.e 'one.two.3.four'
        won't work.
        PynamoDB Path is converted a bit differently: one.two[3] . We
        just need to change it to one.two.3
        :param path:
        :return:
        """
        raw = str(path)
        for index in re.findall(cls.index_regex, raw):
            n = index.strip('[]')
            raw = raw.replace(index, f'.{n}')
        return raw


class ConditionConverter(_PynamoDBExpressionsConverter):
    """
    Converts PynamoDB conditions to MongoDB query map. Supported classes from
    `pynamodb.expressions.condition`: Comparison, Between, In, Exists,
    NotExists, BeginsWith, Contains, And, Or, Not.

    IsType and size are not supported. Add support if you want
    """
    comparison_map: Dict[str, str] = {
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
        '<>': '$ne'
    }

    @classmethod
    def convert(cls, condition: Condition) -> dict:
        op = condition.operator
        if op == 'OR':
            return {
                '$or': [cls.convert(cond) for cond in condition.values]
            }
        if op == 'AND':
            return {
                '$and': [cls.convert(cond) for cond in condition.values]
            }
        if op == 'NOT':
            return {
                '$nor': [cls.convert(condition.values[0])]
            }
        if op == 'attribute_exists':
            return {
                cls.path_to_raw(condition.values[0]): {'$exists': True}
            }
        if op == 'attribute_not_exists':
            return {
                cls.path_to_raw(condition.values[0]): {'$exists': False}
            }
        if op == 'contains':
            return {
                cls.path_to_raw(condition.values[0]): {
                    '$regex': cls.value_to_raw(condition.values[1])
                }
            }
        if op == 'IN':
            return {
                cls.path_to_raw(condition.values[0]): {
                    '$in': list(
                        cls.value_to_raw(v) for v in
                        islice(condition.values, 1, None)
                    )
                }
            }
        if op == '=':
            return {
                cls.path_to_raw(condition.values[0]): cls.value_to_raw(
                    condition.values[1])
            }
        if op in cls.comparison_map:
            _mongo_op = cls.comparison_map[op]
            return {
                cls.path_to_raw(condition.values[0]): {
                    _mongo_op: cls.value_to_raw(condition.values[1])
                }
            }
        if op == 'BETWEEN':
            return {
                cls.path_to_raw(condition.values[0]): {
                    '$gte': cls.value_to_raw(condition.values[1]),
                    '$lte': cls.value_to_raw(condition.values[2])
                }
            }
        if op == 'begins_with':
            return {
                cls.path_to_raw(condition.values[0]): {
                    '$regex': f'^{cls.value_to_raw(condition.values[1])}'
                }
            }
        raise NotImplementedError(f'Operator: {op} is not supported')


class UpdateExpressionConverter(_PynamoDBExpressionsConverter):
    """
    Currently just SetAction and RemoveAction, ListAppend, ListPrepend
    are supported, you can implement increment and decrement
    """

    @classmethod
    def convert(cls, action: Action):
        if isinstance(action, SetAction):
            path, value = action.values
            if isinstance(value, Value):
                return {
                    '$set': {cls.path_to_raw(path): cls.value_to_raw(value)}
                }
            if isinstance(value,
                          _ListAppend):  # appending from one list to another is not supported. However, Dynamo seems to support it
                if isinstance(value.values[0], Path):  # append
                    return {
                        '$push': {cls.path_to_raw(path): {
                            '$each': cls.value_to_raw(value.values[1])}
                        }
                    }
                else:  # prepend
                    return {
                        '$push': {
                            cls.path_to_raw(path): {
                                '$each': cls.value_to_raw(value.values[0]),
                                '$position': 0
                            },
                        }
                    }
            # does not work, but the idea is right.
            # Only need to make right mongo query
            # if isinstance(value, _Increment):
            #     return {
            #         '$set': {cls.path_to_raw(path): {
            #             '$add': [f'${cls.path_to_raw(value.values[0])}', int(cls.value_to_raw(value.values[1]))]  # make sure it's int, it is your responsibility
            #         }}
            #     }
            # if isinstance(value, _Decrement):
            #     return {
            #         '$set': {cls.path_to_raw(path): {
            #             '$add': [f'${cls.path_to_raw(value.values[0])}', -int(cls.value_to_raw(value.values[1]))]  # make sure it's int, it is your responsibility
            #         }}
            #     }
            raise NotImplementedError(
                f'Operand of type: {value.__class__.__name__} not supported'
            )
        if isinstance(action, RemoveAction):
            path, = action.values
            return {
                '$unset': {cls.path_to_raw(path): ""}
                # empty string does not matter https://www.mongodb.com/docs/manual/reference/operator/update/unset/#mongodb-update-up.-unset
            }
        raise NotImplementedError(
            f'Action {action.__class__.__name__} is not implemented'
        )


class PynamoDBToPyMongoAdapter:

    def __init__(self, mongodb_connection: MongoDBConnection):
        self.mongodb = mongodb_connection

    def batch_get(self, model_class, items, attributes_to_get=None):
        collection = self._collection_from_model(model_class)
        query_params = []
        hash_key_name, range_key_name = self.__get_table_keys(model_class)
        if isinstance(items[0], tuple):
            query_params.append({'$or': [{hash_key_name: item[0],
                                          range_key_name: item[1]}
                                         for item in items]})
        else:
            query_params.append(
                {'$or': [{hash_key_name: item} for item in items]})
        raw_items = collection.find(*query_params)
        return [model_class.from_json(
            model_json=self.mongodb.decode_keys(item),
            attributes_to_get=attributes_to_get)
            for item in raw_items]

    def delete(self, model_instance):
        collection = self._collection_from_model(model_instance)
        query = {}
        try:
            query = model_instance.get_keys()
        except AttributeError:
            if isinstance(model_instance.attribute_values, dict):
                query = model_instance.attribute_values
        collection.delete_one(query)

    def save(self, model_instance):
        json_to_save = model_instance.dynamodb_model()
        collection = self._collection_from_model(model_instance)

        json_to_save.pop('mongo_id', None)
        encoded_document = self.mongodb.encode_keys(
            {
                key: value for key, value in json_to_save.items()
                if value is not None
            }
        )
        collection.replace_one(model_instance.get_keys(),
                               encoded_document, upsert=True)

    def update(self, model_instance, actions: List[Action],
               condition: Optional[Condition] = None,
               settings: OperationSettings = OperationSettings.default):
        collection = self._collection_from_model(model_instance)
        _update = {}
        for dct in [UpdateExpressionConverter.convert(a) for a in actions]:
            for action, query in dct.items():
                _update.setdefault(action, {}).update(query)
        res = collection.find_one_and_update(
            filter=model_instance.get_keys(),
            update=_update,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        if res:
            type(model_instance).from_json(res, instance=model_instance)

    def get(self, model_class, hash_key, range_key=None) -> Model:
        result = self.get_nullable(model_class=model_class,
                                   hash_key=hash_key,
                                   sort_key=range_key)
        if not result:
            raise model_class.DoesNotExist()
        return result

    def get_nullable(self, model_class, hash_key, sort_key=None
                     ) -> Optional[Model]:
        hash_key_name, range_key_name = self.__get_table_keys(model_class)

        if not hash_key_name:
            raise AssertionError('Can not identify the hash key name of '
                                 f'model: \'{type(model_class).__name__}\'')
        if sort_key and not range_key_name:
            raise AssertionError(
                f'The range key value is specified for '
                f'model \'{type(model_class).__name__}\' but there is no '
                f'attribute in the model marked as range_key')

        collection = self._collection_from_model(model_class)
        params = {hash_key_name: hash_key}
        if range_key_name and sort_key:
            params[range_key_name] = sort_key
        raw_item = collection.find_one(params)
        if raw_item:
            raw_item = self.mongodb.decode_keys(raw_item)
            return model_class.from_json(raw_item)

    def query(self, model_class, hash_key, range_key_condition=None,
              filter_condition=None, limit=None, last_evaluated_key=None,
              attributes_to_get=None, scan_index_forward=True):
        # works both for Model and Index
        hash_key_name = getattr(model_class._hash_key_attribute(),
                                'attr_name', None)
        range_key_name = getattr(model_class._range_key_attribute(),
                                 'attr_name', None)
        if issubclass(model_class, indexes.Index):
            model_class = model_class.Meta.model

        collection = self._collection_from_model(model_class)
        _query = {hash_key_name: hash_key}
        if range_key_condition is not None:
            _query.update(ConditionConverter.convert(range_key_condition))

        if filter_condition is not None:
            _query.update(ConditionConverter.convert(filter_condition))

        limit = limit or 0  # ZERO means no limit
        last_evaluated_key = last_evaluated_key or 0

        cursor = collection.find(_query).limit(limit).skip(last_evaluated_key)
        if range_key_name:
            cursor = cursor.sort(
                range_key_name, ASCENDING if scan_index_forward else
                DESCENDING
            )
        return Result(
            result=(model_class.from_json(self.mongodb.decode_keys(i),
                                          attributes_to_get) for i in cursor),
            _evaluated_key=last_evaluated_key,
            page_size=collection.count_documents(_query)
        )

    def scan(self, model_class, filter_condition=None, limit=None,
             last_evaluated_key=None, attributes_to_get=None):
        collection = self._collection_from_model(model_class)
        _query = {}

        if filter_condition is not None:
            _query.update(ConditionConverter.convert(filter_condition))

        limit = limit or 0  # ZERO means no limit
        last_evaluated_key = last_evaluated_key or 0

        cursor = collection.find(_query).limit(limit).skip(last_evaluated_key)
        return Result(
            result=(model_class.from_json(self.mongodb.decode_keys(i),
                                          attributes_to_get) for i in cursor),
            _evaluated_key=last_evaluated_key,
            page_size=collection.count_documents(_query)
        )

    def refresh(self, consistent_read):
        raise NotImplementedError

    def _collection_from_model(self, model: Model) -> Collection:
        name = model.Meta.table_name
        return self.mongodb.collection(collection_name=name)

    def count(self, model_class, hash_key=None,
              range_key_condition=None,
              filter_condition=None,
              index_name=None,
              limit=None) -> int:
        collection = self._collection_from_model(model_class)

        hash_key_name = getattr(model_class._hash_key_attribute(),
                                'attr_name', None)
        if index_name:
            hash_key_name = getattr(
                model_class._indexes[index_name]._hash_key_attribute(),
                'attr_name', None
            )

        _query = {hash_key_name: hash_key}
        if range_key_condition is not None:
            _query.update(ConditionConverter.convert(range_key_condition))

        if filter_condition is not None:
            _query.update(ConditionConverter.convert(filter_condition))

        if limit:
            return collection.count_documents(_query, limit=limit)

        return collection.count_documents(_query)

    def batch_write(self, model_class) -> BatchWrite:
        return BatchWrite(model=model_class, mongo_connection=self.mongodb)

    @staticmethod
    def __get_table_keys(model_class) -> tuple:
        short_to_body_mapping = {attr_body.attr_name: attr_body
                                 for attr_name, attr_body in
                                 model_class._attributes.items()}
        hash_key_name = None
        range_key_name = None
        for short_name, body in short_to_body_mapping.items():
            if body.is_hash_key:
                hash_key_name = short_name
                continue
            if body.is_range_key:
                range_key_name = short_name
                continue
        return hash_key_name, range_key_name
