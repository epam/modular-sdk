import decimal
import json
import re
from itertools import islice
from typing import TYPE_CHECKING, Any, TypeVar

from pynamodb.attributes import Attribute
from pynamodb.constants import BINARY, BOOLEAN, LIST, MAP, NULL, NUMBER, STRING
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path, Value, _ListAppend
from pynamodb.expressions.update import Action, RemoveAction, SetAction

from modular_sdk.commons import DynamoDBJsonSerializer
from modular_sdk.models.pynamongo.attributes import AS_IS

if TYPE_CHECKING:
    from pynamodb.models import Model
    from pynamodb.indexes import Index
    from bson.objectid import ObjectId


class PynamoDBModelToMongoDictSerializer:
    __slots__ = ()

    @classmethod
    def _to_mongo(cls, dct: dict[str, Any]):
        typ, value = next(iter(dct.items()))
        if typ in (AS_IS, BINARY, STRING, BOOLEAN):
            return value
        if typ == LIST:
            return [cls._to_mongo(v) for v in value]
        if typ == MAP:
            return {k: cls._to_mongo(v) for k, v in value.items()}
        if typ == NULL:
            return None
        if typ == NUMBER:
            return json.loads(value)
        raise ValueError(f'Not supported attribute type for MongoDB: {typ}')

    @classmethod
    def _from_mongo(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {NULL: True}
        if value is True or value is False:
            return {BOOLEAN: value}
        if isinstance(value, (int, float)):
            return {NUMBER: json.dumps(value)}
        if isinstance(value, bytes):
            return {BINARY: value}
        if isinstance(value, str):
            return {STRING: value}
        if isinstance(value, list):
            return {LIST: [cls._from_mongo(v) for v in value]}
        if isinstance(value, dict):
            return {MAP: {k: cls._from_mongo(v) for k, v in value.items()}}
        return {AS_IS: value}

    def serialize(self, instance: 'Model') -> dict:
        # TODO: encode not supported keys
        return {k: self._to_mongo(v) for k, v in instance.serialize().items()}

    def deserialize_to(self, instance: 'Model', dct: dict[str, Any]) -> None:
        # TODO: DynamicMapAttribute has a bug: attribute_values always
        #  appear inside a dynamic map for some reason
        if _id := dct.pop('_id', None):
            self.set_mongo_id(instance, _id)
        instance.deserialize({k: self._from_mongo(v) for k, v in dct.items()})

    def deserialize(
        self, model: type['Model'], dct: dict[str, Any]
    ) -> 'Model':
        instance = model()
        if _id := dct.pop('_id', None):
            self.set_mongo_id(instance, _id)
        instance.deserialize({k: self._from_mongo(v) for k, v in dct.items()})
        return instance

    @staticmethod
    def get_mongo_id(instance: 'Model') -> 'ObjectId | None':
        return getattr(instance, '__mongo_id__', None)

    @staticmethod
    def set_mongo_id(instance: 'Model', _id: 'ObjectId'):
        setattr(instance, '__mongo_id__', _id)

    # all methods that use private members of PynamoDB are below
    @staticmethod
    def instance_serialized_keys(instance: 'Model') -> dict[str, Any]:
        """
        Returns a dict with one or two items: Hash/Range attr name to
        serialized value
        """
        return instance._get_keys()

    @staticmethod
    def serialize_keys(
        model: type['Model'], hash_key, range_key=None
    ) -> tuple[Any, Any]:
        """
        The same as one above but without keys, only values in correct order
        """
        return model._serialize_keys(hash_key, range_key)

    @staticmethod
    def model_keys_names(model: type['Model']) -> tuple[str, str | None]:
        """
        Only keys names in correct order
        """
        h = model._hash_key_attribute().attr_name
        r = None
        if r_name := model._range_key_attribute():
            r = r_name
        return h, r

    @staticmethod
    def model_indexes(model: type['Model']) -> dict:
        return model._indexes

    @staticmethod
    def index_keys(index: 'Index') -> tuple['Attribute', 'Attribute | None']:
        """
        Returns the attribute class for the range key.
        One may wonder why PynamoDB 5.5.1 does not have this method...
        """
        h, r = None, None
        for attr_cls in index.Meta.attributes.values():
            if attr_cls.is_hash_key:
                h = attr_cls
            elif attr_cls.is_range_key:
                r = attr_cls
        return h, r


T = TypeVar('T')


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
    def value_to_raw(value: Value) -> str | dict | list | int | float:
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

    comparison_map = {
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
        '<>': '$ne',
    }

    @classmethod
    def convert(cls, condition: Condition) -> dict:
        op = condition.operator
        if op == 'OR':
            return {'$or': [cls.convert(cond) for cond in condition.values]}
        if op == 'AND':
            return {'$and': [cls.convert(cond) for cond in condition.values]}
        if op == 'NOT':
            return {'$nor': [cls.convert(condition.values[0])]}
        if op == 'attribute_exists':
            return {cls.path_to_raw(condition.values[0]): {'$exists': True}}
        if op == 'attribute_not_exists':
            return {cls.path_to_raw(condition.values[0]): {'$exists': False}}
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
                        cls.value_to_raw(v)
                        for v in islice(condition.values, 1, None)
                    )
                }
            }
        if op == '=':
            return {
                cls.path_to_raw(condition.values[0]): cls.value_to_raw(
                    condition.values[1]
                )
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
                    '$lte': cls.value_to_raw(condition.values[2]),
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
            if isinstance(
                value, _ListAppend
            ):  # appending from one list to another is not supported. However, Dynamo seems to support it
                if isinstance(value.values[0], Path):  # append
                    return {
                        '$push': {
                            cls.path_to_raw(path): {
                                '$each': cls.value_to_raw(value.values[1])
                            }
                        }
                    }
                else:  # prepend
                    return {
                        '$push': {
                            cls.path_to_raw(path): {
                                '$each': cls.value_to_raw(value.values[0]),
                                '$position': 0,
                            }
                        }
                    }
            # does not work, but seems like the idea is correct.
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
            (path,) = action.values
            return {
                '$unset': {cls.path_to_raw(path): ''}
                # empty string does not matter https://www.mongodb.com/docs/manual/reference/operator/update/unset/#mongodb-update-up.-unset
            }
        raise NotImplementedError(
            f'Action {action.__class__.__name__} is not implemented'
        )


class AttributesToGetToProjectionConvertor:
    __slots__ = ()

    @staticmethod
    def convert(attributes_to_get=None) -> tuple[str, ...]:
        if not attributes_to_get:
            return ()
        if not isinstance(attributes_to_get, (list, tuple)):
            attributes_to_get = (attributes_to_get,)
        res = set()
        for attr in attributes_to_get:
            path = None
            if isinstance(attr, Attribute):
                path = attr.attr_path
            elif isinstance(attr, Path):
                path = attr.path
            elif isinstance(attr, str):
                path = attr.split('.')
            if path:
                res.add('.'.join(path))
        return tuple(res)
