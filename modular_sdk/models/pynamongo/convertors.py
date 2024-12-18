import json
import re
from itertools import islice
from typing import TYPE_CHECKING, Any

from pynamodb.attributes import Attribute
from pynamodb.constants import BINARY, BOOLEAN, LIST, MAP, NULL, NUMBER, STRING
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path, Value, _ListAppend
from pynamodb.expressions.update import Action, RemoveAction, SetAction, AddAction

from modular_sdk.models.pynamongo.attributes import AS_IS

if TYPE_CHECKING:
    from bson.objectid import ObjectId
    from pynamodb.indexes import Index
    from pynamodb.models import Model


def attribute_value_to_mongo(dct: dict[str, Any]) -> Any:
    typ, value = next(iter(dct.items()))
    if typ in (AS_IS, BINARY, STRING, BOOLEAN):
        return value
    if typ == LIST:
        return [attribute_value_to_mongo(v) for v in value]
    if typ == MAP:
        return {k: attribute_value_to_mongo(v) for k, v in value.items()}
    if typ == NULL:
        return None
    if typ == NUMBER:
        return json.loads(value)
    raise ValueError(f'Not supported attribute type for MongoDB: {typ}')


def mongo_to_attribute_value(value: Any) -> dict[str, Any]:
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
        return {LIST: [mongo_to_attribute_value(v) for v in value]}
    if isinstance(value, dict):
        return {
            MAP: {k: mongo_to_attribute_value(v) for k, v in value.items()}
        }
    return {AS_IS: value}


class PynamoDBModelToMongoDictSerializer:
    __slots__ = ()
    _mongo_id_attr = '__mongo_id__'

    def serialize(self, instance: 'Model') -> dict:
        # TODO: encode not supported keys?
        return {
            k: attribute_value_to_mongo(v)
            for k, v in instance.serialize().items()
        }

    def deserialize_to(self, instance: 'Model', dct: dict[str, Any]) -> None:
        # TODO: DynamicMapAttribute has a bug: attribute_values always
        #  appear inside a dynamic map for some reason
        if _id := dct.pop('_id', None):
            self.set_mongo_id(instance, _id)
        instance.deserialize(
            {k: mongo_to_attribute_value(v) for k, v in dct.items()}
        )

    def deserialize(
        self, model: type['Model'], dct: dict[str, Any]
    ) -> 'Model':
        instance = model()
        if _id := dct.pop('_id', None):
            self.set_mongo_id(instance, _id)
        instance.deserialize(
            {k: mongo_to_attribute_value(v) for k, v in dct.items()}
        )
        return instance

    @classmethod
    def get_mongo_id(cls, instance: 'Model') -> 'ObjectId | None':
        return getattr(instance, cls._mongo_id_attr, None)

    @classmethod
    def set_mongo_id(cls, instance: 'Model', _id: 'ObjectId'):
        setattr(instance, cls._mongo_id_attr, _id)

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


# Looks for [1], [2], [12], etc in a string
INDEX_REGEX = re.compile(r'\[\d+\]')
COMPARISON_MAP = {
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
    '<>': '$ne',
}


def value_to_raw(value: Value) -> Any:
    """
    PynamoDB operand Value contains only one element in a list. This
    element is a dict: {'pynamo type': 'value'}
    :param value:
    :return:
    """
    return attribute_value_to_mongo(value.value)


def path_to_raw(path: Path) -> str:
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
    for index in re.findall(INDEX_REGEX, raw):
        n = index.strip('[]')
        raw = raw.replace(index, f'.{n}', 1)
    return raw


def convert_condition_expression(condition: Condition) -> dict:
    """
    Converts PynamoDB conditions to MongoDB query map. Supported classes from
    `pynamodb.expressions.condition`: Comparison, Between, In, Exists,
    NotExists, BeginsWith, Contains, And, Or, Not.

    IsType and size are not supported. Add support if you want
    """
    op = condition.operator
    if op == 'OR':
        return {
            '$or': [
                convert_condition_expression(cond) for cond in condition.values
            ]
        }
    if op == 'AND':
        return {
            '$and': [
                convert_condition_expression(cond) for cond in condition.values
            ]
        }
    if op == 'NOT':
        return {'$nor': [convert_condition_expression(condition.values[0])]}
    if op == 'attribute_exists':
        return {path_to_raw(condition.values[0]): {'$exists': True}}
    if op == 'attribute_not_exists':
        return {path_to_raw(condition.values[0]): {'$exists': False}}
    if op == 'contains':
        return {
            path_to_raw(condition.values[0]): {
                '$regex': value_to_raw(condition.values[1])
            }
        }
    if op == 'IN':
        return {
            path_to_raw(condition.values[0]): {
                '$in': list(
                    value_to_raw(v) for v in islice(condition.values, 1, None)
                )
            }
        }
    if op == '=':
        return {
            path_to_raw(condition.values[0]): value_to_raw(condition.values[1])
        }
    if op in COMPARISON_MAP:
        return {
            path_to_raw(condition.values[0]): {
                COMPARISON_MAP[op]: value_to_raw(condition.values[1])
            }
        }
    if op == 'BETWEEN':
        return {
            path_to_raw(condition.values[0]): {
                '$gte': value_to_raw(condition.values[1]),
                '$lte': value_to_raw(condition.values[2]),
            }
        }
    if op == 'begins_with':
        return {
            path_to_raw(condition.values[0]): {
                '$regex': f'^{value_to_raw(condition.values[1])}'
            }
        }
    raise NotImplementedError(f'Operator: {op} is not supported')


def convert_update_expression(action: Action) -> dict:
    """
    Currently just SetAction and RemoveAction, ListAppend, ListPrepend
    are supported, you can implement increment and decrement
    Working:
    - set for Any
    - remove for Any
    - add for Number
    - append for List
    - prepend for List

    Somewhat working:
    - remove for Element of list: sets to null instead of removing

    Not working:
    - add for Set
    - delete for Set
    - add that depends on another attribute: "Model.a.set(Model.b + 2)" and
      other recursive stuff
    - set if not exists
    - increment
    - decrement
    """
    if isinstance(action, SetAction):
        path, value = action.values
        if isinstance(value, Value):
            return {'$set': {path_to_raw(path): value_to_raw(value)}}
        # appending from one list to another is not supported.
        # However, Dynamo seems to support it
        if isinstance(value, _ListAppend):
            if isinstance(value.values[0], Path):  # append
                return {
                    '$push': {
                        path_to_raw(path): {
                            '$each': value_to_raw(value.values[1])
                        }
                    }
                }
            else:  # prepend
                return {
                    '$push': {
                        path_to_raw(path): {
                            '$each': value_to_raw(value.values[0]),
                            '$position': 0,
                        }
                    }
                }
        # does not work, but seems like the idea is correct.
        # Only need to make right mongo query
        # if isinstance(value, _Increment):
        #     return {
        #         '$set': {path_to_raw(path): {
        #             '$add': [f'${path_to_raw(value.values[0])}', int(value_to_raw(value.values[1]))]  # make sure it's int, it is your responsibility
        #         }}
        #     }
        # if isinstance(value, _Decrement):
        #     return {
        #         '$set': {path_to_raw(path): {
        #             '$add': [f'${path_to_raw(value.values[0])}', -int(value_to_raw(value.values[1]))]  # make sure it's int, it is your responsibility
        #         }}
        #     }
        raise NotImplementedError(
            f'Operand of type: {value.__class__.__name__} not supported'
        )
    if isinstance(action, RemoveAction):
        (path,) = action.values
        # TODO: make $pull here?
        return {'$unset': {path_to_raw(path): ''}}
    if isinstance(action, AddAction):
        path, value = action.values
        return {'$inc': {path_to_raw(path): value_to_raw(value)}}
    raise NotImplementedError(
        f'Action {action.__class__.__name__} is not implemented'
    )


def convert_attributes_to_get(attributes_to_get=None) -> tuple[str, ...]:
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
