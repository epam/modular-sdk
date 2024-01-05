import base64
import binascii
import json
import os
from datetime import datetime
from typing import (Any, Optional, Dict, Sequence, Iterable, Text, Union,
                    Iterator, Type, List)

from dynamodb_json import json_util
from dynamodb_json import json_util as dynamo_json
from pynamodb import indexes
from pynamodb import models
from pynamodb.attributes import (MapAttribute, Attribute, UnicodeAttribute,
                                 NumberAttribute, ListAttribute,
                                 BooleanAttribute, JSONAttribute)
from pynamodb.constants import BOOLEAN, NUMBER, LIST, MAP, \
    NULL, STRING
from pynamodb.exceptions import DoesNotExist, AttributeDeserializationError
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.indexes import _M
from pynamodb.models import _T, _KeyType, BatchWrite
from pynamodb.pagination import ResultIterator
from pynamodb.settings import OperationSettings

from modular_sdk.commons.constants import MODULAR_SERVICE_MODE_ENV, \
    SERVICE_MODE_DOCKER, PARAM_MONGO_USER, PARAM_MONGO_PASSWORD, \
    PARAM_MONGO_URL, PARAM_MONGO_DB_NAME
from modular_sdk.commons.helpers import classproperty
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.commons.time_helper import utc_iso

_LOG = get_logger(__name__)


def build_mongodb_uri(user: str, password: str, url: str) -> str:
    """
    Just makes the right formatting
    """
    return f'mongodb://{user}:{password}@{url}/'


class M3BooleanAttribute(BooleanAttribute):

    def get_value(self, value: Dict[str, Any]) -> Any:
        if BOOLEAN not in value and NUMBER not in value:
            raise AttributeDeserializationError(self.attr_name, self.attr_type)
        if value.get(BOOLEAN) is not None:
            return value[BOOLEAN]
        return int(value.get(NUMBER))


class MongoSpecificType(Attribute[Any]):
    """
    Sometimes our models have attributes with different types in
    DynamoDB and MongoDB. A good example is Tenant.regions.rId.
    IN DynamoDB it's a string, in MongoDB -> ObjectId.
    This custom attribute can be used to mark such different attributes if
    you know that they are different. Basically, it's the same as
    UnicodeAttribute but it kind of adds more transparency.
    Why exactly UnicodeAttribute - it does nothing to the value,
    returned from DB. Just proxies it. This way we can be sure that we
    will not corrupt some Mongo types (Date, Binary, ObjectId).
    """
    attr_type = STRING

    def serialize(self, value: Any) -> Any:
        """
        This method should return a dynamodb compatible value
        """
        return value

    def deserialize(self, value: Any) -> Any:
        """
        Performs any needed deserialization on the value
        """
        return value


class DynamicAttribute(Attribute):
    _types_to_attributes = {
        str: UnicodeAttribute,
        dict: MapAttribute,
        float: NumberAttribute,
        int: NumberAttribute,
        list: ListAttribute,
        tuple: ListAttribute,
        bool: BooleanAttribute,
        bytes: JSONAttribute  # todo, BinaryAttribute would fit better but
    }
    # ... but this class has a bug -> it does not perform data
    # deserialization. The raw value from DB is returned all the time.
    # For Unicode, Number, List, Bool it is more or less acceptable, and
    # it works. For BinaryAttribute it will not work because BinaryAttribute
    # class encodes the data to b64 before sending to DB. As you can guess,
    # it does not decode the data when you receive it from DB.

    def serialize(self, value: Any) -> Any:
        value_type = type(value)
        attribute_class = self._types_to_attributes.get(value_type)
        if not attribute_class:
            raise AssertionError(
                f'There is no serializer for type: {value_type}')

        if attribute_class is JSONAttribute and isinstance(value, bytes):
            value = json.loads(value)

        attribute_instance = attribute_class()
        self.attr_type = attribute_class.attr_type
        return attribute_instance.serialize(value)

    def get_value(self, value: Any) -> Any:
        attr_type = list(value.keys())
        if len(attr_type) > 1:
            raise AssertionError(
                f'Unexpected key specifier: {attr_type}:{len(attr_type)}; '
                f'Attribute: {value}')
        return dynamo_json.loads(value)


class ModelEncoder(json.JSONEncoder):
    """
    It converts the item to DTO only representation. Do not use
    get_json() on model and then write the result to DB again. Such
    actions corrupt the item
    """

    def default(self, obj):
        if hasattr(obj, 'attribute_values'):
            return obj.attribute_values
        elif isinstance(obj, datetime):
            return utc_iso(_from=obj)
        else:
            # ObjectId, bytes and others
            return str(obj)
        # return json.JSONEncoder.default(self, obj)


def json_to_attribute_value(value: Any) -> Dict[str, Any]:
    """
    Overrides the one from "pynamodb.util" to handle MongoDB specific
    attributes such as ObjectId, Date, Binary and others
    :param value:
    :return:
    """
    if value is None:
        return {NULL: True}
    if value is True or value is False:
        return {BOOLEAN: value}
    if isinstance(value, (int, float)):
        return {NUMBER: json.dumps(value)}
    if isinstance(value, str):
        return {STRING: value}
    if isinstance(value, list):
        return {LIST: [json_to_attribute_value(v) for v in value]}
    if isinstance(value, dict):
        return {MAP: {k: json_to_attribute_value(v) for k, v in value.items()}}
    # changed part below
    # In case we don't know how to convert an attribute, we just proxy it.
    # STRING is used because PynamoDB does nothing to change the value
    # in case it's type STRING. So, the value won't be corrupted.
    # It's used only for on-prem
    return {STRING: value}
    # raise ValueError("Unknown value type: {}".format(type(value).__name__))


class ABCMongoDBHandlerMixin:
    """
    Must NOT be inherited from :class:`abc.ABC` because it's used as a mixin
    with other classes, in particular with :class:`pynamodb.models.Model`
    which already has Metaclass. Making this class inherited from ABC entails
    "metaclass conflict".
    """
    _mongodb = None

    @classmethod
    def mongodb_handler(cls):
        raise NotImplementedError

    @classmethod
    def reset_mongodb(cls):
        cls._mongodb = None


class ModularMongoDBHandlerMixin(ABCMongoDBHandlerMixin):
    @classmethod
    def mongodb_handler(cls):
        if not cls._mongodb:
            from modular_sdk.connections.mongodb_connection import \
                MongoDBConnection
            from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter \
                import PynamoDBToPyMongoAdapter
            user = os.environ.get(PARAM_MONGO_USER)
            password = os.environ.get(PARAM_MONGO_PASSWORD)
            url = os.environ.get(PARAM_MONGO_URL)
            db = os.environ.get(PARAM_MONGO_DB_NAME)
            cls._mongodb = PynamoDBToPyMongoAdapter(
                mongodb_connection=MongoDBConnection(
                    build_mongodb_uri(user, password, url), db
                )
            )
        return cls._mongodb


class RawBaseModel(models.Model):
    """
    Raw abstract base model class which exposes the common API to interact
    both with DynamoDB and MongoDB. Nevertheless, it must not be used by
    itself. Use either BaseModel or create your own class with implemented
    `mongodb_handler`.
    """

    @classmethod
    def mongodb_handler(cls):
        """
        Must return an initialized PynamoDBToPyMongoAdapter or maybe some
        other class which implements its interface.
        :return: :class:`modular_sdkmodels.pynamodb_extension.pynamodb_to_pymongo_adapter.PynamoDBToPyMongoAdapter`
        """
        raise NotImplementedError(
            'You cannot use RawBaseModel by itself. Use BaseModel or'
            ' define your own base class with implemented `mongodb_handler` '
            'class method')

    @classproperty
    def is_docker(cls) -> bool:
        return os.environ.get(MODULAR_SERVICE_MODE_ENV) == SERVICE_MODE_DOCKER

    @classmethod
    def get_nullable(cls, hash_key, range_key=None, attributes_to_get=None,
                     consistent_read=False):
        if cls.is_docker:
            return cls.mongodb_handler().get_nullable(
                model_class=cls, hash_key=hash_key, sort_key=range_key)
        try:
            return cls.get(hash_key, range_key,
                           attributes_to_get=attributes_to_get,
                           consistent_read=consistent_read)
        except DoesNotExist as e:
            _LOG.debug(f'{cls.__name__} does not exist '
                       f'with the following keys: hash_key={hash_key}, '
                       f'range_key={range_key}: {e.msg}')
            return

    def save(self, condition: Optional[Condition] = None,
             settings: OperationSettings = OperationSettings.default
             ) -> Dict[str, Any]:
        if self.is_docker:
            return self.mongodb_handler().save(model_instance=self)
        return super().save(condition, settings)

    @classmethod
    def batch_get(
            cls: Type[_T],
            items: Iterable[Union[_KeyType, Iterable[_KeyType]]],
            consistent_read: Optional[bool] = None,
            attributes_to_get: Optional[Sequence[str]] = None,
            settings: OperationSettings = OperationSettings.default
    ) -> Iterator[_T]:
        if cls.is_docker:
            return cls.mongodb_handler().batch_get(
                model_class=cls,
                items=items,
                attributes_to_get=attributes_to_get)
        return super().batch_get(items, consistent_read, attributes_to_get,
                                 settings)

    @classmethod
    def batch_write(cls: Type[_T], auto_commit: bool = True,
                    settings: OperationSettings = OperationSettings.default
                    ) -> BatchWrite[_T]:
        if cls.is_docker:
            return cls.mongodb_handler().batch_write(model_class=cls)
        return super().batch_write(auto_commit, settings)

    def delete(self, condition: Optional[Condition] = None,
               settings: OperationSettings = OperationSettings.default) -> Any:
        if self.is_docker:
            return self.mongodb_handler().delete(model_instance=self)
        return super().delete(condition, settings)

    def update(self, actions: List[Action],
               condition: Optional[Condition] = None,
               settings: OperationSettings = OperationSettings.default) -> Any:
        if self.is_docker:
            return self.mongodb_handler().update(self, actions, condition,
                                                 settings)
        return super().update(actions, condition, settings)

    def refresh(self, consistent_read: bool = False,
                settings: OperationSettings = OperationSettings.default
                ) -> None:
        if self.is_docker:
            return self.mongodb_handler().refresh(
                consistent_read=consistent_read)
        return super().refresh(consistent_read, settings)

    @classmethod
    def get(
            cls: Type[_T],
            hash_key: _KeyType,
            range_key: Optional[_KeyType] = None,
            consistent_read: bool = False,
            attributes_to_get: Optional[Sequence[Text]] = None,
            settings: OperationSettings = OperationSettings.default
    ) -> _T:
        if cls.is_docker:
            return cls.mongodb_handler().get(
                model_class=cls, hash_key=hash_key, range_key=range_key)
        return super().get(hash_key, range_key, consistent_read,
                           attributes_to_get, settings)

    @classmethod
    def count(
            cls: Type[_T],
            hash_key: Optional[_KeyType] = None,
            range_key_condition: Optional[Condition] = None,
            filter_condition: Optional[Condition] = None,
            consistent_read: bool = False,
            index_name: Optional[str] = None,
            limit: Optional[int] = None,
            rate_limit: Optional[float] = None,
            settings: OperationSettings = OperationSettings.default,
    ) -> int:
        if cls.is_docker:
            return cls.mongodb_handler().count(
                model_class=cls,
                hash_key=hash_key,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                index_name=index_name,
                limit=limit
            )
        return super().count(hash_key, range_key_condition, filter_condition,
                             consistent_read, index_name, limit, rate_limit,
                             settings)

    @classmethod
    def query(
            cls: Type[_T],
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
        if cls.is_docker:
            return cls.mongodb_handler().query(
                model_class=cls,
                hash_key=hash_key,
                filter_condition=filter_condition,
                range_key_condition=range_key_condition,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                attributes_to_get=attributes_to_get,
                scan_index_forward=scan_index_forward
            )
        return super().query(hash_key, range_key_condition, filter_condition,
                             consistent_read, index_name, scan_index_forward,
                             limit, last_evaluated_key, attributes_to_get,
                             page_size, rate_limit, settings)

    @classmethod
    def scan(
            cls: Type[_T],
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
        if cls.is_docker:
            return cls.mongodb_handler().scan(
                model_class=cls,
                filter_condition=filter_condition,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                attributes_to_get=attributes_to_get
            )
        return super().scan(filter_condition, segment, total_segments, limit,
                            last_evaluated_key, page_size, consistent_read,
                            index_name, rate_limit, attributes_to_get,
                            settings)

    def get_json(self) -> dict:
        """
        Returns dict which can be dumped to JSON. So, in case the model
        contains Date or Binary, or ObjectId -> they will become strings.
        :return:
        """
        return json.loads(json.dumps(self, cls=ModelEncoder))

    def dynamodb_model(self):
        model = self.__unmap_map_attribute(item=self)
        result = self.__model_to_dict(model)
        if hasattr(self, 'mongo_id'):
            result['mongo_id'] = self.mongo_id
        return result

    def get_keys(self):
        return self._get_keys()

    def __model_to_dict(self, model):
        try:
            items = model.items()
        except AttributeError:
            return model
        for key, value in items:
            if isinstance(value, MapAttribute):
                processed_value = self.__unmap_map_attribute(item=value)
                model[key] = self.__model_to_dict(processed_value)
            elif isinstance(value, list):
                model[key] = [self.__model_to_dict(
                    self.__unmap_map_attribute(item=each))
                    for each in value]
            elif isinstance(value, dict):
                # just in case there is datetime inside
                value = self.__model_to_dict(value)
                model[key] = json_util.loads(value)
            # elif isinstance(value, datetime):
            #     model[key] = utc_iso(_from=value)
        return model

    @classmethod
    def from_json(cls, model_json: dict,
                  attributes_to_get: Optional[List] = None,
                  instance: Optional[_T] = None) -> models.Model:
        _id = model_json.pop('_id', None)
        instance = instance or cls()
        if attributes_to_get:
            to_get = set(
                attr.attr_name if isinstance(attr, Attribute) else attr
                for attr in attributes_to_get
            )
            model_json = {k: v for k, v in model_json.items() if k in to_get}

        attribute_values = {k: json_to_attribute_value(v) for k, v in
                            model_json.items()}
        # if uncommented, custom DynamicAttribute won't work due to
        # attr_type property
        # instance._update_attribute_types(attribute_values)
        instance.deserialize(attribute_values)
        instance.mongo_id = _id
        return instance

    @staticmethod
    def __unmap_map_attribute(item):
        try:
            attr_values = item.attribute_values
        except AttributeError:
            return item
        if not attr_values or (isinstance(item, MapAttribute) and type(item)
                               == MapAttribute):
            return attr_values
        processed_item = {}
        for attr_value_key in attr_values.keys():
            py_to_ddb = {py_key: db_key
                         for db_key, py_key in
                         item._dynamo_to_python_attrs.items()}
            match = py_to_ddb.get(attr_value_key)
            if match:
                processed_item[match] = attr_values[attr_value_key]
            else:
                processed_item[attr_value_key] = attr_values[attr_value_key]
        return processed_item

    def __repr__(self):
        return str(self.__dict__)


class RawBaseGSI(indexes.GlobalSecondaryIndex):
    @classproperty
    def is_docker(cls) -> bool:
        return os.environ.get(MODULAR_SERVICE_MODE_ENV) == SERVICE_MODE_DOCKER

    @classmethod
    def _range_key_attribute(cls) -> Attribute:
        """
        Returns the attribute class for the range key.
        One may wonder why PynamoDB 5.2.1 does not have this method...
        """
        for attr_cls in cls.Meta.attributes.values():
            if attr_cls.is_range_key:
                return attr_cls

    @classmethod
    def mongodb_handler(cls):
        """
        Must return an initialized PynamoDBToPyMongoAdapter or maybe some
        other class which implements its interface.
        :return: :class:`modular_sdkmodels.pynamodb_extension.pynamodb_to_pymongo_adapter.PynamoDBToPyMongoAdapter`
        """
        raise NotImplementedError(
            'You cannot use RawBaseModel by itself. Use BaseModel or'
            ' define your own base class with implemented `mongodb_handler` '
            'class method')

    @classmethod
    def query(cls, hash_key: _KeyType,
              range_key_condition: Optional[Condition] = None,
              filter_condition: Optional[Condition] = None,
              consistent_read: Optional[bool] = False,
              scan_index_forward: Optional[bool] = None,
              limit: Optional[int] = None,
              last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
              attributes_to_get: Optional[List[str]] = None,
              page_size: Optional[int] = None,
              rate_limit: Optional[float] = None) -> ResultIterator[_M]:
        if cls.is_docker:
            return cls.mongodb_handler().query(
                model_class=cls,
                hash_key=hash_key,
                filter_condition=filter_condition,
                range_key_condition=range_key_condition,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                attributes_to_get=attributes_to_get,
                scan_index_forward=scan_index_forward
            )
        return super().query(hash_key, range_key_condition, filter_condition,
                             consistent_read, scan_index_forward, limit,
                             last_evaluated_key, attributes_to_get, page_size,
                             rate_limit)


class BaseModel(ModularMongoDBHandlerMixin, RawBaseModel):
    pass


class BaseGSI(ModularMongoDBHandlerMixin, RawBaseGSI):
    pass


class LastEvaluatedKey:
    """
    Simple abstraction over DynamoDB last evaluated key & MongoDB offset :)
    """
    payload_key_name = 'key'

    def __init__(self, lek: Optional[Union[dict, int]] = None):
        self._lek = lek

    def serialize(self) -> str:
        payload = {self.payload_key_name: self._lek}
        return base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        ).decode()

    @classmethod
    def deserialize(cls, s: Optional[str] = None) -> 'LastEvaluatedKey':
        if not s or not isinstance(s, str):
            return cls()
        _payload = {}
        try:
            decoded = base64.urlsafe_b64decode(s.encode()).decode()
            _payload = json.loads(decoded)
        except binascii.Error:
            _LOG.warning('Invalid base64 encoding in last evaluated key token')
        except json.JSONDecodeError:
            _LOG.warning('Invalid json string within last evaluated key token')
        except Exception as e:  # you never know :)
            _LOG.warning('Some unexpected exception occurred while '
                         f'deserializing last evaluated key token : \'{e}\'')
        return cls(_payload.get(cls.payload_key_name))

    @property
    def value(self) -> Optional[Union[dict, int]]:
        return self._lek

    @value.setter
    def value(self, v: Optional[Union[dict, int]]):
        self._lek = v

    def __bool__(self) -> bool:
        return bool(self._lek)
