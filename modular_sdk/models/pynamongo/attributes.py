import json
from datetime import datetime, timezone
from typing import Any

from pynamodb.attributes import (
    Attribute,
    BinaryAttribute,
    BinarySetAttribute,
    BooleanAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    NumberSetAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.constants import BINARY, LIST, NUMBER, STRING
from pynamodb.exceptions import AttributeDeserializationError

# Ephemeral attribute type for Mongo to proxy python object to Pymongo
# without changing
AS_IS = 'AS_IS'


class MongoUTCDateTimeAttribute(Attribute[datetime]):
    attr_type = AS_IS

    def serialize(self, value):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def deserialize(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


class MongoBinaryAttribute(Attribute[bytes]):
    attr_type = BINARY

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


class MongoBinarySetAttribute(Attribute[set[bytes]]):
    attr_type = LIST
    null = True

    def serialize(self, value):
        return [{BINARY: v} for v in value] or None

    def deserialize(self, value):
        return {v[BINARY] for v in value}


class MongoNumberSetAttribute(Attribute[set[float]]):
    attr_type = LIST
    null = True

    def serialize(self, value):
        return [{NUMBER: json.dumps(v)} for v in value] or None

    def deserialize(self, value):
        return {json.loads(v[NUMBER]) for v in value}


class MongoUnicodeSetAttribute(Attribute[set[str]]):
    attr_type = LIST
    null = True

    def serialize(self, value: set[str]) -> list[dict] | None:
        return [{STRING: v} for v in value] or None

    def deserialize(self, value: list[dict]) -> set[str]:
        return {v[STRING] for v in value}


class DynamicAttribute(Attribute[Any]):
    """
    This one should work both for Mongo and DynamoDB.
    Not thread safe
    """

    _type_attr = {
        str: UnicodeAttribute(),
        bool: BooleanAttribute(),
        dict: MapAttribute(),
        int: NumberAttribute(),
        float: NumberAttribute(),
        list: ListAttribute(),
        tuple: ListAttribute(),
    }
    _dynamo_typ_attr = {attr.attr_type: attr for attr in _type_attr.values()}

    def serialize(self, value: Any) -> Any:
        attr = self._type_attr.get(type(value))
        if not attr:
            raise NotImplementedError(
                f'{type(value).__name__} is not supported for DynamicAttribute'
            )
        self.attr_type = attr.attr_type
        return attr.serialize(value)

    def deserialize(self, value: Any) -> Any:
        attr = self._dynamo_typ_attr.get(self.attr_type)
        if not attr:
            raise AttributeDeserializationError(self.attr_name, self.attr_type)
        return attr.deserialize(value)

    def get_value(self, value: dict[str, Any]) -> Any:
        attr_type, attr_value = next(iter(value.items()))
        self.attr_type = attr_type
        return attr_value


# NOTE: seems like NullAttribute also can be patched to return True when
# deserialized. The existing behaviour seems broken because currently
# it returns None when deserialized and then if you try to save the
# instance it will be omitted.
MONGO_ATTRIBUTE_PATCH_MAPPING = {
    UTCDateTimeAttribute: MongoUTCDateTimeAttribute,
    BinaryAttribute: MongoBinaryAttribute,
    BinarySetAttribute: MongoBinarySetAttribute,
    NumberSetAttribute: MongoNumberSetAttribute,
    UnicodeSetAttribute: MongoUnicodeSetAttribute,
}
