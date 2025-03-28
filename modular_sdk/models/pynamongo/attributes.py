import json
from datetime import datetime, timezone
from typing import Any
from enum import Enum
from uuid import UUID

from pynamodb.attributes import (
    Attribute,
    BinaryAttribute as PynamoDBBinaryAttribute,
    BinarySetAttribute,
    BooleanAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    NumberSetAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
    TTLAttribute
)
from pynamodb.constants import BINARY, LIST, NUMBER, STRING, BOOLEAN
from pynamodb.exceptions import AttributeDeserializationError

# Ephemeral attribute type for Mongo to proxy python object to Pymongo
# without changing
AS_IS = 'AS_IS'


class MongoUTCDateTimeAttribute(Attribute[datetime]):
    """
    Be careful: if you pass a tz-unaware datetime object here, its actual value
    will be considered already converted to UTC. It means that if your local
    timezone is, say, +2 hours and you do something like:

        model.datetime_attr = datetime(2024, 12, 10, 15)
        model.save()

    DB will have such value: "2024-12-10T15:00:00.000+00:00" (+2 hours
    local padding is missing).
    So pass tz-unaware objects only if you 100% sure your timezone is UTC. Or
    better always pass tz-aware objects:

        model.datetime_attr = datetime.now(timezone.utc)

    or:
        dt = datetime(2024, 12, 10, 15).astimezone(timezone.utc)
        model.datetime_attr = dt

    """
    attr_type = AS_IS

    def serialize(self, value):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def deserialize(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


class MongoTTLAttribute(TTLAttribute):
    """
    Mongo requires its ttl attributes to have Date type whereas DynamoDB need just a Number.
    """
    attr_type = AS_IS

    def serialize(self, value):
        if value is None:
            return None
        return self._normalize(value)

    def deserialize(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
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


# Attributes below can be used for DynamoDB
class BinaryAttribute(Attribute[bytes]):
    # NOTE: PynamoDB's BinaryAttribute encodes values to base64 twice. This
    # implementation fixes that. Not backward-compatible

    attr_type = BINARY

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


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
        bytes: BinaryAttribute()
    }
    _dynamo_typ_attr = {attr.attr_type: attr for attr in _type_attr.values()}

    def serialize(self, value: Any) -> Any:
        attr = self._type_attr.get(type(value))
        if not attr:
            raise NotImplementedError(
                f'{type(value).__name__} is not supported for DynamicAttribute'
            )
        self.attr_type = attr.attr_type  # shared state, not thread-safe
        return attr.serialize(value)

    def deserialize(self, value: tuple[str, Any]) -> Any:
        attr = self._dynamo_typ_attr.get(value[0])
        if not attr:
            raise AttributeDeserializationError(self.attr_name, value[0])
        return attr.deserialize(value[1])

    def get_value(self, value: dict[str, Any]) -> tuple[str, Any]:
        # NOTE: the result of get_value is always passed to the deserialize method (at least in pynamodb==5.5.1)
        #  so, seems like we can safely slightly change the interface of this method to avoid shared state.
        #  We must be able to know somehow the type of this dynamic attribute inside "deserialize". We used to store
        #  that type as a temp instance attribute but that seems not thread-safe since all the instances of model use
        #  this one descriptor class. So returning attr_value and attr_type as a tuple
        return next(iter(value.items()))


class EnumUnicodeAttribute(Attribute[Enum | str]):
    attr_type = STRING

    def __init__(self, enum: type[Enum], **kwargs):
        self._enum = enum
        super().__init__(**kwargs)

    def serialize(self, value: Enum | str) -> str:
        if not isinstance(value, Enum):
            value = self._enum(value)  # user must provide valid values
        else:  # enum value
            if not isinstance(value, self._enum):
                raise TypeError(f'{value} has invalid type: {value.__class__}. '
                                f'Expected type if {self._enum}')
        return str(value.value)

    def deserialize(self, value: Any) -> Enum:
        try:
            return self._enum(value)
        except ValueError:
            raise AttributeDeserializationError(self.attr_name, self.attr_type)


class M3BooleanAttribute(BooleanAttribute):

    def get_value(self, value: dict[str, Any]) -> Any:
        if BOOLEAN not in value and NUMBER not in value:
            raise AttributeDeserializationError(self.attr_name, self.attr_type)
        if value.get(BOOLEAN) is not None:
            return value[BOOLEAN]
        return int(value.get(NUMBER))


class UUIDAttribute(Attribute[UUID]):
    attr_type = STRING

    def __init__(self, without_dashes: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._without_dashes = without_dashes

    def serialize(self, value: UUID) -> str:
        if self._without_dashes:
            return value.hex
        return str(value)

    def deserialize(self, value: str) -> UUID:
        return UUID(value)


MONGO_ATTRIBUTE_PATCH_MAPPING = {
    UTCDateTimeAttribute: MongoUTCDateTimeAttribute,
    PynamoDBBinaryAttribute: BinaryAttribute,
    BinarySetAttribute: MongoBinarySetAttribute,
    NumberSetAttribute: MongoNumberSetAttribute,
    UnicodeSetAttribute: MongoUnicodeSetAttribute,
    TTLAttribute: MongoTTLAttribute
}
