import json
from datetime import datetime, timezone

from pynamodb.attributes import Attribute, UTCDateTimeAttribute, \
    BinaryAttribute, BinarySetAttribute, NumberSetAttribute, \
    UnicodeSetAttribute
from pynamodb.constants import LIST, BINARY, NUMBER, STRING

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

    def serialize(self, value: set[str]) -> list[str] | None:
        return [{STRING: v} for v in value] or None

    def deserialize(self, value: list[str]) -> set[str]:
        return {v[STRING] for v in value}


# NOTE: seems like NullAttribute also can be patched to return True when
# deserialized. The existing behaviour seems broken because currently
# it returns None when deserialized and then if you try to save the
# instance it will be omitted.
MONGO_ATTRIBUTE_PATCH_MAPPING = {
    UTCDateTimeAttribute: MongoUTCDateTimeAttribute,
    BinaryAttribute: MongoBinaryAttribute,
    BinarySetAttribute: MongoBinarySetAttribute,
    NumberSetAttribute: MongoNumberSetAttribute,
    UnicodeSetAttribute: MongoUnicodeSetAttribute
}
