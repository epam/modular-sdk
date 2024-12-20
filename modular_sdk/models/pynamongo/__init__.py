"""
pynamodb 5.5.1
pymongo 4.10.1

These module provides a way to interact with MongoDB using PynamoDB ORM.
You may assume that not all the features are covered and you would be right.
Yet most useful cases (cases that are used by us) are covered.

First, what you should understands is how PynamoDB models are converted to
MongoDB documents. That is done in two steps:
- call .serialize() method on PynamoDB model
- convert the result from DynamoDB JSON to dict that can be accepted by
  Mongo (means removing all DynamoDB types: {"S": "value"} -> "value")
The models are converted this way in order to use PynamoDB's serialization code
that handles all maps/lists traversals, correct serialization of each attr
and validation. Respectively, when MongoDB documents first converted to
DynamoDB JSON and then .deserialize() method is called.

Keep that in mind and look that the table below:

PynamoDB Type        | DynamoDB type | Mongo type
---------------------+---------------+-----------
UnicodeAttribute     | S             | String
NumberAttribute      | N             | Double
BinaryAttribute      | B             | Binary
BooleanAttribute     | BOOL          | Boolean
NullAttribute        | NULL          | Null
---------------------+---------------+-----------
MapAttribute         | M             | Object
DynamicMapAttribute  | M             | Object
ListAttribute        | L             | Array
---------------------+---------------+-----------
UTCDateTimeAttribute | S             | Date
JSONAttribute        | S             | String
---------------------+---------------+-----------
UnicodeSetAttribute  | SS            | Array
NumberSetAttribute   | NS            | Array
BinarySetAttribute   | BS            | Array




What private PynamoDB methods are used:
TODO:

Caveats to know:
- NullAttribute
- DynamicMapping
- Sets
- BinaryAttribute
- dots not supported as in keys


What PynamoDB features are not supported:
- Polymorphism through the use of discriminators (https://pynamodb.readthedocs.io/en/stable/polymorphism.html)
- Transaction Operations (https://pynamodb.readthedocs.io/en/stable/transaction.html)
- Conditional writes (https://pynamodb.readthedocs.io/en/stable/conditional.html#conditioning-on-keys)

"""
from .adapter import PynamoDBToPymongoAdapter, ResultIterator
from .attributes import (
    AS_IS,
    BinaryAttribute,
    DynamicAttribute,
    EnumUnicodeAttribute,
    M3BooleanAttribute,
    MongoBinarySetAttribute,
    MongoNumberSetAttribute,
    MongoUnicodeSetAttribute,
    MongoUTCDateTimeAttribute,
    UUIDAttribute,
)
from .convertors import PynamoDBModelToMongoDictSerializer
from .patch import patch_attributes

__all__ = (
    'PynamoDBToPymongoAdapter',
    'ResultIterator',
    'AS_IS',
    'BinaryAttribute',
    'DynamicAttribute',
    'EnumUnicodeAttribute',
    'M3BooleanAttribute',
    'MongoBinarySetAttribute',
    'MongoNumberSetAttribute',
    'MongoUnicodeSetAttribute',
    'MongoUTCDateTimeAttribute',
    'UUIDAttribute',
    'PynamoDBModelToMongoDictSerializer',
    'patch_attributes',
)
