from datetime import datetime, timezone
from uuid import UUID

import pytest
from pynamodb.attributes import (
    BinaryAttribute,
    BinarySetAttribute,
    BooleanAttribute,
    JSONAttribute,
    ListAttribute,
    MapAttribute,
    NullAttribute,
    NumberAttribute,
    NumberSetAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.models import Model

from modular_sdk.commons.constants import Cloud
from modular_sdk.models.pynamongo.attributes import (
    DynamicAttribute,
    EnumUnicodeAttribute,
    UUIDAttribute,
)
from modular_sdk.models.pynamongo.convertors import (
    convert_attributes_to_get,
    convert_condition_expression,
    convert_update_expression,
    instance_as_dict,
    instance_as_json_dict,
)


class Nested(MapAttribute):
    one = UnicodeAttribute(attr_name='o')
    two = UnicodeAttribute(attr_name='t')


class TestModel(Model):
    string = UnicodeAttribute()
    short_string = UnicodeAttribute(attr_name='s')
    number = NumberAttribute(attr_name='num')
    map = MapAttribute()
    list = ListAttribute()
    nested = Nested(attr_name='n')


class SerializeTestModel(Model):
    string = UnicodeAttribute(hash_key=True, attr_name='s')
    number = NumberAttribute()
    boolean = BooleanAttribute()
    binary = BinaryAttribute()
    binary_set = BinarySetAttribute()
    unicode_set = UnicodeSetAttribute()
    number_set = NumberSetAttribute()
    json = JSONAttribute()
    utc_datetime = UTCDateTimeAttribute()
    null = NullAttribute()
    mapping = MapAttribute(default=dict)
    nested = Nested(default=dict)
    list = ListAttribute(of=MapAttribute, default=list)
    enum = EnumUnicodeAttribute(enum=Cloud)
    uuid = UUIDAttribute()
    dynamic = DynamicAttribute()
    nullable = UnicodeAttribute(null=True)


def test_convert_attributes_to_get():
    assert convert_attributes_to_get() == ()
    assert convert_attributes_to_get([TestModel.string]) == ('string',)
    assert set(
        convert_attributes_to_get(
            [TestModel.short_string, TestModel.map['key']]
        )
    ) == {'s', 'map.key'}
    assert set(
        convert_attributes_to_get([TestModel.list[0], TestModel.nested.one])
    ) == {'list[0]', 'n.o'}
    assert set(
        convert_attributes_to_get(
            [TestModel.list[0], TestModel.nested.one, TestModel.nested.two]
        )
    ) == {'list[0]', 'n.o', 'n.t'}
    assert set(
        convert_attributes_to_get(
            ('string', 's', 'map', TestModel.short_string)
        )
    ) == {'string', 's', 'map', 's'}


class TestConditionExpressionConvertor:
    def test_attr_exists(self):
        assert convert_condition_expression(TestModel.list.exists()) == {
            'list': {'$exists': True}
        }

    def test_attr_does_not_exists(self):
        assert convert_condition_expression(
            TestModel.list.does_not_exist()
        ) == {'list': {'$exists': False}}

    def test_contains(self):
        assert convert_condition_expression(
            TestModel.short_string.contains('test')
        ) == {'s': {'$regex': 'test'}}

    def test_is_in(self):
        assert convert_condition_expression(
            TestModel.map['key'].is_in('one', 'two')
        ) == {'map.key': {'$in': ['one', 'two']}}

    def test_equal(self):
        assert convert_condition_expression(
            TestModel.short_string == 'test'
        ) == {'s': 'test'}

    def test_gt(self):
        assert convert_condition_expression(TestModel.number > 10) == {
            'num': {'$gt': 10}
        }

    def test_lt(self):
        assert convert_condition_expression(TestModel.number < 10) == {
            'num': {'$lt': 10}
        }

    def test_gte(self):
        assert convert_condition_expression(TestModel.number >= 10) == {
            'num': {'$gte': 10}
        }

    def test_lte(self):
        assert convert_condition_expression(TestModel.number <= 10) == {
            'num': {'$lte': 10}
        }

    def test_not_equal(self):
        assert convert_condition_expression(
            TestModel.short_string != 'test'
        ) == {'s': {'$ne': 'test'}}

    def test_between(self):
        assert convert_condition_expression(
            TestModel.number.between(10, 20)
        ) == {'num': {'$gte': 10, '$lte': 20}}

    def test_begins_with(self):
        assert convert_condition_expression(
            TestModel.short_string.startswith('test')
        ) == {'s': {'$regex': '^test'}}

    def test_and(self):
        assert convert_condition_expression(
            (TestModel.number > 10) & (TestModel.short_string == 'test')
        ) == {'$and': [{'num': {'$gt': 10}}, {'s': 'test'}]}

    def test_or(self):
        assert convert_condition_expression(
            (TestModel.number > 10) | (TestModel.short_string == 'test')
        ) == {'$or': [{'num': {'$gt': 10}}, {'s': 'test'}]}

    def test_not(self):
        assert convert_condition_expression(~(TestModel.number > 10)) == {
            '$nor': [{'num': {'$gt': 10}}]
        }

    def test_complex_condition(self):
        cond = (
            (TestModel.map['one'] == 'one') & (TestModel.map['two'] == 'two')
            | (
                (TestModel.map['three'] == 'three')
                & (~TestModel.short_string.contains('sss'))
            )
            | (TestModel.nested.one.startswith('one'))
        )
        assert convert_condition_expression(cond) == {
            '$or': [
                {
                    '$or': [
                        {'$and': [{'map.one': 'one'}, {'map.two': 'two'}]},
                        {
                            '$and': [
                                {'map.three': 'three'},
                                {'$nor': [{'s': {'$regex': 'sss'}}]},
                            ]
                        },
                    ]
                },
                {'n.o': {'$regex': '^one'}},
            ]
        }


class TestUpdateExpressionConvertor:
    def test_set(self):
        assert convert_update_expression(
            TestModel.short_string.set('one')
        ) == {'$set': {'s': 'one'}}

    def test_set_list_item(self):
        assert convert_update_expression(TestModel.list[0].set(1)) == {
            '$set': {'list.0': 1}
        }

    def test_remove(self):
        assert convert_update_expression(TestModel.map['test'].remove()) == {
            '$unset': {'map.test': ''}
        }

    def test_remove_list_item(self):
        """
        With shifting
        """
        assert convert_update_expression(TestModel.list[10].remove()) == [
            {
                '$set': {
                    'list': {
                        '$concatArrays': [
                            {'$slice': ['$list', 10]},
                            {
                                '$slice': [
                                    '$list',
                                    {'$add': [1, 10]},
                                    {'$size': '$list'},
                                ]
                            },
                        ]
                    }
                }
            }
        ]

    def test_add_number(self):
        assert convert_update_expression(TestModel.number.add(10)) == {
            '$inc': {'num': 10}
        }

    def test_append(self):
        assert convert_update_expression(
            TestModel.list.set(TestModel.list.append([1, 2, 3]))
        ) == {'$push': {'list': {'$each': [1, 2, 3]}}}

    def test_prepend(self):
        assert convert_update_expression(
            TestModel.list.set(TestModel.list.prepend([1, 2, 3]))
        ) == {'$push': {'list': {'$each': [1, 2, 3], '$position': 0}}}

    def test_add_different_attr(self):
        assert convert_update_expression(
            TestModel.number.set(TestModel.map['number'] + 1)
        ) == [{'$set': {'num': {'$add': ['$map.number', 1]}}}]

    @pytest.mark.skip(reason='Not implemented')
    def test_add_to_set(self): ...

    @pytest.mark.skip(reason='Not implemented')
    def test_delete_from_set(self): ...

    @pytest.mark.skip(reason='Not implemented')
    def test_is_not_exists(self): ...


def test_instance_as_dict():
    dt = datetime(2025, 1, 29, 13, 41, 2, 945314, tzinfo=timezone.utc)
    u = UUID('b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34')
    item = SerializeTestModel(
        string='string',
        number=1.1,
        boolean=True,
        binary=b'data',
        binary_set={b'one', b'two', b'three'},
        unicode_set={'one', 'two', 'three'},
        number_set={1, 2, 3},
        json={'key': 'value'},
        utc_datetime=dt,
        null=None,
        mapping={'key': [1, 'two', {'key': dt}]},
        nested={'one': 1, 'two': 2},
        list=[{'k1': 'value', 'k2': 1, 'k3': u, 'k4': None}],
        enum=Cloud.AWS,
        uuid=u,
        dynamic={'key': 'value'},
        nullable=None,
    )
    assert instance_as_dict(item, exclude_none=False) == {
        'string': 'string',
        'number': 1.1,
        'boolean': True,
        'binary': b'data',
        'binary_set': {b'one', b'two', b'three'},
        'unicode_set': {'three', 'two', 'one'},
        'number_set': {1, 2, 3},
        'json': {'key': 'value'},
        'utc_datetime': dt,
        'null': None,
        'mapping': {'key': [1, 'two', {'key': dt}]},
        'nested': {'one': 1, 'two': 2},
        'list': [{'k1': 'value', 'k2': 1, 'k3': u, 'k4': None}],
        'enum': Cloud.AWS,
        'uuid': u,
        'dynamic': {'key': 'value'},
        'nullable': None,
    }
    assert instance_as_dict(item, exclude_none=True) == {
        'string': 'string',
        'number': 1.1,
        'boolean': True,
        'binary': b'data',
        'binary_set': {b'one', b'two', b'three'},
        'unicode_set': {'three', 'two', 'one'},
        'number_set': {1, 2, 3},
        'json': {'key': 'value'},
        'utc_datetime': dt,
        'mapping': {'key': [1, 'two', {'key': dt}]},
        'nested': {'one': 1, 'two': 2},
        'list': [{'k1': 'value', 'k2': 1, 'k3': u}],
        'enum': Cloud.AWS,
        'uuid': u,
        'dynamic': {'key': 'value'},
    }


def test_instance_as_json_dict():
    dt = datetime(2025, 1, 29, 13, 41, 2, 945314, tzinfo=timezone.utc)
    u = UUID('b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34')
    item = SerializeTestModel(
        string='string',
        number=1.1,
        boolean=True,
        binary=b'data',
        binary_set={b'one', b'two', b'three'},
        unicode_set={'one', 'two', 'three'},
        number_set={1, 2, 3},
        json={'key': 'value'},
        utc_datetime=dt,
        null=None,
        mapping={'key': [1, 'two', {'key': dt}]},
        nested={'one': 1, 'two': 2},
        list=[{'k1': 'value', 'k2': 1, 'k3': u, 'k4': None}],
        enum=Cloud.AWS,
        uuid=u,
        dynamic={'key': 'value'},
        nullable=None,
    )
    res = instance_as_json_dict(item, exclude_none=False)
    expected = {
        'string': 'string',
        'number': 1.1,
        'boolean': True,
        'binary': 'data',
        'binary_set': ['three', 'one', 'two'],
        'unicode_set': ['three', 'one', 'two'],
        'number_set': [1, 2, 3],
        'json': {'key': 'value'},
        'utc_datetime': '2025-01-29T13:41:02.945314Z',
        'null': None,
        'mapping': {'key': [1, 'two', {'key': '2025-01-29T13:41:02.945314Z'}]},
        'nested': {'one': 1, 'two': 2},
        'list': [
            {
                'k1': 'value',
                'k2': 1,
                'k3': 'b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34',
                'k4': None,
            }
        ],
        'enum': 'AWS',
        'uuid': 'b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34',
        'dynamic': {'key': 'value'},
        'nullable': None,
    }
    # NOTE: checking sets separatelly
    for attr in ('binary_set', 'unicode_set', 'number_set'):
        assert sorted(res.pop(attr)) == sorted(expected.pop(attr))
    assert res == expected

    res = instance_as_json_dict(item, exclude_none=True)
    expected = {
        'string': 'string',
        'number': 1.1,
        'boolean': True,
        'binary': 'data',
        'binary_set': ['three', 'one', 'two'],
        'unicode_set': ['three', 'one', 'two'],
        'number_set': [1, 2, 3],
        'json': {'key': 'value'},
        'utc_datetime': '2025-01-29T13:41:02.945314Z',
        'mapping': {'key': [1, 'two', {'key': '2025-01-29T13:41:02.945314Z'}]},
        'nested': {'one': 1, 'two': 2},
        'list': [
            {
                'k1': 'value',
                'k2': 1,
                'k3': 'b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34',
            }
        ],
        'enum': 'AWS',
        'uuid': 'b2e25eb0-e27d-4ecf-89f4-9ddbf3725f34',
        'dynamic': {'key': 'value'},
    }
    # NOTE: checking sets separatelly
    for attr in ('binary_set', 'unicode_set', 'number_set'):
        assert sorted(res.pop(attr)) == sorted(expected.pop(attr))
    assert res == expected
