import base64
import json
import gzip

import pytest

from modular_sdk.commons import dict_without, build_payload, build_message, build_secure_message


@pytest.fixture
def dict_sample() -> dict:
    return {
        'str': 'str',
        'map': {
            'str': 'str',
            'map': {
                'str': 'str',
                'list': ['one', 'two']
            }
        },
        'list': [
            {
                'str': 'str',
                'list': ['one', 'two']
            }
        ]
    }


def test_dict_without(dict_sample):
    dct = dict_sample
    assert dict_without(dct, {'str': None, 'map': None, 'list': None}) == {}
    assert dict_without(dct, {'str': None}) == {
        'map': {
            'str': 'str',
            'map': {
                'str': 'str',
                'list': ['one', 'two']
            }
        },
        'list': [
            {
                'str': 'str',
                'list': ['one', 'two']
            }
        ]
    }
    assert dict_without(dct, {'map': None}) == {
        'str': 'str',
        'list': [
            {
                'str': 'str',
                'list': ['one', 'two']
            }
        ]
    }
    assert dict_without(dct, {'map': {'str': None}}) == {
        'str': 'str',
        'map': {
            'map': {
                'str': 'str',
                'list': ['one', 'two']
            }
        },
        'list': [
            {
                'str': 'str',
                'list': ['one', 'two']
            }
        ]
    }
    assert dict_without(dct, {'map': {'map': {'list': None}}, 'list': [{'str': None}]}) == {
        'str': 'str',
        'map': {
            'str': 'str',
            'map': {
                'str': 'str',
            }
        },
        'list': [
            {
                'list': ['one', 'two']
            },
        ]
    }


def test_build_payload():
    assert build_payload('id', 'name', {'key': 'value'}, False) == [
        {'id': 'id', 'type': 'name', 'params': {'key': 'value'}}
    ]
    assert build_payload('id', 'name', {'key': 'value'}, True) == [
        {'id': 'id', 'type': None, 'params': {'key': 'value', 'type': 'name'}}
    ]


def test_build_message():
    assert build_message('id', 'name', [{'key1': 'value1'}, {'key2': 'value2'}]) == [{'id': 'id', 'type': 'name', 'params': {'key1': 'value1'}}, {'id': 'id', 'type': 'name', 'params': {'key2': 'value2'}}]
    assert build_message('id', 'name', {'key1': 'value1'}) == [{'id': 'id', 'type': 'name', 'params': {'key1': 'value1'}}]

    assert build_message('id', 'name', {'key1': 'value1'}, True) == [{'id': 'id', 'type': None, 'params': {'key1': 'value1', 'type': 'name'}}]

    res = build_message('id', 'name', {'key1': 'value1'}, True, True)
    assert json.loads(gzip.decompress(base64.b64decode(res))) == [{'id': 'id', 'type': None, 'params': {'key1': 'value1', 'type': 'name'}}]


def test_build_secure_message():
    # weird thing
    assert build_secure_message('id', 'name', {'key': 'value', 'key1': 'value1'}, ['key'], True) == [{'id': 'id', 'type': None, 'params': {'key': '*****', 'key1': 'value1', 'type': 'name'}}]