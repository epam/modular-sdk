import pytest

from modular_sdk.commons import dict_without


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
