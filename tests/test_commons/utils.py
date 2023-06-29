import unittest
from functools import cached_property

from test_commons.import_helper import ImportFromSourceContext

with ImportFromSourceContext():
    from modular_sdk.commons import dict_without


class TestUtils(unittest.TestCase):
    """
    BaseSafeUpdateModel allows to keep extra attributes for "save" method,
    in case they exist in DB but are not defined in our Model.
    """

    @cached_property
    def sample(self) -> dict:
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

    def test_dict_without(self):
        dct = self.sample
        self.assertEqual(
            dict_without(dct, {'str': None, 'map': None, 'list': None}),
            {}
        )
        self.assertEqual(
            dict_without(dct, {'str': None}),
            {
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
        )
        self.assertEqual(
            dict_without(dct, {'map': None}),
            {
                'str': 'str',
                'list': [
                    {
                        'str': 'str',
                        'list': ['one', 'two']
                    }
                ]
            }
        )
        self.assertEqual(
            dict_without(dct, {'map': {'str': None}}),
            {
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
        )
        self.assertEqual(
            dict_without(dct, {'map': {'map': {'list': None}},
                               'list': [{'str': None}]}),
            {
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
        )
