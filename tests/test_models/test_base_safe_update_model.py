import unittest
from typing import Type, TypeVar
from unittest.mock import MagicMock

from pynamodb.attributes import MapAttribute, UnicodeAttribute, \
    NumberAttribute, BooleanAttribute, ListAttribute
from pynamodb.constants import ITEM

from test_commons.import_helper import ImportFromSourceContext

with ImportFromSourceContext():
    from modular_sdk.models.pynamodb_extension.base_safe_update_model import \
        BaseSafeUpdateModel
    from modular_sdk.commons import DynamoDBJsonSerializer

KEY_NAME = 'k'
STR_NAME = 's'
NUMBER_NAME = 'n'
BOOL_NAME = 'b'
LIST_STR_NAME = 'ls'
LIST_MAP_NAME = 'lm'
LIST_MAP_CUSTOM_NAME = 'lmc'
MAP_NAME = 'm'
MAP_CUSTOM_NAME = 'mc'


class ExampleMapAttrPartial(MapAttribute):
    str_ = UnicodeAttribute(attr_name=STR_NAME)
    list_str = ListAttribute(of=UnicodeAttribute, null=True,
                             attr_name=LIST_STR_NAME)


class ExampleMapAttrFull(MapAttribute):
    str_ = UnicodeAttribute(attr_name=STR_NAME)
    number_ = NumberAttribute(attr_name=NUMBER_NAME)
    bool_ = BooleanAttribute(attr_name=BOOL_NAME)
    list_str = ListAttribute(of=UnicodeAttribute, null=True,
                             attr_name=LIST_STR_NAME)
    map_ = MapAttribute(null=True, attr_name=MAP_NAME)


class TestModel1(BaseSafeUpdateModel):
    class Meta:
        table_name = 'User'
        region = 'us-west-1'

    key = UnicodeAttribute(hash_key=True, attr_name=KEY_NAME)


class TestModel2(BaseSafeUpdateModel):
    class Meta:
        table_name = 'User'
        region = 'us-west-1'

    key = UnicodeAttribute(hash_key=True, attr_name=KEY_NAME)
    str_ = UnicodeAttribute(attr_name=STR_NAME)
    number_ = NumberAttribute(attr_name=NUMBER_NAME)
    bool_ = BooleanAttribute(attr_name=BOOL_NAME)
    map_ = MapAttribute(default=dict, attr_name=MAP_NAME)
    list_str = ListAttribute(of=UnicodeAttribute, default=list,
                             attr_name=LIST_STR_NAME)
    list_map = ListAttribute(of=MapAttribute, default=list,
                             attr_name=LIST_MAP_NAME)


class TestModel3PartialMapCustom(BaseSafeUpdateModel):
    class Meta:
        table_name = 'User'
        region = 'us-west-1'

    key = UnicodeAttribute(hash_key=True, attr_name=KEY_NAME)
    str_ = UnicodeAttribute(attr_name=STR_NAME)
    number_ = NumberAttribute(attr_name=NUMBER_NAME)
    bool_ = BooleanAttribute(attr_name=BOOL_NAME)
    map_ = MapAttribute(default=dict, attr_name=MAP_NAME)
    list_str = ListAttribute(of=UnicodeAttribute, default=list,
                             attr_name=LIST_STR_NAME)
    list_map = ListAttribute(of=MapAttribute, default=list,
                             attr_name=LIST_MAP_NAME)

    map_custom = ExampleMapAttrPartial(default=dict, attr_name=MAP_CUSTOM_NAME)
    list_map_custom = ListAttribute(of=ExampleMapAttrFull, default=list,
                                    attr_name=LIST_MAP_CUSTOM_NAME)


class TestModel4PartialListMapCustom(BaseSafeUpdateModel):
    class Meta:
        table_name = 'User'
        region = 'us-west-1'

    key = UnicodeAttribute(hash_key=True, attr_name=KEY_NAME)
    str_ = UnicodeAttribute(attr_name=STR_NAME)
    number_ = NumberAttribute(attr_name=NUMBER_NAME)
    bool_ = BooleanAttribute(attr_name=BOOL_NAME)
    map_ = MapAttribute(default=dict, attr_name=MAP_NAME)
    list_str = ListAttribute(of=UnicodeAttribute, default=list,
                             attr_name=LIST_STR_NAME)
    list_map = ListAttribute(of=MapAttribute, default=list,
                             attr_name=LIST_MAP_NAME)

    map_custom = ExampleMapAttrFull(default=dict, attr_name=MAP_CUSTOM_NAME)
    list_map_custom = ListAttribute(of=ExampleMapAttrPartial, default=list,
                                    attr_name=LIST_MAP_CUSTOM_NAME)


class TestBaseSafeUpdateModel(unittest.TestCase):
    """
    BaseSafeUpdateModel allows to keep extra attributes for "save" method,
    in case they exist in DB but are not defined in our Model.
    """
    MT = TypeVar('MT', bound=BaseSafeUpdateModel, )

    @staticmethod
    def patched_get_item(model: Type[MT], item: dict
                         ) -> Type[MT]:
        """
        Patches model cls._get_connection().get_item() so that it return
        the given item.
        :param model:
        :param item:
        :return:
        """

        class Mocked(model):
            pass

        Mocked._get_connection = MagicMock()
        Mocked._get_connection().get_item.return_value = {
            ITEM: item
        }
        return Mocked

    @property
    def from_db(self) -> dict:
        """
        Returns an item in DynamoDB json format
        :return:
        """
        return DynamoDBJsonSerializer.serialize_model({
            KEY_NAME: 'value',
            STR_NAME: 'str',
            NUMBER_NAME: 1,
            BOOL_NAME: True,
            LIST_STR_NAME: ['one', 'two', 'three'],
            LIST_MAP_NAME: [{'key': 'value', 'key1': 'value1'}],
            LIST_MAP_CUSTOM_NAME: [
                {
                    STR_NAME: 'str',
                    NUMBER_NAME: 1,
                    BOOL_NAME: True,
                    LIST_STR_NAME: ['one', 'two', 'three'],
                    MAP_NAME: {
                        'key': 'value',
                    }
                },
                {
                    STR_NAME: 'str',
                    NUMBER_NAME: 2,
                    BOOL_NAME: False,
                },
            ],
            MAP_NAME: {'key': 'value'},
            MAP_CUSTOM_NAME: {
                STR_NAME: 'str',
                NUMBER_NAME: 1,
                BOOL_NAME: True,
                LIST_STR_NAME: ['one', 'two', 'three'],
                MAP_NAME: {
                    'key': 'value',
                }
            }
        })

    def test_commons(self):
        raw = self.from_db
        model = self.patched_get_item(TestModel1, raw)
        item = model.get_nullable('mock')

        _should_not_exist = (
            STR_NAME, NUMBER_NAME, BOOL_NAME, LIST_STR_NAME, LIST_MAP_NAME,
            LIST_MAP_CUSTOM_NAME, MAP_NAME
        )
        for attr in _should_not_exist:
            self.assertFalse(hasattr(item, attr))

        self.assertTrue(hasattr(item, model._additional_data_attr_name))
        additional_data = getattr(item, model._additional_data_attr_name)
        self.assertIsInstance(additional_data, dict)

        item.save()
        model._get_connection().put_item.assert_called()

    def test_model1(self):
        raw = self.from_db
        model = self.patched_get_item(TestModel1, raw)
        item = model.get_nullable('mock')

        self.assertEqual(item.key, 'value')

        additional_data = getattr(item, model._additional_data_attr_name)
        expected = DynamoDBJsonSerializer.deserialize_model(raw)
        expected.pop(KEY_NAME)
        self.assertEqual(additional_data, expected)

        item.save()
        keys = model._get_connection().put_item.call_args.args
        saved_data = model._get_connection().put_item.call_args.kwargs[
            'attributes']
        self.assertEqual(keys, (item.key,))
        raw.pop(KEY_NAME)
        self.assertEqual(raw, saved_data)

    def test_model2(self):
        raw = self.from_db
        model = self.patched_get_item(TestModel2, raw)
        item = model.get_nullable('mock')

        self.assertEqual(item.key, 'value')
        self.assertEqual(item.str_, 'str')
        self.assertEqual(item.number_, 1)
        self.assertEqual(item.bool_, True)
        self.assertEqual(item.map_.as_dict(), {'key': 'value'})
        self.assertEqual(item.list_str, ['one', 'two', 'three'])
        self.assertEqual(item.list_map, [{'key': 'value', 'key1': 'value1'}])

        additional_data = getattr(item, model._additional_data_attr_name)
        expected = DynamoDBJsonSerializer.deserialize_model(raw)
        expected.pop(KEY_NAME)
        expected.pop(STR_NAME)
        expected.pop(NUMBER_NAME)
        expected.pop(BOOL_NAME)
        expected.pop(MAP_NAME)
        expected.pop(LIST_STR_NAME)
        expected.pop(LIST_MAP_NAME)
        self.assertEqual(additional_data, expected)

        item.save()
        keys = model._get_connection().put_item.call_args.args
        saved_data = model._get_connection().put_item.call_args.kwargs[
            'attributes']
        self.assertEqual(keys, (item.key,))
        raw.pop(KEY_NAME)
        self.assertEqual(raw, saved_data)

    def test_model3(self):
        raw = self.from_db
        model = self.patched_get_item(TestModel3PartialMapCustom, raw)
        item = model.get_nullable('mock')
        self.assertEqual(item.map_custom.as_dict(),
                         {'str_': 'str', 'list_str': ['one', 'two', 'three']})

        additional_data = getattr(item, model._additional_data_attr_name)
        self.assertEqual(additional_data[MAP_CUSTOM_NAME],
                         {NUMBER_NAME: 1, BOOL_NAME: True,
                          MAP_NAME: {'key': 'value'}})

        item.save()
        keys = model._get_connection().put_item.call_args.args
        saved_data = model._get_connection().put_item.call_args.kwargs[
            'attributes']
        self.assertEqual(keys, (item.key,))
        raw.pop(KEY_NAME)
        self.assertEqual(raw, saved_data)

    def test_model4(self):
        raw = self.from_db
        model = self.patched_get_item(TestModel4PartialListMapCustom, raw)
        item = model.get_nullable('mock')
        self.assertEqual(item.list_map_custom[0].as_dict(),
                         {'list_str': ['one', 'two', 'three'], 'str_': 'str'})
        self.assertEqual(item.list_map_custom[1].as_dict(),
                         {'str_': 'str'})

        additional_data = getattr(item, model._additional_data_attr_name)
        self.assertEqual(additional_data[LIST_MAP_CUSTOM_NAME],
                         [{'n': 1, 'b': True, 'm': {'key': 'value'}},
                          {'n': 2, 'b': False}])
        item.save()
        keys = model._get_connection().put_item.call_args.args
        saved_data = model._get_connection().put_item.call_args.kwargs[
            'attributes']
        self.assertEqual(keys, (item.key,))
        raw.pop(KEY_NAME)
        self.assertEqual(raw, saved_data)

    # todo review:fix
    @unittest.skip('Currently the bug exists. '
                   'In case it is fixes, the test should pass')
    def test_model4_list_change(self):
        """
        Bug: additional data will be corrupted in case we change some items
        in list of custom maps. In case this bus is somehow fixes,
        the test should fail
        :return:
        """
        raw = self.from_db
        model = self.patched_get_item(TestModel4PartialListMapCustom, raw)
        item = model.get_nullable('mock')

        additional_data = getattr(item, model._additional_data_attr_name)
        self.assertEqual(additional_data[LIST_MAP_CUSTOM_NAME],
                         [{'n': 1, 'b': True, 'm': {'key': 'value'}},
                          {'n': 2, 'b': False}])
        item.list_map_custom.insert(0,
                                    ExampleMapAttrPartial(str_='test_string'))
        item.save()
        keys = model._get_connection().put_item.call_args.args
        saved_data = model._get_connection().put_item.call_args.kwargs[
            'attributes']
        self.assertEqual(keys, (item.key,))
        raw.pop(KEY_NAME)

        _raw = DynamoDBJsonSerializer.deserialize_model(raw)
        _raw[LIST_MAP_CUSTOM_NAME].insert(0, {STR_NAME: 'test_string'})
        raw = DynamoDBJsonSerializer.serialize_model(_raw)
        self.assertEqual(raw, saved_data)
