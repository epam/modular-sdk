import random
from unittest.mock import patch

import pymongo
from modular_sdk.models.pynamongo.indexes_creator import (
    IndexesCreator,
    IndexesExtractor,
    extract_cmp_dict,
    index_information_to_index_models,
    iter_comparing,
)
from pymongo.operations import IndexModel
from pynamodb.attributes import TTLAttribute, UnicodeAttribute
from pynamodb.indexes import (
    AllProjection,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)
from pynamodb.models import Model


class GSI1(GlobalSecondaryIndex):
    class Meta:
        index_name = 'gsi1-custom-name'
        projection = AllProjection()

    attr1 = UnicodeAttribute(hash_key=True)


class GSI2(GlobalSecondaryIndex):
    class Meta:
        projection = AllProjection()

    attr2 = UnicodeAttribute(hash_key=True)
    attr3 = UnicodeAttribute(range_key=True)


class LSI1(LocalSecondaryIndex):
    class Meta:
        projection = AllProjection()

    h = UnicodeAttribute(attr_name='hash', hash_key=True)
    attr1 = UnicodeAttribute(range_key=True)


class MyModel(Model):
    class Meta:
        table_name = 'MyModel'

    h = UnicodeAttribute(attr_name='hash', hash_key=True)
    sort = UnicodeAttribute(range_key=True)
    attr1 = UnicodeAttribute()
    attr2 = UnicodeAttribute()
    attr3 = UnicodeAttribute()
    ttl = TTLAttribute()

    gsi1 = GSI1()
    gsi2 = GSI2()
    lsi1 = LSI1()


class TestIndexesExtractor:
    def test_get_primary_hash_range_names(self):
        res = IndexesExtractor._get_primary_hash_range_names(MyModel)
        assert res == ('hash', 'sort')

    def test_get_index_hash_range_names(self):
        res = IndexesExtractor._get_index_hash_range_names(GSI1)
        assert res == ('attr1', None)

        res = IndexesExtractor._get_index_hash_range_names(GSI2)
        assert res == ('attr2', 'attr3')

        res = IndexesExtractor._get_index_hash_range_names(LSI1)
        assert res == ('hash', 'attr1')

    def test_dynamodb_index_name(self):
        assert IndexesExtractor.dynamodb_index_name(GSI1) == 'gsi1-custom-name'
        assert IndexesExtractor.dynamodb_index_name(GSI2) == 'gsi2'
        assert IndexesExtractor.dynamodb_index_name(LSI1) == 'lsi1'

    def test_get_primary(self):
        assert IndexesExtractor().get_primary(MyModel).document == {
            'name': 'hash_1_sort_-1',
            'unique': True,
            'key': {'hash': 1, 'sort': -1},
        }
        assert IndexesExtractor().get_primary(
            MyModel, unique=False
        ).document == {
            'name': 'hash_1_sort_-1',
            'unique': False,
            'key': {'hash': 1, 'sort': -1},
        }
        assert IndexesExtractor(
            primary_index_name='main', range_key_order=pymongo.ASCENDING
        ).get_primary(MyModel, unique=False).document == {
            'name': 'main',
            'unique': False,
            'key': {'hash': 1, 'sort': 1},
        }

    def test_get_ttl(self):
        item = IndexesExtractor().get_ttl(MyModel)
        assert item.document == {
            'name': 'ttl_1',
            'expireAfterSeconds': 0,
            'key': {'ttl': 1},
        }

    def test_get_iter_indexes(self):
        items = tuple(IndexesExtractor().iter_indexes(MyModel))
        assert len(items) == 3
        by_name = {item.document['name']: item for item in items}
        assert by_name['attr1_1'].document == {
            'name': 'attr1_1',
            'key': {'attr1': 1},
        }
        assert by_name['attr2_1_attr3_-1'].document == {
            'name': 'attr2_1_attr3_-1',
            'key': {'attr2': 1, 'attr3': -1},
        }
        assert by_name['hash_1_attr1_-1'].document == {
            'name': 'hash_1_attr1_-1',
            'key': {'hash': 1, 'attr1': -1},
        }

    def test_get_iter_indexes_dynamodb_name(self):
        items = tuple(
            IndexesExtractor(
                index_name_builder=IndexesExtractor.dynamodb_index_name,
                hash_key_order=pymongo.DESCENDING,
                range_key_order=pymongo.ASCENDING,
            ).iter_indexes(MyModel)
        )
        assert len(items) == 3
        by_name = {item.document['name']: item for item in items}
        assert by_name['gsi1-custom-name'].document == {
            'name': 'gsi1-custom-name',
            'key': {'attr1': -1},
        }
        assert by_name['gsi2'].document == {
            'name': 'gsi2',
            'key': {'attr2': -1, 'attr3': 1},
        }
        assert by_name['lsi1'].document == {
            'name': 'lsi1',
            'key': {'hash': -1, 'attr1': 1},
        }


def test_index_information_to_index_models():
    info = {
        'index1': {'key': [('hash', 1), ('sort', -1)], 'unique': True, 'v': 2},
        'index2': {'key': ['test'], 'v': 2},
        'index3': {'key': [('test', -1)]},
        'ttl': {'key': [('ttl', 1)], 'expireAfterSeconds': 0},
    }
    items = tuple(index_information_to_index_models(info))
    assert len(items) == 4
    by_name = {item.document['name']: item for item in items}
    assert by_name['index1'].document == {
        'key': {'hash': 1, 'sort': -1},
        'unique': True,
        'name': 'index1',
    }
    assert by_name['index2'].document == {'key': {'test': 1}, 'name': 'index2'}
    assert by_name['index3'].document == {
        'key': {'test': -1},
        'name': 'index3',
    }
    assert by_name['ttl'].document == {
        'key': {'ttl': 1},
        'name': 'ttl',
        'expireAfterSeconds': 0,
    }


def test_extract_cmp_dict():
    m = IndexModel(keys=[('hash', 1), ('sort', -1)], name='test', unique=False)
    assert extract_cmp_dict(m) == {'key': {'hash': 1, 'sort': -1}}


def test_iter_comparing():
    needed = (
        IndexModel(keys=[('hash', 1), ('sort', -1)], name='one', unique=True),
        IndexModel(keys='attr', name='two'),
        IndexModel(keys=[('attr', -1)], name='three'),
        IndexModel(keys=[('ttl', 1)], name='ttl', expireAfterSeconds=0),
    )
    existing = (
        IndexModel(keys=[('hash', 1), ('sort', -1)], unique=True),
        IndexModel(keys='attr', name='attr_1'),
        IndexModel(keys=[('attr1', 1)], name='attr1_1'),
    )

    res = tuple(iter_comparing(needed, existing))
    assert res[0][0] is needed[0] and res[0][1] is existing[0]
    assert res[1][0] is needed[1] and res[1][1] is existing[1]

    assert res[2][0] is needed[2] and res[2][1] is None
    assert res[3][0] is needed[3] and res[3][1] is None

    res = tuple(iter_comparing(existing, needed))
    assert res[0][0] is existing[0] and res[0][1] is needed[0]
    assert res[1][0] is existing[1] and res[1][1] is needed[1]

    assert res[2][0] is existing[2] and res[2][1] is None


class TestIndexCreator:
    def test_ensure(self, mongo_database):
        for _ in range(random.randint(1, 5)):
            IndexesCreator(mongo_database).ensure(MyModel)

        indexes = mongo_database.get_collection('MyModel').index_information()
        for name in (
            '_id_',
            'hash_1_sort_-1',
            'attr1_1',
            'attr2_1_attr3_-1',
            'hash_1_attr1_-1',
            'ttl_1',
        ):
            assert name in indexes

        with patch.object(
            MyModel.Meta.mongo_collection, 'create_indexes'
        ) as mock_method:
            IndexesCreator(mongo_database).ensure(MyModel)
            mock_method.assert_not_called()

    def test_sync(self, mongo_database):
        IndexesCreator(mongo_database).sync(MyModel)
        MyModel.Meta.mongo_collection.create_index('testing1', name='testing1')
        MyModel.Meta.mongo_collection.drop_index('hash_1_sort_-1')
        IndexesCreator(mongo_database).sync(MyModel)
        indexes = MyModel.Meta.mongo_collection.index_information()
        for name in (
            '_id_',
            'hash_1_sort_-1',
            'attr1_1',
            'attr2_1_attr3_-1',
            'hash_1_attr1_-1',
            'ttl_1',
        ):
            assert name in indexes
