from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Iterable,
    MutableMapping,
    Sequence,
    cast,
)

import pymongo
from pymongo.errors import OperationFailure
from pymongo.operations import IndexModel

from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.pynamongo.models import PynamoDBToPymongoAdapter

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.database import Database  # noqa
    from pynamodb.indexes import Index
    from pynamodb.models import Model

_LOG = get_logger(__name__)


class IndexesExtractor:
    """
    Holds logic how to extract Pymongo IndexModels from PynamoDB models
    """

    __slots__ = '_pin', '_gen_name', '_ho', '_ro'

    @staticmethod
    def _iter_model_indexes(
        model: type['Model'],
    ) -> Generator['Index', None, None]:
        """
        Iterates over defined PynamoDB indexes, both local and global
        """
        yield from model._indexes.values()

    @staticmethod
    def _get_primary_hash_range_names(
        model: type['Model'],
    ) -> tuple[str, str | None]:
        h, r = None, None
        for attr in model.get_attributes().values():
            if attr.is_hash_key:
                h = attr.attr_name
            if attr.is_range_key:
                r = attr.attr_name
        return cast(str, h), r

    @staticmethod
    def _get_index_hash_range_names(index: 'Index') -> tuple[str, str | None]:
        h, r = None, None
        for attr in index.Meta.attributes.values():
            if attr.is_hash_key:
                h = attr.attr_name
            if attr.is_range_key:
                r = attr.attr_name
        return cast(str, h), r

    @staticmethod
    def default_mongo_name(index: 'Index') -> str | None:
        """
        Resolves index name from Pynamodb index instance.
        None means that the default Mongo name will be generated
        """
        return None

    @staticmethod
    def dynamodb_index_name(index: 'Index') -> str | None:
        return index.Meta.index_name

    def __init__(
        self,
        primary_index_name: str | None = None,
        index_name_builder: Callable[
            ['Index'], str | None
        ] = default_mongo_name,
        hash_key_order: int = pymongo.ASCENDING,
        range_key_order: int = pymongo.DESCENDING,
    ):
        """
        :param primary_index_name: name for set for the primary DynamoDB
        index. It will be autogenerated by Pymongo if not specified
        :param index_name_builder: function that accepts PynamoDB's index
        instance as one parameter and generated a name for it. The default
        function always returns None of Pymongo will use its autogenerated
        names by default
        :param hash_key_order: order for hash keys.
        :param range_key_order: order for range keys. Descending by default
        seems reasonable because we tend to have DynamoDB models where
        sort key is some kind of date and we need to retrieve latest items
        """
        self._pin = primary_index_name
        self._gen_name = index_name_builder
        self._ho = hash_key_order
        self._ro = range_key_order

    def get_primary(
        self, model: type['Model'], /, *, unique: bool = True
    ) -> IndexModel:
        h, r = self._get_primary_hash_range_names(model)
        keys = [(h, self._ho)]
        if r is not None:
            keys.append((r, self._ro))
        return IndexModel(keys=keys, name=self._pin, unique=unique)

    def get_ttl(self, model: type['Model'], /) -> IndexModel | None:
        # TODO: convert model's ttl attribute if found
        return

    def iter_indexes(
        self, model: type['Model'], /
    ) -> Generator[IndexModel, None, None]:
        for index in self._iter_model_indexes(model):
            h, r = self._get_index_hash_range_names(index)
            keys = [(h, self._ho)]
            if r is not None:
                keys.append((r, self._ro))
            yield IndexModel(keys=keys, name=self._gen_name(index))

    def iter_all_indexes(
        self, model: type['Model']
    ) -> Generator[IndexModel, None, None]:
        if primary := self.get_primary(model):
            yield primary
        if ttl := self.get_ttl(model):
            yield ttl
        yield from self.iter_indexes(model)


def index_information_to_index_models(
    info: MutableMapping[str, Any],
) -> Generator[IndexModel, None, None]:
    """
    Converts the result of Collection.index_information() to an iterator of IndexModel items
    """
    _additional = ('unique',)  # add ttl?
    for name, data in info.items():
        yield IndexModel(
            keys=data['key'],
            name=name,
            **{k: v for k, v in data.items() if k in _additional},
        )


def iter_comparing(
    needed: Iterable[IndexModel], existing: Sequence[IndexModel]
) -> Generator[tuple[IndexModel, IndexModel | None], None, None]:
    """
    Yields each item from "needed" and a corresponding item from existing if
    found or None. Compares indexes based on their attributes
    (not based on names)
    """
    _compare_keys = ('key', 'unique')

    _existing = tuple(
        {k: v for k, v in item.document.items() if k in _compare_keys}
        for item in existing
    )

    for item in needed:
        to_cmp = {k: v for k, v in item.document.items() if k in _compare_keys}
        try:
            i = _existing.index(to_cmp)
            yield item, existing[i]
        except ValueError:
            yield item, None


def ensure_indexes(
    indexes: Iterable[IndexModel], collection: 'Collection', **kwargs: Any
) -> None:
    """
    Makes sure that the given indexes exists. It will not remove any existing indexes. Also, it will NOT
    recreate an index if the similar one already exists but with a different name. Some notes about Mongo:
    - pymongo does not create an indexes if another exactly the same one already exists whether you specify an
      index name or not
    - pymongo does not raise any exception when you try to create an index if another exactly the same one already
      exists unless you specify a different name
    - Unique Compound Mongo index works as you'd expect: the combination of fields must be unique
    - TODO: check if there is projections for indexes
    """
    to_create = []
    for index, existing in iter_comparing(
        indexes,
        tuple(
            index_information_to_index_models(collection.index_information())
        ),
    ):
        if existing is not None:
            _LOG.warning(
                f'Index: "{index.document["name"]}" won`t be created '
                f'because there is another one called '
                f'"{existing.document["name"]}" with the '
                f'same attributes'
            )
            continue
        to_create.append(index)
    if not to_create:
        _LOG.info('No indexes need to be created')
        return

    _LOG.info(f'Going to create indexes: {", ".join(i.document["name"] for i in to_create)}')
    try:
        collection.create_indexes(to_create, **kwargs)
    except OperationFailure:
        _LOG.exception('Failed to create any indexes for Mongo')


class IndexesCreator:
    """
    Just helpers class to put all the function above together
    """

    __slots__ = ('_adapter',)

    def __init__(self, db: 'Database | None' = None):
        """
        :param db: default Database object to use if it cannot be resolved
        from models
        :param ignore_indexes: tuple of indexes names to
        """
        self._adapter = PynamoDBToPymongoAdapter(db)

    def ensure(
        self, model: type['Model'], /, *, table_name: str | None = None
    ) -> None:
        """
        Creates missing indexes, but does not touch ones we do not know about
        """
        if table_name:
            collection = self._adapter.get_database(model).get_collection(
                table_name
            )
        else:
            collection = self._adapter.get_collection(model)

        ensure_indexes(
            indexes=IndexesExtractor().iter_all_indexes(model),
            collection=collection,
        )

    def sync(
        self,
        model: type['Model'],
        /,
        *,
        table_name: str | None = None,
        always_keep: tuple[str, ...] = ('_id_',),
    ) -> None:
        """
        Makes sure that existing mongo indexes correspond to those defined in code using PynamoDB models meaning
        that it will drop indexes what is does not know about (excluding '_id_')
        """
        if table_name:
            collection = self._adapter.get_database(model).get_collection(
                table_name
            )
        else:
            collection = self._adapter.get_collection(model)

        needed = tuple(IndexesExtractor().iter_all_indexes(model))
        info = collection.index_information()
        for name in always_keep:
            info.pop(name, None)
        existing = tuple(index_information_to_index_models(info))

        to_create = []
        to_drop = []
        for defined, created in iter_comparing(needed, existing):
            if created is not None:
                _LOG.warning(
                    f'Index: "{defined.document["name"]}" won`t be created '
                    f'because there is another one called '
                    f'"{created.document["name"]}" with the '
                    f'same attributes'
                )
                continue
            to_create.append(defined)
        for created, defined in iter_comparing(existing, needed):
            if defined is None:
                _LOG.warning(
                    f'Index "{created.document["name"]}" will be '
                    'dropped because it is not defined in code'
                )
                to_drop.append(created)

        for index in to_drop:
            name = index.document['name']
            _LOG.info(f'Going to drop index: {name}')
            collection.drop_index(name)

        if not to_create:
            _LOG.info('No indexes need to be created')
            return

        _LOG.info(f'Going to create indexes: {", ".join(i.document["name"] for i in to_create)}')
        try:
            collection.create_indexes(to_create)
        except OperationFailure:
            _LOG.exception('Failed to create any indexes for Mongo')
