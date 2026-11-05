from copy import deepcopy
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator

from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.models import Model
from pynamodb.transactions import TransactWrite

if TYPE_CHECKING:
    from pymongo.client_session import ClientSession

    from modular_sdk.models.pynamongo.adapter import (
        PynamoDBToPymongoAdapter,
    )


class DynamoDBTransactionSession:
    """Operations that can be added to a DynamoDB write transaction."""

    def __init__(self, transaction: TransactWrite):
        self._transaction = transaction

    def save(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> None:
        self._transaction.save(instance, condition=condition)

    def delete(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> None:
        self._transaction.delete(instance, condition=condition)

    def update(
        self,
        instance: Model,
        actions: list[Action],
        condition: Condition | None = None,
    ) -> None:
        self._transaction.update(
            instance,
            actions=actions,
            condition=condition,
        )

    def check(self, instance: Model, condition: Condition) -> None:
        model = type(instance)
        hash_key = getattr(instance, model._hash_keyname)
        range_key = (
            getattr(instance, model._range_keyname)
            if model._range_keyname
            else None
        )
        self._transaction.condition_check(
            model,
            hash_key,
            range_key=range_key,
            condition=condition,
        )


@contextmanager
def dynamodb_transaction(
    model: type[Model],
) -> Iterator[DynamoDBTransactionSession]:
    """Create and commit a DynamoDB transaction on successful exit."""
    connection = model._get_connection().connection
    with TransactWrite(connection=connection) as transaction:
        yield DynamoDBTransactionSession(transaction)


class MongoTransactionSession:
    """Operations that execute inside one MongoDB transaction."""

    def __init__(
        self,
        adapter: 'PynamoDBToPymongoAdapter',
        session: 'ClientSession',
        client
    ):
        self._adapter = adapter
        self._session = session
        self._client = client
        self._snapshots: dict[int, tuple[Model, dict]] = {}

    def _adapter_for(
        self,
        instance: Model,
    ) -> 'PynamoDBToPymongoAdapter':
        model_adapter = getattr(type(instance), 'mongo_adapter', None)
        adapter = model_adapter() if callable(model_adapter) else self._adapter
        collection = adapter.get_collection(instance)
        if collection.database.client is not self._client:
            raise ValueError(
                'All models in a MongoDB transaction must use the same '
                'MongoClient'
            )
        return adapter

    def _snapshot(self, instance: Model) -> None:
        if id(instance) not in self._snapshots:
            self._snapshots[id(instance)] = (
                instance,
                deepcopy(instance.__dict__),
            )

    def restore_local_state(self) -> None:
        """
        Restore model instances changed before a transaction rollback.
        """
        for instance, state in self._snapshots.values():
            instance.__dict__.clear()
            instance.__dict__.update(state)

    def save(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> None:
        self._snapshot(instance)
        self._adapter_for(instance).save(
            instance,
            condition=condition,
            session=self._session,
        )

    def delete(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> None:
        self._snapshot(instance)
        self._adapter_for(instance).delete(
            instance,
            condition=condition,
            session=self._session,
        )

    def update(
        self,
        instance: Model,
        actions: list[Action],
        condition: Condition | None = None,
    ) -> None:
        self._snapshot(instance)
        self._adapter_for(instance).update(
            instance,
            actions=actions,
            condition=condition,
            session=self._session,
        )

    def check(self, instance: Model, condition: Condition) -> None:
        self._adapter_for(instance).check(
            instance,
            condition=condition,
            session=self._session,
        )


@contextmanager
def mongo_transaction(
    adapter: 'PynamoDBToPymongoAdapter',
    model: type[Model] | None = None,
) -> Iterator[MongoTransactionSession]:
    """
    Create and commit a MongoDB transaction on successful exit
    """
    database = (
        adapter.get_collection(model).database
        if model is not None
        else adapter.mongo_database
    )
    assert database is not None, (
        'Mongo database must be set either on the model Meta '
        'or on PynamoDBToPymongoAdapter'
    )
    client = database.client
    with client.start_session() as session:
        with session.start_transaction():
            transaction_session = MongoTransactionSession(
                adapter,
                session,
                client
            )
            try:
                yield transaction_session
            except BaseException:
                transaction_session.restore_local_state()
                raise


class TransactionAdapter:
    """Backend-neutral transaction adapter."""

    def __init__(self, model: type[Model]):
        self._model = model

    def transaction(self):
        is_mongo_model = getattr(self._model, 'is_mongo_model', None)
        if callable(is_mongo_model) and is_mongo_model():
            return mongo_transaction(
                self._model.mongo_adapter(),
                model=self._model,
            )
        return dynamodb_transaction(self._model)
