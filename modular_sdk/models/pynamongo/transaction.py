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
    ):
        self._adapter = adapter
        self._session = session

    def save(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> dict:
        return self._adapter.save(
            instance,
            condition=condition,
            session=self._session,
        )

    def delete(
        self,
        instance: Model,
        condition: Condition | None = None,
    ) -> dict:
        return self._adapter.delete(
            instance,
            condition=condition,
            session=self._session,
        )

    def update(
        self,
        instance: Model,
        actions: list[Action],
        condition: Condition | None = None,
    ) -> dict:
        return self._adapter.update(
            instance,
            actions=actions,
            condition=condition,
            session=self._session,
        )

    def check(self, instance: Model, condition: Condition) -> None:
        self._adapter.check(
            instance,
            condition=condition,
            session=self._session,
        )


@contextmanager
def mongo_transaction(
    adapter: 'PynamoDBToPymongoAdapter',
) -> Iterator[MongoTransactionSession]:
    """
    Create and commit a MongoDB transaction on successful exit
    """
    client = adapter.mongo_database.client
    with client.start_session() as session:
        with session.start_transaction():
            yield MongoTransactionSession(adapter, session)


class TransactionAdapter:
    """Backend-neutral transaction adapter."""

    def __init__(self, model: type[Model]):
        self._model = model

    def transaction(self):
        is_mongo_model = getattr(self._model, 'is_mongo_model', None)
        if callable(is_mongo_model) and is_mongo_model():
            return mongo_transaction(self._model.mongo_adapter())
        return dynamodb_transaction(self._model)
