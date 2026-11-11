from typing import Any


class ModularException(Exception):

    def __init__(self, code, content):
        self.code = code
        self.content = content

    def __str__(self):
        return f'{self.code}:{self.content}'


class TransactionError(Exception):
    """
    Base class for errors raised by the transaction adapter.
    """

    def __init__(
        self,
        message: str,
        *,
        model: type[Any] | None = None,
        collection=None,
    ):
        super().__init__(message)
        self.model = model
        self.collection = collection
        self.collection_name = (
            getattr(collection, 'name', collection)
            if collection is not None
            else None
        )
        self.model_name = model.__name__ if model is not None else None


class ConditionalWriteFailed(TransactionError):
    """
    Raised when a transaction condition is not satisfied.
    """


class DuplicateKeyError(TransactionError):
    """
    Raised when a transaction attempts to write a duplicate key.
    """


class TransientTransactionError(TransactionError):
    """
    Raised when a transaction fails for a potentially retryable reason.
    """
