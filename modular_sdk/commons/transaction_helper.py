from typing import Any

from botocore.exceptions import ClientError
from pynamodb.exceptions import (
    DeleteError,
    PutError,
    TransactWriteError as PynamoDBTransactWriteError,
    UpdateError,
)
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    DuplicateKeyError as MongoDuplicateKeyError,
    WTimeoutError,
)

from modular_sdk.commons.exception import (
    ConditionalWriteFailed,
    DuplicateKeyError,
    TransactionError,
    TransientTransactionError,
)


def error_chain(error: BaseException):
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = getattr(current, 'cause', None) or current.__cause__


def error_code(error: BaseException) -> str | None:
    response = getattr(error, 'response', None)
    if isinstance(response, dict):
        return response.get('Error', {}).get('Code')
    details = getattr(error, 'details', None)
    if isinstance(details, dict):
        return details.get('codeName') or details.get('code')
    return None


def cancellation_codes(error: BaseException) -> set[str]:
    response = getattr(error, 'response', None)
    reasons = response.get('CancellationReasons', []) if isinstance(
        response, dict
    ) else []
    return {
        reason.get('Code')
        for reason in reasons
        if isinstance(reason, dict) and reason.get('Code')
    }


def is_transient(error: BaseException) -> bool:
    transient_codes = {
        'TransactionConflict',
        'ProvisionedThroughputExceeded',
        'ThrottlingError',
        'RequestLimitExceeded',
        'InternalServerError',
        'TransactionInProgress',
    }
    for cause in error_chain(error):
        if isinstance(cause, (AutoReconnect, ConnectionFailure, WTimeoutError)):
            return True
        has_error_label = getattr(cause, 'has_error_label', None)
        if callable(has_error_label) and (
            has_error_label('TransientTransactionError')
            or has_error_label('UnknownTransactionCommitResult')
        ):
            return True
        codes = {error_code(cause)} | cancellation_codes(cause)
        if codes & transient_codes:
            return True
    return False


def is_duplicate(error: BaseException) -> bool:
    for cause in error_chain(error):
        codes = {error_code(cause)} | cancellation_codes(cause)
        if isinstance(cause, MongoDuplicateKeyError) or codes & {
            'DuplicateKey',
            'DuplicateItem',
        }:
            return True
    return False


def is_conditional(error: BaseException) -> bool:
    conditional_codes = {
            'ConditionalCheckFailed',
            'ConditionalCheckFailedException',
            'TransactionCanceledException',
    }
    for cause in error_chain(error):
        if isinstance(cause, (PutError, UpdateError, DeleteError)):
            return True
        codes = {error_code(cause)} | cancellation_codes(cause)
        if isinstance(cause, (PynamoDBTransactWriteError, ClientError)):
            if codes & conditional_codes:
                return True
    return False


def translate_error(
    error: BaseException,
    *,
    model: type[Any] | None,
    collection=None,
) -> None:
    if isinstance(error, TransactionError):
        raise error
    if is_transient(error):
        error_type = TransientTransactionError
    elif is_duplicate(error):
        error_type = DuplicateKeyError
    elif is_conditional(error):
        error_type = ConditionalWriteFailed
    else:
        raise error

    message = str(error) or error_type.__name__
    raise error_type(
        message,
        model=model,
        collection=collection,
    ) from error