from pynamodb.connection import TableConnection

from modular_sdk.modular import Modular
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.pynamodb_extension.base_safe_update_model import \
    BaseSafeUpdateModel

_LOG = get_logger(__name__)


class BaseRoleAccessModel(BaseSafeUpdateModel):
    """
    Each inherited model will use creds received by assuming a role from
    env variables, and if the creds expire, they will be received again.
    Use custom modular_sdk.models.base_meta.BaseMeta instead of standard Meta in
    the inherited models
    Not highly critical but still - problems:
    - only one role available (the one from envs);
    - if role is set in envs, hard-coded aws keys from Model.Meta/BaseMeta
      will be ignored;
    Take all this into consideration, use BaseRoleAccessModel and BaseMeta
    together.
    """

    @classmethod
    def _get_connection(cls) -> TableConnection:
        _modular = Modular()
        sts = _modular.sts_service()
        if sts.assure_modular_credentials_valid():
            env = _modular.environment_service()
            for model in BaseRoleAccessModel.__subclasses__():
                if model._connection:
                    # works as well but seems too tough
                    # model._connection = None
                    _LOG.warning(
                        f'Existing connection found in {model.__name__}. '
                        f'Updating credentials in botocore session and '
                        f'dropping the existing botocore client...')
                    model._connection.connection.session.set_credentials(
                        env.modular_aws_access_key_id(),
                        env.modular_aws_secret_access_key(),
                        env.modular_aws_session_token()
                    )
                    model._connection.connection._client = None
                else:
                    _LOG.info(
                        f'Existing connection not found in {model.__name__}'
                        f'. Probably the first request. Connection will be '
                        f'created using creds from envs which '
                        f'already have been updated')
        return super()._get_connection()
