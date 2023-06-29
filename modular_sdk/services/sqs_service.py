import json

from botocore.exceptions import ParamValidationError, ClientError
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.services.aws_creds_provider import AWSCredentialsProvider
from modular_sdk.services.environment_service import EnvironmentService


_LOG = get_logger('modular_sdk-sqs-service')


class SQSService(AWSCredentialsProvider):
    def __init__(self, aws_region, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None,
                 environment_service: EnvironmentService = None):
        super(SQSService, self).__init__(
            service_name='sqs',
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        self.environment_service = environment_service

    def send_message(self, message: dict) -> bool:
        queue = self.environment_service.queue_url()
        if not queue:
            _LOG.warning('SQS Queue is not set to envs. Message won`t be set')
            return False
        try:
            self.client.send_message(
                QueueUrl=queue,
                MessageBody=json.dumps(message)
            )
            return True
        except (ClientError, ParamValidationError) as e:
            _LOG.warning(
                f'Cannot push message to the SQS \'{queue}\' queue: {e}'
            )
            return False
