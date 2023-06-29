from modular_sdk.services.aws_creds_provider import AWSCredentialsProvider


class LambdaService(AWSCredentialsProvider):
    def __init__(self, aws_region, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None):
        super(LambdaService, self).__init__(
            service_name='lambda',
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def get_policy(self, name):
        return self.client.get_policy(FunctionName=name)
