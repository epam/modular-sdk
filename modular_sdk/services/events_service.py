from modular_sdk.services.aws_creds_provider import AWSCredentialsProvider


class EventsService(AWSCredentialsProvider):
    def __init__(self, aws_region, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None):
        super(EventsService, self).__init__(
            service_name='events',
            aws_region=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def describe_rule(self, name):
        return self.client.describe_rule(Name=name)
