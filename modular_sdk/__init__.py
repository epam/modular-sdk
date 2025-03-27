import json
import logging
import os


def _init_envs_from_ssm():
    """
    Supposed to be used only inside AWS Lambda to fetch secret envs
    from a certain SSM Parameter during the cold start
    """
    name = os.getenv('MODULAR_SDK_ENVIRONMENT_SSM_PARAMETER_NAME')
    # additional check whether it's a lambda
    handler = os.getenv('AWS_LAMBDA_FUNCTION_NAME')
    if not (name and handler):
        return

    import boto3

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    try:
        data = boto3.client('ssm').get_parameter(
            Name=name,
            WithDecryption=True
        )['Parameter']['Value']
    except Exception as e:  # noqa
        log.warning(f'Could not get SSM Parameter {name}: {e}')
        return
    try:
        loaded = json.loads(data)
    except json.JSONDecodeError:
        log.warning(f'Could not decode json inside SSM Parameter {name}')
        return
    if not isinstance(loaded, dict):
        log.warning(f'SSM parameter {name} must contain a dict')
        return
    os.environ.update({k: str(v) for k, v in loaded.items()})
    log.info(f'Envs from SSM Parameter {name} were loaded')


_init_envs_from_ssm()

del _init_envs_from_ssm
