import json
from abc import abstractmethod, ABC

from modular_sdk.commons.error_helper import RESPONSE_OK_CODE, ERROR_MESSAGE_MAP


class AbstractResponseHelper(ABC):
    exception = None

    def build_response_item(self, code, content):
        return {
            'statusCode': code,
            'headers': {
                'Content-Type': 'application/json',
                'Code': code
            },
            'isBase64Encoded': False,
            'body': json.dumps(content)
        }

    @staticmethod
    @abstractmethod
    def _prepare_body(code, content):
        pass

    @abstractmethod
    def build_response(self, content,
                       code=RESPONSE_OK_CODE):
        pass

    def raise_error_response(self, code, content):
        if not self.exception:
            raise AssertionError('Expected "exception" variable to be defined')
        detailed_error = f'{ERROR_MESSAGE_MAP[code]}. {content}'
        raise self.exception(code, detailed_error)
