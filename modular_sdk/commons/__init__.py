import copy
import dataclasses
import warnings
from functools import partial
from uuid import uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from modular_sdk.commons.exception import ModularException

RESPONSE_BAD_REQUEST_CODE = 400
RESPONSE_UNAUTHORIZED = 401
RESPONSE_FORBIDDEN_CODE = 403
RESPONSE_RESOURCE_NOT_FOUND_CODE = 404
RESPONSE_CONFLICT_CODE = 409
RESPONSE_OK_CODE = 200
RESPONSE_INTERNAL_SERVER_ERROR = 500
RESPONSE_NOT_IMPLEMENTED = 501
RESPONSE_SERVICE_UNAVAILABLE_CODE = 503


def deprecated(message):
    def deprecated_decorator(func):
        def deprecated_func(*args, **kwargs):
            warnings.warn(
                "{} is a deprecated function. {}".format(func.__name__,
                                                         message),
                category=DeprecationWarning,
                stacklevel=2)
            warnings.simplefilter('default', DeprecationWarning)
            return func(*args, **kwargs)

        return deprecated_func

    return deprecated_decorator


@deprecated('not a part of the lib')
def build_response(content, code=200):
    if code == RESPONSE_OK_CODE:
        if isinstance(content, str):
            return {
                'code': code,
                'body': {
                    'message': content
                }
            }
        elif isinstance(content, dict):
            return {
                'code': code,
                'body': {
                    'items': [content]
                }
            }
        return {
            'code': code,
            'body': {
                'items': content
            }
        }
    raise ModularException(
        code=code,
        content=content
    )


def get_missing_parameters(event, required_params_list):
    missing_params_list = []
    for param in required_params_list:
        if event.get(param) is None:
            missing_params_list.append(param)
    return missing_params_list


def validate_params(event, required_params_list):
    """
    Checks if all required parameters present in lambda payload.
    :param event: the lambda payload
    :param required_params_list: list of the lambda required parameters
    :return: bad request response if some parameter[s] is/are missing,
        otherwise - none
    """
    missing_params_list = get_missing_parameters(event, required_params_list)

    if missing_params_list:
        raise ValueError('The following parameters '
                         'are missing: {0}'.format(missing_params_list))


def generate_id():
    return str(uuid4())


def default_instance(value, _type: type, *args, **kwargs):
    return value if isinstance(value, _type) else _type(*args, **kwargs)


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class DynamoDBJsonSerializer(SingletonMeta):
    serializer = TypeSerializer()
    deserializer = TypeDeserializer()

    @classmethod
    def serialize_model(cls, model: dict) -> dict:
        return {
            k: cls.serializer.serialize(v)
            for k, v in model.items()
        }

    @classmethod
    def deserialize_model(cls, model: dict) -> dict:
        return {
            k: cls.deserializer.deserialize(v)
            for k, v in model.items()
        }


def deep_pop(dct: dict, to_pop: dict) -> None:
    for key, _to_pop in to_pop.items():
        if not isinstance(_to_pop, (dict, list)):
            dct.pop(key, None)
            continue
        # isinstance(_to_pop, (dict, list))
        _dct = dct.get(key)
        if type(_dct) != type(_to_pop):
            continue
        if isinstance(_to_pop, dict):  # going deeper
            deep_pop(_dct, _to_pop)
        else:  # isinstance(_to_pop, list)
            for i, d in enumerate(_dct):
                p = _to_pop[i] if len(to_pop) > i else None
                if p:
                    deep_pop(d, p)


def dict_without(dct: dict, without: dict) -> dict:
    cp = copy.deepcopy(dct)
    deep_pop(cp, without)
    return cp


class DataclassBase:
    """
    Provides some useful methods for dataclass instances.
    Ignore warnings below
    """

    @staticmethod
    def _factory(x: dict, exclude: set = None) -> dict:
        dct = {k: v for k, v in x if v is not None}
        if exclude:
            [dct.pop(key, None) for key in exclude]
        return dct

    def dict(self, exclude: set = None) -> dict:
        return dataclasses.asdict(
            self, dict_factory=partial(self._factory, exclude=exclude)
        )

    @classmethod
    def from_dict(cls, dct: dict):
        """
        Ignoring extra kwargs
        :param dct:
        :return:
        """
        return cls(**{
            k.name: dct.get(k.name) for k in dataclasses.fields(cls)
        })
