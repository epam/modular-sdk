from typing import Dict, List, Optional

from pynamodb import models
from pynamodb.attributes import Attribute, MapAttribute, ListAttribute

from modular_sdk.commons import DynamoDBJsonSerializer
from modular_sdk.commons.log_helper import get_logger
from modular_sdk.models.pynamodb_extension.base_model import BaseModel

NULL = 'NULL'

_LOG = get_logger(__name__)


class BaseSafeUpdateModel(BaseModel):
    """
    Allows not to override existing attributes that are not specified
    in Models on item update.
    """
    _additional_data_attr_name = '_additional_data'

    @classmethod
    def _retrieve_additional_data(cls, document: dict,
                                  attributes: Dict[str, Attribute]) -> dict:
        """
        Additional data represents those attributes that are not defined
        in Python model, but the do exist in DB. It includes all the nested
        mappings and lists of mappings
        :param document: raw data from DynamoDB
        :param attributes: result of instance.get_attributes()
        :return:
        {
            "not_defined_attr": "value",
            "partly_defined_mapping: {
                "not_defined_attr": "value"
            },
            "partly_defined_list": [
                {},
                {},
                {
                    "not_defined_attr": "value"
                }
            ]
        }
        """
        name_to_instance = {
            attr.attr_name: attr for attr in attributes.values()
        }
        additional_data = {}
        for key, value in document.items():
            if key not in name_to_instance:  # not defined in model
                additional_data[key] = value
                continue
            # key in value
            attr = name_to_instance[key]
            if isinstance(attr, MapAttribute) and type(attr) != MapAttribute:
                additional_data[key] = cls._retrieve_additional_data(
                    value or {}, attr.get_attributes()
                )
            elif isinstance(attr, ListAttribute) and \
                    attr.element_type and \
                    issubclass(attr.element_type, MapAttribute) and \
                    attr.element_type != MapAttribute:
                inner_attributes = attr.element_type.get_attributes()
                additional_data[key] = [
                    cls._retrieve_additional_data(v or {}, inner_attributes)
                    for v in value
                ]
            # else:
            #     pass
        return additional_data

    @classmethod
    def _update_with_additional_data(cls, document: dict,
                                     additional_data: dict):
        """
        Kind of deep update
        :param document:
        :param additional_data:
        :return:
        """
        for key, value in additional_data.items():
            if key not in document:  # not defined
                document[key] = value
                continue
            # deep update
            doc = document[key]
            if type(doc) != type(value):
                _LOG.warning(
                    'Somehow the type of existing model declaration '
                    f'does not correspond to the type of additional '
                    f'value: {doc} - {value}'
                )
                continue
            if isinstance(doc, dict):
                cls._update_with_additional_data(doc, value)
            elif isinstance(doc, list):  # list of dicts
                # here there is a problem. We keep nested additional
                # data for lists by its order. But nothing prevents us from
                # changing the number of items in the list or clearing it,
                # for example. Currently, it will work correctly in case the
                # items of the list are not impaired
                for i, dct in enumerate(doc):
                    _data = value[i] if len(value) > i else None
                    if _data:
                        cls._update_with_additional_data(dct, _data)

    @classmethod
    def _instantiate(cls, attribute_values):
        instance = super()._instantiate(attribute_values)

        additional_data = cls._retrieve_additional_data(
            DynamoDBJsonSerializer.deserialize_model(attribute_values),
            instance.get_attributes()
        )
        setattr(instance, cls._additional_data_attr_name, additional_data)
        return instance

    def _get_save_args(self, null_check: bool = True, condition=None):
        """
        Gets the proper *args, **kwargs for saving and retrieving this object
        :param null_check: If True, then attributes are checked for null
        :param condition: If set, a condition
        """
        attribute_values = self.serialize(null_check)
        # ---- our code below ----
        dct = DynamoDBJsonSerializer.deserialize_model(attribute_values)
        self._update_with_additional_data(
            document=dct,
            additional_data=getattr(self, self._additional_data_attr_name, {})
        )
        attribute_values = DynamoDBJsonSerializer.serialize_model(dct)
        # ---- our code above ----
        hash_key_attribute = self._hash_key_attribute()
        hash_key = attribute_values.pop(hash_key_attribute.attr_name, {}).get(
            hash_key_attribute.attr_type)
        range_key = None
        range_key_attribute = self._range_key_attribute()
        if range_key_attribute:
            range_key = attribute_values.pop(range_key_attribute.attr_name,
                                             {}).get(
                range_key_attribute.attr_type)
        args = (hash_key,)
        kwargs = {}
        if range_key is not None:
            kwargs['range_key'] = range_key
        version_condition = self._handle_version_attribute(
            attributes=attribute_values)
        if version_condition is not None:
            condition &= version_condition
        kwargs['attributes'] = attribute_values
        kwargs['condition'] = condition
        return args, kwargs

    def dynamodb_model(self):
        """For MongoDB"""
        result = super().dynamodb_model()
        self._update_with_additional_data(
            document=result,
            additional_data=getattr(self, self._additional_data_attr_name, {})
        )
        return result

    @classmethod
    def from_json(cls, model_json: dict,
                  attributes_to_get: Optional[List] = None, instance=None
                  ) -> Optional[models.Model]:
        """
        For MongoDB
        TODO use attributes_to_get as projection expression
        :param model_json:
        :param attributes_to_get:
        :param instance:
        :return:
        """
        if not model_json:
            return
        _additional_data = \
            cls._retrieve_additional_data(model_json, cls.get_attributes())
        _additional_data.pop('_id', None)
        instance = super().from_json(model_json, attributes_to_get, instance)
        setattr(instance, cls._additional_data_attr_name, _additional_data)
        return instance
