from typing import Any


class classproperty:
    """
    Decorator that converts a method with a single cls argument into a property
    that can be accessed directly from the class.
    Cannot be used to create a writable property.
    """

    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


def model_collection(model: type[Any] | None, adapter=None):
    if model is None:
        return None
    if adapter is not None:
        return adapter.get_collection(model)
    return getattr(getattr(model, 'Meta', None), 'table_name', None)
