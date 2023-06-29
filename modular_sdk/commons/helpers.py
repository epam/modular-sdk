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


def replace_keys_in_dict(dictionary: dict, old_character: str,
                         new_character: str) -> dict:
    new = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            value = replace_keys_in_dict(value, old_character, new_character)
        new[key.replace(old_character, new_character)] = value
    return new
