from typing import Type, TypeVar

T = TypeVar("T")


class IoC:
    def __init__(self):
        self._values = {}

    def set(self, key: Type[T], value: T):
        if not isinstance(value, key):
            raise TypeError(f"Value must be an instance of {key}, got {type(value)}")
        self._values[key] = value

    def get(self, key: Type[T]) -> T:
        if key not in self._values:
            raise AttributeError(f"Key {key} is not registered.")
        attribute = self._values[key]
        return attribute


ioc = IoC()
