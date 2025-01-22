from __future__ import annotations

from typing import TypeVar, Generic, Callable

T = TypeVar("T")


class staticproperty(Generic[T]):
    def __init__(self, getter: Callable[[], T]) -> None:
        self.__getter = getter

    def __get__(self, obj: object, objtype: type) -> T:
        return self.__getter()

    @staticmethod
    def __call__(getter_fn: Callable[[], T]) -> staticproperty[T]:
        return staticproperty(getter_fn)
