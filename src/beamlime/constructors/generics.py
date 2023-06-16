from __future__ import annotations

from typing import Any, Generic, Tuple, TypeVar, Union

_RawType = TypeVar('_RawType')

class UserConfig(Generic[_RawType]):
    ...

_DependencyType = TypeVar("_DependencyType")
_SubDependencyType = TypeVar("_SubDependencyType")


def wrap_union(obj_type: type, *obj_types: type):
    """
    Return an union of multiple types.

    This is a work-around to avoid syntax checking error on IDE "
    "for having only one argument for ``Union.__getitem__``.
    It can be removed from minimum support of py311,
    then unpacking is possible for ``__getitem__``.
    """

    def _wrap(
        _obj_types: Tuple[type, ...] = obj_types, _wrapped=Union[obj_type, obj_type]
    ):
        if not _obj_types:
            return _wrapped
        return _wrap(_obj_types[:-1], Union[_obj_types[-1], _wrapped])

    return _wrap()


class _Holder(Generic[_DependencyType, _SubDependencyType]):
    __supertype__: Any
    __origin__: Any

    def __new__(cls, x: _DependencyType):
        return x
    def __call__(self,x) -> Any:
        return x


class Holder(Generic[_DependencyType, _SubDependencyType]):
    """

    class UserClass:
        member1: Holder[int]
        member2: Holder[float]

    class Provider:
        def get_member1(self) -> Holder[int]:
            return Holder[int](1)

        def get_member2(self) -> Holder[float]:
            return Holder[float](0.01)

    """
    def __new__(cls, x):
        return x

    def __class_getitem__(cls, tp, stp=None):
        from typing import get_origin

        if get_origin(tp) == cls:
            return tp
        elif get_origin(tp) == Union:
            raise TypeError(
                "Union of types can not be placed in the single Holder. "
                "Please use ``UnionHolder`` instead."
            )
        elif isinstance(tp, Tuple):
            _tp, _stp = tp
        else:
            _tp, _stp = tp, stp
        new_holder = _Holder[_tp, _stp]
        new_holder.__origin__ = _tp
        new_holder.__supertype__ = _tp
        return new_holder

class UnionHolder(Generic[_DependencyType]):
    @classmethod
    def __class_getitem__(cls, params):
        if not isinstance(params, tuple):
            return Holder[params]
        return wrap_union(*tuple(Holder[param] for param in params))


def isdependency(tp) -> bool:
    from typing import get_origin

    return get_origin(tp) == Holder
