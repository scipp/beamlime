from __future__ import annotations

from functools import partial
from typing import Callable, Dict, List, Literal, Optional, Tuple, Type, TypeVar, Union

from .inspectors import ProductType, UnknownType
from .providers import Constructor, Provider, UnknownProvider


class ProviderNotFoundError(Exception):
    ...


class ProviderExistsError(Exception):
    ...


class MismatchingProductTypeError(Exception):
    ...


_lambda_name = (lambda: None).__qualname__


def _validate_callable_as_provider(callable_obj: Constructor) -> Literal[True]:
    if (
        not isinstance(callable_obj, partial)
        and hasattr(callable_obj, "__name__")
        and not (callable_obj.__name__ == _lambda_name)
        and hasattr(callable_obj, "__qualname__")
        and callable_obj.__qualname__ != callable_obj.__name__
    ):
        raise NotImplementedError(
            "A member method of a class can not be " "registered as a provider yet."
        )
    return True


def _is_constructor_same(func_left: Callable, func_right: Callable) -> bool:
    """
    Returns true if the two function objects
    belong to the same module and have same name.
    """
    from inspect import getsourcefile

    if func_left.__name__ == "<lambda>" or func_right.__name__ == "<lambda>":
        return func_left is func_right
    else:
        return func_left is func_right or (
            func_left.__module__ == func_right.__module__
            and func_left.__qualname__ == func_right.__qualname__
            and getsourcefile(func_left) == getsourcefile(func_right)
        )


def _is_overlapping_provider_sharable(tp: ProductType, binders: Tuple[Binder, ...]):
    _providers = [binder[tp] for binder in binders if tp in binder]
    if _providers:
        _standard = _providers.pop()
        if any([_provider for _provider in _providers if _provider != _standard]):
            return False
    return True


def _check_overlap(*binders: Binder):
    from functools import reduce

    tp_sets = [set(tp for tp in binder) for binder in binders]
    flat_tp_sets = reduce(lambda x, y: x.union(y), tp_sets)

    # If there is any overlapping providers
    if sum([len(tp_set) for tp_set in tp_sets]) > len(flat_tp_sets):
        conflicted = {
            tp
            for tp in flat_tp_sets
            if not _is_overlapping_provider_sharable(tp, binders)
        }
        # If there is any conflicting providers
        if any(conflicted):
            raise ProviderExistsError(
                "Binders have conflicting providers" f" {conflicted}"
            )


_Other = TypeVar("_Other")
_Separator = TypeVar("_Separator")


def _split_by_type(
    _arguments: Union[Tuple, List], _type: Type[_Separator]
) -> Tuple[List[_Separator], List]:
    _correct_type = [_arg for _arg in _arguments if isinstance(_arg, _type)]
    _other_type = [_arg for _arg in _arguments if not isinstance(_arg, _type)]
    return _correct_type, _other_type


class Binder:
    """Locally used binder between dependencies and providers."""

    def __init__(self, *__initials: Union[Constructor, Binder]) -> None:
        self._providers: Dict[ProductType, Provider] = dict()
        if __initials:
            _binders, _non_binders = _split_by_type(__initials, Binder)

            if _non_binders:
                __initial_binder = Binder()

                for _provider in _non_binders:
                    __initial_binder.register(_provider)

                _binders.append(__initial_binder)

            self.__merge(*_binders)
        else:
            ...

    def items(self):
        return self._providers.items()

    def __merge(self, *__others: Binder):
        """Merge providers after checking overlap."""
        _check_overlap(*__others)
        for _binder in __others:
            for _product_type, _provider in _binder.items():
                self[_product_type] = _provider
        return self

    def __iadd__(self, __other: object):
        """
        Merge other binder into this binder.
        """
        if not isinstance(__other, Binder):
            raise NotImplementedError(
                "+= operation between Binder and other type is not supported."
            )
        return self.__merge(__other)

    def __add__(self, __other: object):
        """
        Return a new binder containing all providers two binders have.
        """
        if not isinstance(__other, Binder):
            raise NotImplementedError(
                "+ operation between Binder and other type is not supported."
            )
        return Binder(self, __other)

    def __getitem__(self, product_type: ProductType) -> Provider:
        """
        Retrieve a provider call by the ``product type``.
        """
        if product_type not in self._providers:
            product_label = (
                product_type.__name__
                if hasattr(product_type, "__name__")
                else product_type
            )
            raise ProviderNotFoundError(f"Provider for ``{product_label}`` not found.")
        else:
            return self._providers[product_type]

    def _validate_new_provider(
        self, product_type: ProductType, new_provider_call: Provider
    ) -> None:
        """
        Raises
        ------
        ProviderExistsError:
            If there is an existing provider of the ``product_type``.
            However, if the full name of the ``new_provider_call``
            is same as the existing one, it will register
            the new callable object as the provider.

        MismatchingProductTypeError:
            If the return type of the ``provider_call`` is
            incompatible with ``product_type``.
            The return type of the ``provider_call`` should be a subclass of
            ``product_type``or the same object as ``product_type``.
            See ``ProviderCall.is_supprted_type``.
        """
        if product_type in self._providers:
            existing_provider = self._providers[product_type]
            if not _is_constructor_same(
                existing_provider.constructor, new_provider_call.constructor
            ):
                raise ProviderExistsError(
                    f"Can not register ``{new_provider_call.call_name}``."
                    f"Provider for ``{product_type.__name__}`` "
                    f"already exist. ``{existing_provider.call_name}``"
                )
        elif not new_provider_call.issupported(product_type):
            raise MismatchingProductTypeError(
                f"{product_type} can not be provided by "
                f"{new_provider_call.constructor}"
            )

    def __setitem__(
        self, product_type: ProductType, provider_call: Constructor
    ) -> None:
        """
        Register a provider function ``provider_call`` of ``product_type``
        into the ``self._providers`` dictionary.

        If ``product_type`` is ``UnknownType``,
        it will retrieve the product type from the provider call
        and use it as an index.

        ``self.provider`` decorator from ``beamlime.constructors`` module
        can also register a function or a class as a provider.

        See ``_validate_new_provider`` for new provider validation.

        """
        new_provider_call = Provider(provider_call)
        if product_type == UnknownType:
            _product_type = new_provider_call.product_spec.product_type
        else:
            _product_type = product_type

        self._validate_new_provider(_product_type, new_provider_call)
        self._providers[_product_type] = new_provider_call

    def register(self, provider_call: Constructor) -> None:
        """
        Register a provider function ``provider_call``
        into the provider dictionary.
        """
        self[UnknownType] = provider_call

    def pop(self, product_type: ProductType) -> Provider:
        """
        Remove and return the provider of ``product_type``.
        Returns ``UnknownProvider`` if not found.
        """
        return self._providers.pop(product_type, UnknownProvider)

    def clear(self) -> None:
        """Clear Providers."""
        self._providers.clear()

    def provider(self, callable_obj):
        """
        Register the decorated provider function into this binder.
        """
        _validate_callable_as_provider(callable_obj)

        self.register(callable_obj)
        return callable_obj

    def __iter__(self):
        return iter(self._providers)

    def __len__(self):
        return len(self._providers)


class GlobalBinder(Binder):
    """
    Globally used binder between dependencies and providers.

    Singleton object.
    """

    _instance: Optional[GlobalBinder] = None

    def __new__(cls) -> GlobalBinder:
        """Create or return the singleton object or ``GlobalBinder``."""
        if not isinstance(cls._instance, GlobalBinder):
            cls._instance = super().__new__(GlobalBinder)
            return cls._instance
        else:
            return cls._instance

    def __init__(self) -> None:
        if hasattr(self, "_providers"):
            ...
        else:
            self._providers: Dict[ProductType, Provider] = dict()


_GlobalBinder = GlobalBinder()


def provider(callable_obj: Constructor) -> Constructor:
    """
    Register the decorated provider function into the ``GlobalBinder()``.
    """
    _GlobalBinder.provider(callable_obj)
    return callable_obj


def get_global_binder() -> GlobalBinder:
    """Returns the singleton object of GlobalBinder."""
    return GlobalBinder()
