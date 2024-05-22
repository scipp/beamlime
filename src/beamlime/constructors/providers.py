# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from functools import lru_cache, partial
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Literal,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    overload,
)

from .inspectors import (
    DependencySpec,
    Product,
    ProductSpec,
    UnknownType,
    collect_argument_specs,
    collect_attr_specs,
    get_product_spec,
)

DependencySpecDict = Dict[str, DependencySpec]

_lambda_name = (lambda: None).__qualname__


class ProviderNotFoundError(Exception): ...


class ConflictProvidersError(Exception): ...


class ProviderExistsError(Exception): ...


class MismatchingProductTypeError(Exception): ...


def _validate_callable_as_provider(
    callable_obj: Callable[..., Product],
) -> Literal[True]:
    """
    Raises
    ------
    NotImplementedError
        If the constructor of a provider is a method of a class.

    Notes
    -----
    It is not supported to use a method of a class as a provider,
    because it is not possible to check
    if a ``callable_obj`` is bound to an instantiated object or just a class
    without an access to the object containing the method.
    Currently, it is checking if ``__qualname__`` is different from ``__name__``
    to see if the ``callable_obj`` is a method.

    TODO: Update the if statement with explicit check for a method.
    """
    if (
        not isinstance(callable_obj, partial)
        and not isinstance(callable_obj, type)
        and hasattr(callable_obj, "__name__")
        and not (callable_obj.__name__ == _lambda_name)
        and hasattr(callable_obj, "__qualname__")
        and callable_obj.__qualname__ != callable_obj.__name__
    ):
        raise NotImplementedError(
            "A member method of a class can not be registered as a provider yet."
        )
    return True


class Provider(Generic[Product]):
    """
    Function wrapper that provides certain type of product.

    It is similar to ``partial`` but in addition, it contains
    argument or attribute dependency information of the wrapped callable object.

    """

    def __init__(
        self,
        _constructor: Constructor[Product],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Notes
        -----
        Nested ``Provider`` or ``partial`` as a ``constructor``
        is forbidden similar to ``partial``.
        """
        self._constructor: Callable[..., Product]
        self._init_constructor(_constructor)

        self.args: tuple[Any, ...]
        self.keywords: dict[str, Any]
        self._init_arguments(_constructor, args, kwargs)

        self.arg_dep_specs: DependencySpecDict
        self.attr_dep_specs: DependencySpecDict
        self.product_spec: ProductSpec
        self._init_dependencies()

    def _init_constructor(self, _constructor: Constructor[Product]) -> None:
        if isinstance(_constructor, Provider):
            self._constructor = _constructor.constructor
        elif isinstance(_constructor, partial):
            self._constructor = _constructor.func
        else:
            self._constructor = _constructor

        _validate_callable_as_provider(self._constructor)

    def _init_arguments(
        self,
        _constructor: Constructor[Product],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if isinstance(_constructor, (partial, Provider)):
            self.args = (*_constructor.args, *args)
            self.keywords = {**_constructor.keywords, **kwargs}
        else:
            self.args = args
            self.keywords = kwargs

    def _init_dependencies(self) -> None:
        self.arg_dep_specs = collect_argument_specs(
            self._constructor, *self.args, **self.keywords
        )
        self.attr_dep_specs = collect_attr_specs(self._constructor)
        self.product_spec = get_product_spec(self._constructor)

    @property
    def call_name(self) -> str:
        return ".".join((self.constructor.__module__, self.constructor.__qualname__))

    @property
    def constructor(self) -> Callable[..., Product]:
        return self._constructor

    def can_provide(self, product_type: Union[Type[Product], ProductSpec]) -> bool:
        """
        Check if the given ``product_type`` can be supported by this provider.
        Product type should be the same type as the returned type of this provider.
        If they are not the same types,
        the requested product type should be the parent class
        of returned type of this provider.
        """
        from typing import Any, get_origin

        requested_tp = ProductSpec(product_type).returned_type
        provided_tp = self.product_spec.returned_type

        if provided_tp in (Any, UnknownType) or requested_tp == provided_tp:
            return True
        elif orig_tp := get_origin(requested_tp):
            # If the requested type is generic.
            # Note that special form generics such as ``Union``
            # that are not instance of ``type`` will return ``False``.
            return self.can_provide(orig_tp)
        elif isinstance(provided_tp, type) and isinstance(requested_tp, type):
            # If the requested type is a parent class of the provided type.
            return issubclass(provided_tp, requested_tp)
        else:
            return False

    def __call__(self, *args: Any, **kwargs: Any) -> Product:
        """Call the constructor with extra arguments."""
        return self.constructor(*self.args, *args, **self.keywords, **kwargs)

    def __eq__(self, other: object) -> bool:
        """Compare ``constructor``, ``args`` and ``keywords`` of the provider."""
        if not isinstance(other, Provider):
            raise NotImplementedError(
                "Comparison between Provider with other type is not supported."
            )
        else:
            return (
                self.constructor is other.constructor
                and self.args == other.args
                and self.keywords == other.keywords
            )

    def __hash__(self) -> int:
        return hash(self.constructor)

    def __repr__(self) -> str:
        return f"Provider({self.call_name}, *{self.args}, **{self.keywords})."

    @classmethod
    def __copy__(cls, _obj: Provider[Product]) -> Provider[Product]:
        return cls(_obj)


_Item = TypeVar("_Item")


def split_sequence_by_filter(
    filter_func: Callable[[_Item], bool], sequence: Iterable[_Item]
) -> Tuple[Tuple[_Item, ...], Tuple[_Item, ...]]:
    """Split a sequence into two sequences based on the filter.

    The first one of returned sequences contains items that pass the filter.
    The other sequence contains the rest.
    """
    filtered = tuple(filter(filter_func, sequence))
    rest = tuple(filter(lambda x: x not in filtered, sequence))
    return filtered, rest


_Key = TypeVar("_Key")
_Value = TypeVar("_Value")


def split_dict_by_filter(
    filter_func: Callable[[_Key, _Value], bool], sequence: dict[_Key, _Value]
) -> Tuple[dict[_Key, _Value], dict[_Key, _Value]]:
    """Split a dictionary into two dictionaries based on the filter.

    The first one of returned dictionaries contains items that pass the filter.
    The other dictionary contains the rest.
    """
    filtered = {
        key: value for key, value in sequence.items() if filter_func(key, value)
    }
    rest = {key: value for key, value in sequence.items() if key not in filtered}
    return filtered, rest


class _ArgumentFilter(ABC):
    def __init__(self) -> None:
        self.arguments: dict[str, Any]

    @abstractmethod
    def arguments_filter(self, arg_name: str, arg_val: Any) -> bool: ...

    @abstractmethod
    def save_arguments(self, arguments: dict) -> None: ...

    def was_called(self) -> bool:
        return hasattr(self, 'arguments')

    def __call__(
        self, arguments: dict[str, Any], reverse: bool = False
    ) -> dict[str, Any]:
        if not self.was_called():
            self.save_arguments(arguments)
            filtered, rest = arguments, {}
        else:
            filtered, rest = split_dict_by_filter(self.arguments_filter, arguments)

        return rest if reverse else filtered


class _ArgumentsInstanceFilter(_ArgumentFilter):
    """Filter arguments by instance equality."""

    def arguments_filter(self, arg_name: str, arg_val: Any) -> bool:
        # self.arguments.get(arg_name) is not sufficient
        # because it will return ``None`` even if it was not used in the previous call.
        return arg_name in self.arguments and self.arguments.get(arg_name) is arg_val

    def save_arguments(self, arguments: dict) -> None:
        self.arguments = arguments


class _ArgumentsHashFilter(_ArgumentFilter):
    """Filter arguments by hash keys."""

    def __init__(self) -> None:
        from functools import _lru_cache_wrapper

        super().__init__()
        self.indicators: dict[str, _lru_cache_wrapper] = {}

    def initialize_indicators(self) -> None:
        for arg_name, arg_val in self.arguments.items():
            indicator = lru_cache(maxsize=2)(lambda **_: None)
            indicator(**{arg_name: arg_val})
            self.indicators[arg_name] = indicator

    def reset_indicators(self, *arg_names) -> None:
        for arg_name in arg_names:
            indicator = self.indicators[arg_name]
            indicator.cache_clear()
            indicator(**{arg_name: self.arguments.get(arg_name)})

    def check_indicator(self, arg_name: str, arg_val: Any) -> bool:
        indicator = self.indicators[arg_name]
        indicator(**{arg_name: arg_val})
        if indicator.cache_info().currsize > 1:
            self.reset_indicators(arg_name)
            return False
        else:
            return True

    def arguments_filter(self, arg_name: str, arg_val: Any) -> bool:
        if arg_val.__hash__ is not None and arg_name in self.indicators:
            return self.check_indicator(arg_name, arg_val)
        return False

    def save_arguments(self, arguments: dict[str, Any]) -> None:
        self.arguments = arguments
        self.initialize_indicators()


class ArgumentsFilter:
    """
    Remember arguments and check if the new arguments match the old ones.
    """

    def __init__(self, func: Callable[..., Any]) -> None:
        from inspect import Signature, signature

        self._sig: Signature = signature(func)
        self.instance_filter = _ArgumentsInstanceFilter()
        self.hash_filter = _ArgumentsHashFilter()
        super().__init__()

    def __call__(self, *args: Any, **kwargs: Any) -> bool:
        """
        Saves the arguments and returns ``True`` on the first call.
        The second time it is called, it checks if the new arguments match the old ones.

        It checks instance equalities of all arguments first
        and separate hashable arguments from unhashable ones if it fails.
        For hashable arguments, it uses ``lru_cache`` and
        for unhashable arguments, it checks instance equalities.

        Returns
        -------
        ``True``
            If it is the first call or the new arguments match the first ones.

        ``False``
            If the new arguments don't match the first ones.
        """

        bound = self._sig.bind(*args, **kwargs)
        instance_filter_rest = self.instance_filter(bound.arguments, reverse=True)
        hash_filter_rest = self.hash_filter(instance_filter_rest, reverse=True)
        return len(hash_filter_rest) == 0


class SingletonProviderCalledWithDifferentArgs(Exception): ...


class SingletonProvider(Provider[Product]):
    """
    Singleton provider always returns the same instance.

    Therefore passing different arguments from the first call is not allowed.
    ``functools.lru_cache`` is used to check if the hashable arguments are the same.
    For unhashable arguments, ``cached_arguments_filter``
    check if they are the same instances.

    ``SingletonProvider`` is needed for sharing the same object by multiple objects.
    The arguments filter prioritizes instance equality over hash equality.
    If the hashable arguments check is not compatible,
    consider providing the dependencies also as singletons.

    Raises
    ------
    SingletonProviderCalledWithDifferentArgs
        If the provider is called with different argument from the first call.
    """

    def __init__(
        self, _constructor: Constructor[Product], /, *args: Any, **kwargs: Any
    ) -> None:
        super().__init__(_constructor, *args, **kwargs)
        self.cached_result: Product
        self.cached_arguments_filter = ArgumentsFilter(self._constructor)

    def __call__(self, *args: Any, **kwargs: Any) -> Product:
        if not hasattr(self, "cached_result"):
            self.cached_result = self.constructor(
                *self.args, *args, **self.keywords, **kwargs
            )
        if not self.cached_arguments_filter(*args, **kwargs):
            err_msg = (
                f"SingletonProvider {self} was called with "
                "different arguments from the first call."
            )
            raise SingletonProviderCalledWithDifferentArgs(err_msg)
        return self.cached_result


class UnknownProviderCalled(Exception): ...


def unknown_provider_call() -> Any:
    raise UnknownProviderCalled("Unknown provider is called.")


UnknownProvider = Provider(unknown_provider_call)


def check_conflicting_providers(*prov_grs: ProviderGroup) -> None:
    """Raise an error is given factories have any conflicting providers."""
    from functools import reduce

    keys_list: list[set[Type[Any]]] = [set(prov_gr.keys()) for prov_gr in prov_grs]
    union_keys = reduce(lambda x, y: x.union(y), keys_list)

    def _collect_by(tp: Type[Product]) -> set[Provider[Product]]:
        return {group[tp] for group in prov_grs if tp in group}

    # If there is any overlapping providers or conflicting providers
    if conflicts := {
        tp: providers for tp in union_keys if len((providers := _collect_by(tp))) > 1
    }:
        raise ConflictProvidersError(
            f"Factories have conflicting providers for, {conflicts}"
        )


def merge(*prov_grs: ProviderGroup) -> ProviderGroup:
    """Return a new ``ProviderGroup`` containing all providers of ``prov_grs``."""
    prov_gr = ProviderGroup()
    prov_gr.merge(*prov_grs)
    return prov_gr


def _product_type_label(tp: Type[Product]) -> str:
    return tp.__name__ if hasattr(tp, "__name__") else str(tp)


Constructor = Union[Provider[Product], partial[Product], Callable[..., Product]]
P = TypeVar("P")


class ProviderGroup:
    """
    Group of providers.
    """

    def __init__(self, *initial_providers: Callable[..., Product]) -> None:
        """
        Initializes an empty internal provider dictionary
        and fills it with the initial providers from the argument.
        """
        self._providers: Dict[Type[Product], Provider[Product]] = {}
        for _provider in initial_providers:
            self.provider(_provider)

    def keys(self) -> KeysView[Type[Product]]:
        return self._providers.keys()

    def values(self) -> ValuesView[Provider[Product]]:
        return self._providers.values()

    def items(self) -> ItemsView[Type[Product], Provider[Product]]:
        return self._providers.items()

    @classmethod
    def __copy__(cls, _obj: ProviderGroup) -> ProviderGroup:
        """Return a new provider group containing same providers."""
        return merge(_obj)

    def __iter__(self) -> Iterator[Type[Product]]:
        """Return an iterator of the product type(s) this group can provide."""
        return iter(self.keys())

    def __len__(self) -> int:
        """Return the number of product type(s) this group can provide."""
        return len(self._providers)

    def merge(self, *others: ProviderGroup) -> None:
        """Merge other provider groups into this group after checking conflicts."""
        from copy import copy

        check_conflicting_providers(self, *others)
        for _group in others:
            copied_providers = {
                key: copy(provider) for key, provider in _group._providers.items()
            }
            self._providers.update(copied_providers)

    def __add__(self, another: object) -> ProviderGroup:
        """Return a new group containing all providers of two groups."""
        if not isinstance(another, ProviderGroup):
            raise NotImplementedError(
                "+ operation between ProviderGroup and other type is not supported."
            )
        return merge(self, another)

    def pop(self, product_type: Type[Product]) -> Provider[Product]:
        """
        Remove and return the provider of ``product_type``.
        Return ``UnknownProvider`` if not found.
        """
        return self._providers.pop(product_type, UnknownProvider)

    def clear(self) -> None:
        """Clear all providers of this group."""
        self._providers.clear()

    def _validate_and_register(
        self, product_type: Type[Product], provider: Provider[Product]
    ) -> None:
        """
        Validate a provider and add the provider if valid.

        Raises
        ------
        ProviderExistsError
            If there is an existing provider of the ``product_type``.

        MismatchingProductTypeError
            If the return type of the ``provider`` is not a subclass of
            ``product_type``or the same object as ``product_type``.
            See ``Provider.can_provide``.
        """
        if (
            existing_provider := self._providers.get(product_type, None)
        ) and existing_provider != provider:
            raise ProviderExistsError(
                f"Can not register ``{provider}``."
                f"Provider of ``{_product_type_label(product_type)}``, "
                f"``{existing_provider}`` already exists."
            )
        elif not provider.can_provide(product_type):
            raise MismatchingProductTypeError(
                f"{_product_type_label(product_type)} "
                f"can not be provided by {provider}."
            )
        self._providers[product_type] = provider

    def __getitem__(self, product_type: Type[Product]) -> Provider[Product]:
        """
        Return the provider of the requested product type.

        Raises
        ------
        ProviderNotFoundError
            If there is any providers for the requested product type.

        """
        try:
            return self._providers[product_type]
        except KeyError:
            product_label = _product_type_label(product_type)
            raise ProviderNotFoundError(
                f"Provider for ``{product_label}`` not found."
            ) from None

    @staticmethod
    def _wrap_provider(
        provider_call: Callable[..., Product], provider_tp: Type[Provider] | None = None
    ) -> Provider[Product]:
        """Wrap a callable object with a ``Provider`` or its subclass."""
        if provider_tp is None:
            if isinstance(provider_call, Provider):
                return type(provider_call)(provider_call)
            else:
                return Provider(provider_call)
        else:
            return provider_tp(provider_call)

    def __setitem__(
        self, product_type: Type[Product], provider_call: Callable[..., Product]
    ) -> None:
        """
        Register a callable object ``provider_call`` as a provider of ``product_type``.
        See ``_validate_and_register`` for new provider validation.

        Notes
        -----
        If the new provider is same as the existing one,
        it will register the new callable object as the provider
        instead of raising ``ProviderExistsError``.

        """
        new_provider = self._wrap_provider(provider_call)
        self._validate_and_register(product_type, new_provider)

    @overload
    def provider(
        self,
        provider_call: Type[Product],
        /,
        *,
        provider_type: Type[Provider] = Provider,
    ) -> Type[Product]:  # This signiture is needed when it decorates a class.
        ...

    @overload
    def provider(
        self,
        provider_call: Callable[..., Product],
        /,
        *,
        provider_type: Type[Provider] = Provider,
    ) -> Callable[..., Product]: ...

    @overload
    def provider(
        self, provider_call: None = None, /, *, provider_type: Type[Provider]
    ) -> Callable[[P], P]: ...

    def provider(
        self,
        provider_call: None | Callable[..., Product] | Type[Product] = None,
        /,
        *,
        provider_type: Type[Provider] | None = None,
    ) -> (
        Callable[
            [Callable[..., Product] | Type[Product]],
            Callable[..., Product] | Type[Product],
        ]
        | Callable[..., Product]
        | Type[Product]
    ):
        """
        Register the decorated callable into this group.
        The product type will be retrieved from the annotation.

        Examples
        --------
        >>> from beamlime import ProviderGroup
        >>> from typing import Literal
        >>> number_providers = ProviderGroup()
        >>> @number_providers.provider
        ... def give_one() -> Literal[1]:
        ...   return 1
        ...
        >>> number_providers[Literal[1]]() == 1
        True
        """

        @overload
        def wrapper(
            provider_call: Callable[..., Product], provider_tp: Type[Provider] | None
        ) -> Callable[..., Product]: ...

        @overload
        def wrapper(
            provider_call: Type[Product], provider_tp: Type[Provider] | None
        ) -> Type[Product]: ...

        def wrapper(
            provider_call: Callable[..., Product] | Type[Product],
            provider_tp: Type[Provider] | None,
        ) -> Callable[..., Product] | Type[Product]:
            new_provider = self._wrap_provider(provider_call, provider_tp)
            _product_type = new_provider.product_spec.product_type
            self[_product_type] = new_provider
            return provider_call

        if provider_call is None:
            return partial(wrapper, provider_tp=provider_type)
        else:
            return wrapper(provider_call, provider_tp=provider_type)


__all__ = ["Product"]
