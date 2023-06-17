# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from functools import partial
from types import FunctionType, LambdaType
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union, final

from .inspectors import DependencySpec, ProductSpec, ProductType, UnknownType

Constructor = Union[LambdaType, partial, Callable, FunctionType, type]


class UnknownProvider:
    ...


class ProviderExistsError(Exception):
    ...


class ProviderNotFoundError(Exception):
    ...


class MismatchingProductTypeError(Exception):
    ...


def _find_attr_provider(dep_type: ProductType):
    try:
        return _Providers()[dep_type]
    except ProviderNotFoundError:
        return UnknownProvider


class AttrSubProviderGroup:
    def __init__(self, _constructor: type) -> None:
        """
        Attribute annotations are retrieved by typing.get_type_hints.
        """
        from typing import get_type_hints

        self._subproviders: Dict[str, Union[ProviderCall, Type[UnknownProvider]]] = {
            attr_name: _find_attr_provider(attr_type)
            for attr_name, attr_type in get_type_hints(_constructor).items()
        }

    def inject_dependencies(self, incomplete_obj: Any) -> Any:
        """
        Check if the ``incomplete_obj`` has attribute dependencies to be filled.
        If a provider is not found but the ``incomplete_obj`` already has the attribute,
        it skips the attribute injection.

        Raises
        ------
            ProviderNotFoundError:
                If a provider is not found for the attribute and
                the ``incomplete_obj`` still doesn't have the attribute.
        """
        for attr_name, attr_subprovider in self._subproviders.items():
            if attr_subprovider is UnknownProvider and not hasattr(
                incomplete_obj, attr_name
            ):
                raise ProviderNotFoundError(
                    "Provider for the attribute " f"{attr_name} not found."
                )
            elif attr_subprovider is UnknownProvider:
                ...  # Skip injecting dependency.
            else:
                setattr(incomplete_obj, attr_name, attr_subprovider())
        return incomplete_obj


def _find_arg_provider(arg_spec: DependencySpec):
    try:
        return _Providers()[arg_spec.product_type]
    except ProviderNotFoundError as err:
        from inspect import Signature

        from .inspectors import UnknownType

        if (
            arg_spec.default_product != Signature.empty
            or arg_spec.product_type == UnknownType
        ):
            return UnknownProvider
        raise err


class ArgSubProviderGroup:
    def __init__(self, _constructor: Callable) -> None:
        from .inspectors import collect_argument_dep_specs

        self._subproviders = {
            arg_name: _prov
            for arg_name, arg_spec in collect_argument_dep_specs(_constructor).items()
            if (_prov := _find_arg_provider(arg_spec)) != UnknownProvider
        }

    def build_arguments(self) -> Dict[str, Any]:
        return {arg_name: _prov() for arg_name, _prov in self._subproviders.items()}


@final
class ProviderCall:
    def __new__(cls, _provider_call: Union[Constructor, ProviderCall]) -> ProviderCall:
        if isinstance(_provider_call, ProviderCall):
            return _provider_call
        else:
            return super().__new__(ProviderCall)

    def __init__(self, _provider_call: Union[Constructor, ProviderCall]) -> None:
        if isinstance(_provider_call, ProviderCall):
            ...
        else:
            from .inspectors import get_product_spec

            self._provider_call: Constructor = _provider_call
            self.product_spec = get_product_spec(_provider_call)

    @property
    def call_name(self) -> str:
        return ".".join((self.constructor.__module__, self.constructor.__qualname__))

    @property
    def constructor(self) -> Constructor:
        return self._provider_call

    def issupported(self, product_type: Union[ProductType, ProductSpec]):
        """Check if the given ``product_type`` can be supported by this provider."""
        from .inspectors import issubproduct

        return issubproduct(ProductSpec(product_type), self.product_spec)

    def __call__(self) -> Any:
        """Build and return the product with attribute dependencies injected."""
        # TODO: Split this into two steps, build chain of sub-providers
        # and call the providers.
        arg_subproviders = ArgSubProviderGroup(self.constructor)
        _obj = self.constructor(**arg_subproviders.build_arguments())
        attr_subproviders = AttrSubProviderGroup(_obj.__class__)
        return attr_subproviders.inject_dependencies(_obj)


def _is_provider_call_same(func_left: Callable, func_right: Callable) -> bool:
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


class _Providers:
    """
    The singleton object/class that holds provider dictionary.

    """

    _instance: Optional[_Providers] = None
    _providers: Dict[ProductType, ProviderCall] = dict()

    def __new__(cls) -> _Providers:
        if not cls._instance:
            cls._instance = super().__new__(_Providers)
            return cls._instance
        else:
            return cls._instance

    def __getitem__(self, product_type: ProductType) -> ProviderCall:
        """
        Retrieve a provider call by the ``product type``.
        """
        if product_type not in self._providers:
            product_label = (
                product_type.__name__
                if hasattr(product_type, "__name__")
                else product_type
            )
            raise ProviderNotFoundError("Provider for ", product_label, " not found.")
        else:
            return self._providers[product_type]

    def _validate_new_provider(
        self, product_type: ProductType, new_provider_call: ProviderCall
    ) -> None:
        """
        Raises
        ------
        ProviderExistsError:
            When a provider of ``product_type`` already exists in the Container,
            and the new provider ``provider_call`` has different name
            or is from different module.

        MismatchingProductTypeError:
            If the return type of the ``provider_call`` is
            incompatible with ``product_type``.
            The return type of the ``provider_call`` should be a subclass of
            ``product_type``or the same object as ``product_type``.
            See ``ProviderCall.is_supprted_type``.
        """
        if product_type in self._providers:
            existing_provider = self._providers[product_type]
            if not _is_provider_call_same(
                existing_provider.constructor, new_provider_call.constructor
            ):
                if hasattr(product_type, "__name__"):
                    product_label = product_type.__name__
                else:
                    product_label = str(product_type)
                raise ProviderExistsError(
                    f"Can not register ``{new_provider_call.call_name}``."
                    f"Provider for ``{product_label}`` "
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
        into the provider dictionary.

        If a provider of the ``product_type`` already exists,
        a new provider with the same name from the same module
        will overwrite the existing one.
        If the new provider doesn't have the same name or
        is from a different module, it will raise an error.

        If ``product_type`` is ``UnknownType``,
        it will retrieve the product type from the provider call
        and use it as an index.

        You can use ``provider`` decorator from beamlime.constructors
        module to register a function or a class as a provider.

        See ``Providers._validate_new_provider`` for new provider validation.

        """
        new_provider_call = ProviderCall(provider_call)
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

    def pop(self, product_type: ProductType) -> Optional[ProviderCall]:
        """
        Remove and return the provider of ``product_type``.
        Returns None if not found.
        """
        return self._providers.pop(product_type, None)

    def clear_all(self) -> None:
        """Clear Providers."""
        self._providers.clear()


_Constructor = TypeVar("_Constructor")  # type: ignore[misc]
_lambda_name = (lambda: None).__qualname__


def provider(callable_obj: _Constructor) -> _Constructor:
    """
    Register the decorated provider function into the ``Providers``.
    """
    # If the callable object belongs to a class.

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

    _Providers().register(callable_obj)  # type: ignore[arg-type]
    return callable_obj


def get_providers() -> _Providers:
    """Returns a singleton object of _Providers."""
    return _Providers()
