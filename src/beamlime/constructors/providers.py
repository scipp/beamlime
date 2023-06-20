# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from functools import partial
from types import FunctionType, LambdaType
from typing import Any, Callable, Dict, Union, final

from .inspectors import DependencySpec, ProductSpec, ProductType


def _find_attr_provider(dep_type: ProductType):
    from .binders import ProviderNotFoundError
    from .contexts import context_binder

    with context_binder() as binder:
        try:
            return binder[dep_type]
        except ProviderNotFoundError:
            return UnknownProvider


class AttrSubProviders:
    def __init__(self, _constructor: type) -> None:
        """
        Attribute annotations are retrieved by typing.get_type_hints.
        """
        from typing import get_type_hints

        self._subproviders: Dict[str, Provider] = {
            attr_name: _find_attr_provider(attr_type)
            for attr_name, attr_type in get_type_hints(_constructor).items()
        }

    def inject_dependencies(self, incomplete_obj):
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
                from .binders import ProviderNotFoundError

                raise ProviderNotFoundError(
                    "Provider for the attribute " f"{attr_name} not found."
                )
            elif attr_subprovider is UnknownProvider:
                ...  # Skip injecting dependency.
            else:
                setattr(incomplete_obj, attr_name, attr_subprovider())
        return incomplete_obj


def _find_arg_provider(arg_spec: DependencySpec):
    from .binders import ProviderNotFoundError
    from .contexts import context_binder

    with context_binder() as binder:
        try:
            return binder[arg_spec.product_type]
        except ProviderNotFoundError as err:
            from inspect import Signature

            from .inspectors import UnknownType

            if (
                arg_spec.default_product != Signature.empty
                or arg_spec.product_type == UnknownType
            ):
                return UnknownProvider
            raise err


class ArgSubProviders:
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
class Provider:
    def __new__(cls, _provider_call: Union[Constructor, Provider]) -> Provider:
        if isinstance(_provider_call, Provider):
            return _provider_call
        else:
            return super().__new__(Provider)

    def __init__(self, _provider_call: Union[Constructor, Provider]) -> None:
        if isinstance(_provider_call, Provider):
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

    def __call__(self, *args, **kwargs):
        """Build and return the product with attribute dependencies injected."""
        # TODO: Split this into two steps, build chain of sub-providers
        # and call the providers.
        _constructor: Constructor

        if args or kwargs:
            _constructor = partial(self.constructor, *args, **kwargs)
        else:
            _constructor = self.constructor
        arg_subproviders = ArgSubProviders(_constructor)
        _obj = _constructor(**arg_subproviders.build_arguments())
        attr_subproviders = AttrSubProviders(_obj.__class__)
        return attr_subproviders.inject_dependencies(_obj)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Provider):
            raise NotImplementedError(
                "Comparison between Provider " "with other type is not supported."
            )
        else:
            return self.constructor is other.constructor


def unknown_provider_call():
    from ..constructors import ProviderNotFoundError

    raise ProviderNotFoundError("Unknown provider is called.")


UnknownProvider = Provider(unknown_provider_call)
Constructor = Union[LambdaType, partial, Callable, FunctionType, type, Provider]
