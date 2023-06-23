# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from typing import Any, Dict, Type

from .base import (
    FactoryBase,
    MismatchingProductTypeError,
    Product,
    ProviderExistsError,
    ProviderNotFoundError,
)
from .inspectors import DependencySpec, ProductType, UnknownType
from .providers import Constructor, Provider


class RegistrationInterface(FactoryBase):
    """Provider registration interfaces."""

    def register(self, product_type: ProductType, provider_call: Constructor) -> None:
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

    def _validate_new_provider(
        self, product_type: ProductType, new_provider: Provider
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
            if existing_provider != new_provider:
                raise ProviderExistsError(
                    f"Can not register ``{new_provider.call_name}``."
                    f"Provider for ``{product_type.__name__}`` "
                    f"already exist. ``{existing_provider.call_name}``"
                )
        if not new_provider.can_provide(product_type):
            raise MismatchingProductTypeError(
                f"{product_type} can not be provided by " f"{new_provider.constructor}"
            )

    def provider(self, callable_obj: Constructor):
        """
        Register the decorated provider function into this factory.
        """
        from .inspectors import UnknownType

        self.register(UnknownType, callable_obj)
        return callable_obj


class Empty:
    ...


class InjectionInterface(RegistrationInterface):
    """Dependency injection interfaces."""

    def build_arg_dependency(self, arg_spec: DependencySpec):
        try:
            return self._assemble(arg_spec.product_type)
        except ProviderNotFoundError as err:
            from inspect import Signature

            from .inspectors import UnknownType

            if (arg_spec.default_product != Signature.empty) or (
                arg_spec.product_type == UnknownType
            ):
                return Empty
            raise err

    def build_arguments(self, provider: Provider):
        return {
            arg_name: _product
            for arg_name, arg_spec in provider.arg_dep_specs.items()
            if (_product := self.build_arg_dependency(arg_spec)) != Empty
        }

    def build_attr_dependency(self, dep_type: ProductType):
        try:
            return self._assemble(dep_type)
        except ProviderNotFoundError:
            return Empty

    def inject_dependencies(self, _obj: Product) -> Product:
        """
        Check if the ``incomplete_obj`` has attribute dependencies to be filled.
        If ``incomplete_obj`` is missing any attribute with type hint,
        or if the attribute is None, the dependency will be assembled and injected.
        If a provider is not found but the ``incomplete_obj`` already has the attribute,
        it skips the attribute injection.

        Raises
        ------
            ProviderNotFoundError:
                If a provider is not found for the attribute and
                the ``incomplete_obj`` still doesn't have the attribute.

        Attribute annotations are retrieved by typing.get_type_hints.
        """
        from typing import get_type_hints

        _attrs: Dict[str, Any] = {
            _attr_name: self.build_attr_dependency(attr_type)
            for _attr_name, attr_type in get_type_hints(_obj.__class__).items()
            if not hasattr(_obj, _attr_name) or getattr(_obj, _attr_name) is None
        }

        if any(
            missing_attrs := [
                _attr_name for _attr_name, _attr in _attrs.items() if _attr is Empty
            ]
        ):
            raise ProviderNotFoundError(
                f"Provider for the attribute(s) {missing_attrs} not found "
                f"during the dependency injection to the {_obj.__class__} instance."
            )

        for _attr_name, _attr in _attrs.items():
            if _attr is Empty:
                ...  # Skip injecting dependency.
                # The other case where attr_subprovider
                # is not found but the ``_obj`` does not
                # have the attribute is filtered
                # by checking the missing attributes above.
            else:
                setattr(_obj, _attr_name, _attr)
        return _obj

    def _assemble(self, product_type: Type[Product]) -> Product:
        _provider = self.find_provider(product_type)
        kwargs = {**self.build_arguments(_provider), **_provider.keywords}
        _obj = _provider.constructor(*_provider.args, **kwargs)
        return self.inject_dependencies(_obj)
