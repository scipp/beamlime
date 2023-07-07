# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Type

from .inspectors import ProductType
from .providers import (
    Product,
    Provider,
    ProviderGroup,
    ProviderNotFoundError,
    UnknownProvider,
)


class Factory:
    """
    Dependency injection object.

    Notes
    -----
    Currently it does not inject dependencies for magic variables,
    i.e. *args and **kwargs.
    """

    def __init__(self, *initial_prov_grs: ProviderGroup) -> None:
        """
        Initializes a factory with multiple provider groups.
        """
        from .providers import merge

        self.providers: ProviderGroup = merge(*initial_prov_grs)

    @property
    def catalogue(self) -> frozenset[ProductType]:
        """Frozen set of the product type(s) this factory can manufacture."""
        return frozenset(self.providers.keys())

    def __iter__(self) -> Iterator[ProductType]:
        """Return an iterator of the product type(s) this factory can manufacture."""
        return iter(self.catalogue)

    def __len__(self) -> int:
        """Return the number of product type(s) this factory can manufacture."""
        return len(self.catalogue)

    def _call_provider(self, product_type: Type[Product]) -> Product:
        """
        Build keyword argument dependencies of the provider
        and call the provider with them.
        """
        provider = self.providers[product_type]
        keyword_arguments = {
            arg_name: self[arg_spec.dependency_type]
            for arg_name, arg_spec in provider.arg_dep_specs.items()
            if arg_spec.dependency_type in self or not arg_spec.is_optional()
        }
        return provider(**keyword_arguments)

    def _inject_attributes(self, product: Product, product_type) -> Product:
        """
        Build and inject attribute dependencies of the ``product``.
        If ``product`` is missing any attributes that are type-hinted,
        the attribute(dependency) will be assembled and injected.

        Raises
        ------
        ProviderNotFoundError
            If any type-hinted attributes are not populated in ``product`` and
            any providers for the missing attributes are not found.

        Notes
        -----
        When the ``product`` already has the attribute but it is ``None``,
            1. if a provider is not found, it skips injection.
            2. if a provider is found, it overwrites the attribute.

        """
        provider = self.providers[product_type]
        attr_dependencies: Dict[str, Any] = {
            attr_name: attr_spec.dependency_type
            for attr_name, attr_spec in provider.attr_dep_specs.items()
            if getattr(product, attr_name, None) is None
        }

        for attr_name, attr_type in attr_dependencies.items():
            try:
                setattr(product, attr_name, self[attr_type])
            except ProviderNotFoundError as err:
                if not hasattr(product, attr_name):
                    raise err
                else:
                    ...  # Skip injecting dependency.

        return product

    def __getitem__(self, product_type: Type[Product]) -> Product:
        """
        Build an object of ``product_type`` using the registered providers.

        Notes
        -----
        Currently position-only argument dependencies are not supported.
        It only builds the keyword argument dependencies
        and attribute dependencies for the provider.

        TODO: Detect cyclic dependencies earlier.
        """
        try:
            product = self._call_provider(product_type)
            return self._inject_attributes(product, product_type)
        except RecursionError:
            raise RecursionError(
                "Cyclic dependencies found" f"while assembling {product_type}."
            )

    @contextmanager
    def local_factory(self, *provider_groups: ProviderGroup) -> Iterator[Factory]:
        """
        Create and yield a new factory containing
        a copy of ``self.providers`` and ``provider_groups``.
        For existing providers, ``provider_groups`` will overwrite them.
        """
        from copy import copy

        from .providers import merge

        merged = merge(*provider_groups)
        my_providers = copy(self.providers)
        for product_type in merged:
            my_providers.pop(product_type)
        yield Factory(my_providers, merged)

    @contextmanager
    def constant_provider(self, product_type: ProductType, hardcoded_value: Any):
        """
        Use a lambda function that returns ``hardcoded_value``.
        as a temporary provider of ``product_type``.
        """
        with self.temporary_provider(product_type, lambda: hardcoded_value):
            yield None

    @contextmanager
    def partial_provider(self, product_type: ProductType, *args: Any, **kwargs: Any):
        """
        Create a partial provider for ``product_type`` with ``args`` and ``kwargs``
        and use it as a temporary provider.
        """
        _partial: Provider = Provider(self.providers[product_type], *args, **kwargs)
        with self.temporary_provider(product_type, _partial):
            yield None

    @contextmanager
    def temporary_provider(
        self, product_type: ProductType, temp_provider: Callable[..., Product]
    ):
        """
        Replace an existing provider of ``product_type`` with ``temp_provider``
        or register ``temp_provider``.
        ``temp_provider`` will be replaced with the original provider or removed
        as the context terminates.
        """

        original_provider = self.providers.pop(product_type)

        try:
            self.providers[product_type] = temp_provider
            yield None
        finally:
            self.providers.pop(product_type)
            if original_provider is not UnknownProvider:
                self.providers[product_type] = original_provider
