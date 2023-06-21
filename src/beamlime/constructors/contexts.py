# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Union

from .injection import InjectionInterface
from .inspectors import ProductType
from .providers import Constructor, Provider, UnknownProvider


class FactoryContextInterface(InjectionInterface):
    """Context managers for different use of providers."""

    @contextmanager
    def constant_provider(self, product_type: ProductType, hardcoded_value: Any):
        """
        Use a lambda function that returns ``hardcoded_value``.
        as a temporary provider of ``product_type``.
        """
        try:
            with self.temporary_provider(product_type, lambda: hardcoded_value):
                yield None
        finally:
            ...

    @contextmanager
    def partial_provider(self, product_type: ProductType, *args: Any, **kwargs: Any):
        """
        Create a partial provider for ``product_type`` arguments
        and use it as a temporary provider.

        """
        from .providers import Provider

        try:
            _partial_provider = Provider(
                self.find_provider(product_type), *args, **kwargs
            )
            with self.temporary_provider(product_type, _partial_provider):
                yield None
        finally:
            ...

    @contextmanager
    def temporary_provider(
        self, product_type: ProductType, temp_provider: Union[Constructor, Provider]
    ):
        """
        Create a new factory with the ``temp_provider`` and yield it.
        If there is a provider of ``product_type`` in the factory,
        the temporary provider will replace the existing one.
        """

        original_provider = self.pop(product_type)

        try:
            self.register(product_type, temp_provider)
            yield None
        finally:
            self.pop(product_type)
            if original_provider is not UnknownProvider:
                self._providers[product_type] = original_provider
