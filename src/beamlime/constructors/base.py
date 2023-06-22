# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Tuple, TypeVar

from .generics import GenericProvider
from .inspectors import ProductType
from .providers import Provider, UnknownProvider

_Product = TypeVar("_Product")


class ProviderExistsError(Exception):
    ...


class MismatchingProductTypeError(Exception):
    ...


class ProviderNotFoundError(Exception):
    ...


def _is_conflicting(tp: ProductType, factories: Tuple[FactoryBase, ...]):
    _providers = [factory[tp] for factory in factories if tp in factory]
    if _providers:
        _standard = _providers.pop()
        if any([_provider for _provider in _providers if _provider != _standard]):
            return True
    return False


def check_conflicting_providers(*factories: FactoryBase):
    from functools import reduce

    tp_set_list = [set(tp for tp in factory) for factory in factories]
    tp_set = reduce(lambda x, y: x.union(y), tp_set_list)

    # If there is any overlapping providers
    if sum([len(tp_set) for tp_set in tp_set_list]) > len(tp_set):
        conflicted = {tp for tp in tp_set if _is_conflicting(tp, factories)}
        # If there is any conflicting providers
        if any(conflicted):
            raise ProviderExistsError(
                "Binders have conflicting providers" f" {conflicted}"
            )


class FactoryBase:
    """Magic methods and properties handling internal provider dictionary."""

    _providers: Dict[ProductType, Provider]

    @property
    def providers(self) -> MappingProxyType[ProductType, Provider]:
        return MappingProxyType(self._providers)

    @property
    def catalogue(self) -> frozenset[ProductType]:
        return frozenset(self.providers.keys())

    def create_generic_provider(self, generic_tp: type):
        """
        Find a ``GenericProvider`` of origin of ``generic_tp``
        and create a provider with a partial function
        from the ``GenericProvider`` constructor.

        The constructor(provider) of origin of ``generic_tp``
        should take ``generic_tp`` as the first positional argument.

        The origin of the ``generic_tp`` should be a subclass of ``GenericProvider``.

        Returns
        -------
            True:
                It will register the new provider and return True.

            False:
                If it can not create the provider of ``generic_tp``.

        """
        from typing import get_origin

        if (
            (origin := get_origin(generic_tp)) is not None
            and origin in self._providers
            and issubclass(origin, GenericProvider)
        ):
            _constructor = Provider(self._providers[origin], generic_tp)
            self.register(generic_tp, _constructor)
            return True
        else:
            return False

    def find_provider(self, product_type: ProductType):
        if product_type not in self._providers and not self.create_generic_provider(
            product_type
        ):
            product_label = (
                product_type.__name__
                if hasattr(product_type, "__name__")
                else product_type
            )
            raise ProviderNotFoundError(f"Provider for ``{product_label}`` not found.")
        return self._providers[product_type]

    def __iter__(self):
        return iter(self._providers)

    def __len__(self):
        return len(self._providers)

    def __merge__(self, *__others: FactoryBase):
        """Merge providers after checking overlap."""
        check_conflicting_providers(self, *__others)
        for _factory in __others:
            for _product_type, _provider in _factory.providers.items():
                self._providers[_product_type] = _provider
        return self

    def __iadd__(self, __other: object):
        """
        Merge other factory into this factory.
        """
        if not isinstance(__other, FactoryBase):
            raise NotImplementedError(
                "+= operation between Factory and other type is not supported."
            )
        return self.__merge__(__other)

    def pop(self, product_type: ProductType) -> Provider:
        """
        Remove and return the provider of ``product_type``.
        Returns ``UnknownProvider`` if not found.
        """
        return self._providers.pop(product_type, UnknownProvider)

    def clear(self) -> None:
        """Clear Providers."""
        self._providers.clear()
