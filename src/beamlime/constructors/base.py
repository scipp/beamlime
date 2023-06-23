# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Callable, Dict, List, Tuple, TypeVar

from .inspectors import ProductType
from .providers import Provider, UnknownProvider

Product = TypeVar("Product")


class ProviderExistsError(Exception):
    ...


class MismatchingProductTypeError(Exception):
    ...


class ProviderNotFoundError(Exception):
    ...


def _has_conflicts(_overlapped: List[Any]):
    if _overlapped:
        _standard = _overlapped.pop()
        if any([_property for _property in _overlapped if _property != _standard]):
            return True
    return False


def _is_conflicting(tp: ProductType, factories: Tuple[FactoryBase, ...]):
    _providers = [factory.providers[tp] for factory in factories if tp in factory]

    _delayed_tasks = [
        factory.delayed_registers[tp]
        for factory in factories
        if tp in factory.delayed_registers
    ]

    return _has_conflicts(_providers) or _has_conflicts(_delayed_tasks)


def check_conflicting_factories(*factories: FactoryBase):
    from functools import reduce

    tp_set_list = [set(tp for tp in factory) for factory in factories]
    tp_set = reduce(lambda x, y: x.union(y), tp_set_list)

    # If there is any overlapping providers
    if sum([len(tp_set) for tp_set in tp_set_list]) > len(tp_set):
        conflicted = {tp for tp in tp_set if _is_conflicting(tp, factories)}
        # If there is any conflicting providers
        if any(conflicted):
            raise ProviderExistsError(f"There are conflicting factories {conflicted}")


class FactoryBase:
    """Magic methods and properties handling internal provider dictionary."""

    _providers: Dict[ProductType, Provider]
    _delayed_registers_todo: Dict[ProductType, Callable]
    _delayed_registers_done: Dict[ProductType, Callable]

    @property
    def providers(self) -> MappingProxyType[ProductType, Provider]:
        """Mapping proxy of providers."""
        return MappingProxyType(self._providers)

    @property
    def delayed_registers(self) -> MappingProxyType[ProductType, Provider]:
        """Mapping proxy of delayed register calls (todo or done)."""
        return MappingProxyType(
            {**self._delayed_registers_todo, **self._delayed_registers_done}
        )

    @property
    def catalogue(self) -> frozenset[ProductType]:
        """Frozen set of the product type that the factory can produce."""
        return frozenset(self.providers.keys())

    def find_provider(self, product_type: ProductType):
        if product_type not in self._providers:
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
        """Number of product type that the factory can produce."""
        return len(self._providers)

    def __merge__(self, *__others: FactoryBase):
        """
        Merge other factories after checking conflicts.
        ``providers`` and ``delayed_registers`` should not conflict.
        """
        check_conflicting_factories(self, *__others)
        for _factory in __others:
            for _product_type, _provider in _factory.providers.items():
                self._providers[_product_type] = _provider
            for _product_type, _delayed in _factory.delayed_registers.items():
                self._delayed_registers_todo[_product_type] = _delayed
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
