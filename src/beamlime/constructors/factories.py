# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, List, Tuple, Type, TypeVar, Union

from .base import Product, ProviderExistsError
from .contexts import FactoryContextInterface
from .inspectors import ProductType
from .providers import Constructor, Provider

_Separator = TypeVar("_Separator")


def _split_by_type(
    _arguments: Union[Tuple, List], _type: Type[_Separator]
) -> Tuple[List[_Separator], List]:
    _correct_type = [_arg for _arg in _arguments if isinstance(_arg, _type)]
    _other_type = [_arg for _arg in _arguments if not isinstance(_arg, _type)]
    return _correct_type, _other_type


def merge(*factories: Factory):
    """Returns a merged factory."""
    factory = Factory()
    factory.__merge__(*factories)
    return factory


class ProviderPlaceHolderCalledError(Exception):
    ...


def raise_provider_placeholder_called_error():
    err_msg = (
        "Place holder provider is called. " "Delayed registration is not done yet."
    )
    raise ProviderPlaceHolderCalledError(err_msg)


provider_place_holder = Provider(raise_provider_placeholder_called_error)


class Factory(FactoryContextInterface):
    """
    Dependency injection object.

    Currently it does not inject dependencies for magic variables,
    i.e. *args and **kwargs.
    """

    def __init__(self, *__initials: Union[Constructor, Factory]) -> None:
        self._providers = dict()
        self._delayed_registers_todo = dict()
        self._delayed_registers_done = dict()

        if __initials:
            _factories, _non_factories = _split_by_type(__initials, Factory)
            __base_factory = merge(*_factories)
            for _provider in _non_factories:
                __base_factory.provider(_provider)
            self.__merge__(__base_factory)
        else:
            ...

    def __getitem__(self, product_type: Type[Product]) -> Product:
        """
        Retrieve an object created by provider of the ``product type``.
        """
        while self._delayed_registers_todo:
            _delayed_tp, _register = self._delayed_registers_todo.popitem()
            self._delayed_registers_done[_delayed_tp] = _register
            _register(self)

        try:
            return self._assemble(product_type)
        except RecursionError:
            raise RecursionError(f"Failed due to circular dependency of {product_type}")

    @contextmanager
    def local_factory(self, *factories: Factory) -> Iterator[Factory]:
        """
        Create a new factory that has copy of all ``self._providers`` and yield it.
        For existing product type, local factory will overwrite the provider.
        """

        tmp_factory = merge(*factories)
        for _product_type in self.providers:
            if _product_type not in tmp_factory.providers:
                _shared_provider = self.providers[_product_type]
                tmp_factory.register(_product_type, _shared_provider)
        try:
            yield tmp_factory
        finally:
            ...

    def __add__(self, __other: object):
        """
        Return a new factory containing all providers two factories have.
        """
        if not isinstance(__other, Factory):
            raise NotImplementedError(
                "+ operation between Factory and other type is not supported."
            )
        return Factory(self, __other)

    def cache_product(
        self, product_type: ProductType, provider_call: Constructor
    ) -> None:
        """
        Assemble the product and return the same object every time it is requested.

        The provider place holder will be registered and
        the real provider call will be registered at the beginning of ``__getitem__``.
        After the delayed call is run, it will be removed from the ``todo`` list
        and saved into ``done`` list.

        When the factory is merged to the other one,
        the delayed call will be moved to ``todo`` list again,
        and it will be registered at the new factory upon the ``__getitem__`` called.

        It means the product is only cached within the instance of ``Factory``.
        """
        with self.temporary_provider(product_type, provider_call):
            ...  # Check if the provider is valid for the product type.

        self.register(product_type, provider_place_holder)

        def delayed_register(_factory: Factory):
            if self.find_provider(product_type) == provider_place_holder:
                with _factory.temporary_provider(product_type, provider_call):
                    _singleton_product = _factory[product_type]

                _factory.pop(product_type)
                _factory.register(product_type, lambda: _singleton_product)
            else:
                raise ProviderExistsError(
                    f"Delayed registration of {product_type} is not done "
                    "due to existing provider."
                )

        self._delayed_registers_todo[product_type] = delayed_register
