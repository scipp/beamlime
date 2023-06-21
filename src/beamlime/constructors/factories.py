# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, List, Tuple, Type, TypeVar, Union

from .base import _Product
from .contexts import FactoryContextInterface
from .providers import Constructor

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


class Factory(FactoryContextInterface):
    """
    Dependency injection object.

    Currently it does not inject dependencies for magic variables,
    i.e. *args and **kwargs.
    """

    def __init__(self, *__initials: Union[Constructor, Factory]) -> None:
        self._providers = dict()

        if __initials:
            _factories, _non_factories = _split_by_type(__initials, Factory)
            __base_factory = merge(*_factories)
            for _provider in _non_factories:
                __base_factory.provider(_provider)
            self.__merge__(__base_factory)
        else:
            ...

    def __getitem__(self, product_type: Type[_Product]) -> _Product:
        """
        Retrieve an object created by provider of the ``product type``.
        """
        return self.assemble(product_type)

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
