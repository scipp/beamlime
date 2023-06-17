# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Optional, Type

from .inspectors import Product


class _Container:
    """
    Container class that holds provider dictionary and product history.
    This class is used as a singleton so it should only have classmethods.

    After a provider function is registered into Providers,
    you can retrieve the object by the type of the object.

    TODO: Check if there is any circular dependencies.

    Examples
    --------
    >>> from typing import Literal
    >>> from beamlime.constructors import provider, Container
    >>> PiType = Literal[3.14]
    >>>
    >>> @provider
    ... def provide_pi() -> PiType:
    ...   return 3.14
    ...
    >>> Container[PiType]
    3.14
    """

    _instance: Optional[_Container] = None

    def __new__(cls) -> _Container:
        if not cls._instance:
            cls._instance = super().__new__(_Container)
            return cls._instance
        else:
            return cls._instance

    def __getitem__(self, product_type: Type[Product]) -> Product:
        """
        Find a provider call of the type ``product_type``
        and return the result of provider call.
        """
        from . import get_providers

        Providers = get_providers()
        return Providers[product_type]()


def get_container() -> _Container:
    """Returns a singleton object of _Container."""
    return _Container()
