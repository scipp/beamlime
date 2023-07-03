# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from functools import partial
from types import FunctionType, LambdaType
from typing import Callable, Dict, Literal, TypeVar, Union

from .inspectors import DependencySpec, ProductSpec, ProductType, collect_argument_specs

_Product = TypeVar("_Product")
_lambda_name = (lambda: None).__qualname__


def _validate_callable_as_provider(callable_obj: Constructor) -> Literal[True]:
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
    return True


class Provider:
    """
    Function wrapper that provides certain type of product.

    Nested ``Provider`` is avoided similar to ``partial``.

    """

    _constructor: Constructor
    args: tuple
    keywords: dict
    product_spec: ProductSpec
    arg_dep_specs: Dict[str, DependencySpec]

    def __init__(
        self, _constructor: Union[Constructor, Provider], /, *args, **kwargs
    ) -> None:
        from .inspectors import get_product_spec

        if isinstance(_constructor, (partial, Provider)):
            if isinstance(_constructor, Provider):
                self._constructor = _constructor.constructor
            elif isinstance(_constructor, partial):
                self._constructor = _constructor.func

            self.args = (*_constructor.args, *args)
            self.keywords = {**_constructor.keywords, **kwargs}
        else:
            self._constructor = _constructor
            self.args = args
            self.keywords = kwargs

        _validate_callable_as_provider(self._constructor)
        self.arg_dep_specs = collect_argument_specs(
            self._constructor, *self.args, **self.keywords
        )
        self.product_spec = get_product_spec(self._constructor)

    @property
    def call_name(self) -> str:
        return ".".join((self.constructor.__module__, self.constructor.__qualname__))

    @property
    def constructor(self) -> Constructor:
        return self._constructor

    def can_provide(self, product_type: Union[ProductType, ProductSpec]):
        """
        Check if the given ``product_type`` can be supported by this provider.
        Product type should be the same type as the returned type of this provider.
        If they are not the same types,
        the requested product type should be the parent class
        of returned type of this provider.
        """
        from typing import Any, get_origin

        from .inspectors import UnknownType

        requested_tp = ProductSpec(product_type).returned_type
        provided_tp = self.product_spec.returned_type

        if provided_tp in (None, Any, UnknownType):
            return True

        try:
            if requested_tp == provided_tp or issubclass(provided_tp, requested_tp):
                return True
        except TypeError:
            ...

        try:
            return issubclass(provided_tp, get_origin(requested_tp))
        except TypeError:
            return False

    def __call__(self, *args, **kwargs) -> _Product:
        return self.constructor(*(*self.args, *args), **{**self.keywords, **kwargs})

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Provider):
            raise NotImplementedError(
                "Comparison between Provider" " with other type is not supported."
            )
        else:
            return self.constructor is other.constructor


def unknown_provider_call():
    from ..constructors import ProviderNotFoundError

    raise ProviderNotFoundError("Unknown provider is called.")


UnknownProvider = Provider(unknown_provider_call)
Constructor = Union[LambdaType, partial, Callable, FunctionType, type, Provider]
