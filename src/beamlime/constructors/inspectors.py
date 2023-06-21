# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Helper functions for parsing type hints and annotation of a callable object.

from __future__ import annotations

from typing import Any, Callable, Dict, Literal, Type, TypeVar, Union


class InsufficientAnnotationError(Exception):
    ...


class ProductNotFoundError(Exception):
    ...


class UnknownType:
    ...


def validate_annotation(annotation) -> Literal[True]:
    """
    Check if the origin of the annotation is not Union.

    If it for later implementation of Union type handling.
    """
    from typing import get_origin

    if get_origin(annotation) == Union:
        raise NotImplementedError("Union annotation is not supported yet.")
    return True


Product = TypeVar("Product")
ProductType = Type[Product]


def extract_underlying_type(product_type: ProductType) -> Any:
    if hasattr(product_type, "__supertype__"):  # NewType
        return product_type.__supertype__
    else:
        return product_type


class ProductSpec:
    """
    Specification of a product (returned value) of a provider.
    """

    def __new__(cls, product_type: Union[ProductType, ProductSpec]) -> ProductSpec:
        if isinstance(product_type, ProductSpec):
            return product_type
        else:
            return super().__new__(ProductSpec)

    def __init__(self, product_type: Union[ProductType, ProductSpec]) -> None:
        if isinstance(product_type, ProductSpec):
            ...
        else:
            self.product_type = product_type
            self.returned_type = extract_underlying_type(product_type)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, ProductSpec):
            raise NotImplementedError(
                "Comparison between ProductSpec " "and other type is not supported."
            )
        else:
            return self.product_type == __value.product_type


def collect_arg_typehints(callable_obj: Callable) -> Dict[str, Any]:
    from typing import get_type_hints

    if isinstance(callable_obj, type):
        return get_type_hints(callable_obj.__init__)
    else:
        return get_type_hints(callable_obj)


def get_product_spec(callable_obj: Callable) -> ProductSpec:
    """
    Retrieve the product of provider.
    If ``callable_obj`` is a function, it is a return type annotation,
    and if ``callable_obj`` is a class, it is the class itself.
    """
    if isinstance(callable_obj, type):
        return ProductSpec(callable_obj)
    else:
        product = collect_arg_typehints(callable_obj).get("return", UnknownType)
        validate_annotation(product)
        return ProductSpec(product)


class DependencySpec:  # TODO: Can be written in dataclass when mypy problem is resolved
    """
    Specification of sub-dependencies (arguments/attributes) of a provider.
    """

    def __init__(self, product_type: ProductType, default_value: Product) -> None:
        self.product_type = product_type
        self.default_product = default_value


def collect_argument_dep_specs(callable_obj: Callable) -> Dict[str, DependencySpec]:
    """
    Collect Dependencies from the signature and type hints.
    """
    from inspect import signature

    arg_params = signature(callable_obj).parameters
    type_hints = collect_arg_typehints(callable_obj)
    missing_params = [
        param_name
        for param_name in arg_params
        if ((param_name not in ("args", "kwargs")) and (param_name not in type_hints))
    ]
    if missing_params:
        raise InsufficientAnnotationError(
            f"Annotations for {missing_params} are not sufficient."
            "Each argument needs a type hint or a default value."
        )
    return {
        param_name: DependencySpec(
            type_hints.get(param_name, UnknownType), param_spec.default
        )
        for param_name, param_spec in arg_params.items()
    }
