# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Helper functions for parsing type hints and annotation of a callable object.

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, Literal, NewType, TypeVar


class Empty: ...


class InsufficientAnnotationError(Exception): ...


class ProductNotFoundError(Exception): ...


class UnknownType: ...


def validate_annotation(annotation: Any) -> Literal[True]:
    """
    Check if the origin of the annotation is not Union.

    If it for later implementation of Union type handling.
    """
    from types import UnionType
    from typing import Union, get_origin

    if get_origin(annotation) == Union or isinstance(annotation, UnionType):
        raise NotImplementedError("Union annotation is not supported.")
    return True


Product = TypeVar("Product")


def extract_underlying_type(product_type: type[Product]) -> type[Product]:
    if isinstance(product_type, NewType):
        return extract_underlying_type(product_type.__supertype__)
    else:
        return product_type


class ProductSpec:
    """
    Specification of a product (returned value) of a provider.
    """

    def __init__(self, product_type: type[Product] | ProductSpec) -> None:
        self.product_type: type[Product]
        self.returned_type: type[Product]

        if isinstance(product_type, ProductSpec):
            self.product_type = product_type.product_type
            self.returned_type = product_type.returned_type
        else:
            validate_annotation(product_type)
            self.product_type = product_type
            self.returned_type = extract_underlying_type(product_type)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProductSpec):
            raise NotImplementedError(
                "Comparison between ProductSpec and other type is not supported."
            )
        else:
            return self.product_type == other.product_type


def collect_arg_typehints(callable_obj: Callable[..., Product]) -> dict[str, type[Any]]:
    from typing import get_type_hints

    if isinstance(callable_obj, type):
        return get_type_hints(callable_obj.__init__)
    else:
        return get_type_hints(callable_obj)


def get_product_spec(callable_obj: Callable[..., Product]) -> ProductSpec:
    """
    Retrieve the product of provider.
    If ``callable_obj`` is a function, it is a return type annotation,
    and if ``callable_obj`` is a class, it is the class itself.
    """
    if isinstance(callable_obj, type):
        return ProductSpec(callable_obj)
    else:
        product = collect_arg_typehints(callable_obj).get("return", UnknownType)
        return ProductSpec(product)


class DependencySpec(Generic[Product]):
    """
    Specification of sub-dependencies (arguments/attributes) of a provider.
    """

    def __init__(self, dependency_type: Any, default_value: Product) -> None:
        self.dependency_type = self.extract_dependency_type(dependency_type)
        self.runtime_type = extract_underlying_type(dependency_type)
        self.default_product = default_value

    @staticmethod
    def extract_dependency_type(dependency_type: Any) -> None:
        from types import UnionType
        from typing import Union, get_args, get_origin

        if get_origin(dependency_type) == Union or isinstance(
            dependency_type, UnionType
        ):
            if len(args := get_args(dependency_type)) == 2 and type(None) in args:
                # Optional
                return args[0] if isinstance(None, args[1]) else args[1]
            else:
                raise NotImplementedError(
                    "Union annotation for dependencies "
                    "is not supported except for Optional."
                )

        return dependency_type

    def is_optional(self) -> bool:
        return self.default_product is not Empty or self.dependency_type is UnknownType

    def __repr__(self) -> str:
        def trim_repr(str_repr: str, max_len: int = 88) -> str:
            return str_repr if len(str_repr) <= max_len else str_repr[:max_len] + "..."

        default_repr = str(None) if (obj := self.default_product) is Empty else str(obj)
        return (
            f"{self.__class__.__name__}("
            f"dependency_type: '{self.dependency_type.__name__}', "
            f"runtime_type: '{self.runtime_type}', "
            f"default_value: '{trim_repr(default_repr)}')"
        )


def collect_argument_specs(
    callable_obj: Callable[..., Product], *default_args: Any, **default_keywords: Any
) -> dict[str, DependencySpec]:
    """
    Collect Dependencies from the signature and type hints.
    ``default_args`` and ``default_keywords`` will overwrite the annotation.
    """
    from inspect import signature

    type_hints = collect_arg_typehints(callable_obj)

    try:
        _sig = signature(callable_obj)
    except ValueError:  # if signature is not found
        return {}

    arg_params = _sig.parameters
    partial_args = _sig.bind_partial(*default_args, **default_keywords)
    partial_args.apply_defaults()
    defaults = partial_args.arguments

    missing_params = [
        param_name
        for param_name in set(arg_params) - set(defaults)
        if ((param_name not in ("args", "kwargs")) and (param_name not in type_hints))
    ]
    if missing_params:
        raise InsufficientAnnotationError(
            f"Annotations for {missing_params} are not sufficient."
            "Each argument needs a type hint or a default value."
        )

    return {
        param_name: DependencySpec(
            type_hints.get(param_name, UnknownType),
            defaults.get(param_name, Empty),
        )
        for param_name in arg_params
    }


def collect_attr_specs(
    callable_class: Callable[..., Product],
) -> dict[str, DependencySpec]:
    """
    Collect Dependencies from the type hints of the class.
    It ignores any attributes without type hints.
    """
    from typing import get_type_hints

    if not isinstance(callable_class, type):
        return {}
    else:
        return {
            attr_name: DependencySpec(
                attr_type, getattr(callable_class, attr_name, Empty)
            )
            for attr_name, attr_type in get_type_hints(callable_class).items()
        }
