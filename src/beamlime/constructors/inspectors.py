# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Helper functions for parsing type hints and annotation of a callable object.

from __future__ import annotations

from typing import Any, Callable, Dict, Literal, Type, TypeVar, Union


class Empty:
    ...


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
        raise NotImplementedError(
            "Union annotation except for ``Optional`` " "is not supported yet."
        )
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
            validate_annotation(product_type)
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
        # TODO: When the callable_obj is a class,
        # investigating ``__init__`` of the class should be okay
        # but may replace this to the better solution and remove type check ignore tag.
        return get_type_hints(callable_obj.__init__)  # type: ignore[misc]
    else:
        return get_type_hints(callable_obj)


def get_product_spec(callable_obj: Callable) -> ProductSpec:
    """
    Retrieve the product of provider.
    If ``callable_obj`` is a function, it is a return type annotation,
    and if ``callable_obj`` is a class, it is the class itself.
    """
    from functools import partial

    if isinstance(callable_obj, type):
        return ProductSpec(callable_obj)
    elif isinstance(callable_obj, partial):
        return get_product_spec(callable_obj.func)
    else:
        product = collect_arg_typehints(callable_obj).get("return", UnknownType)
        return ProductSpec(product)


class DependencySpec:
    """
    Specification of sub-dependencies (arguments/attributes) of a provider.
    """

    def __init__(self, dependency_type: ProductType, default_value: Product) -> None:
        try:
            validate_annotation(dependency_type)
        except NotImplementedError as err:
            from typing import get_args

            if len((args := get_args(dependency_type))) == 2 and type(None) in args:
                # Allow ``Optional``.
                self.dependency_type = (
                    args[0] if isinstance(args[1], type(None)) else args[1]
                )
            else:
                raise err
        else:
            self.dependency_type = dependency_type
        finally:
            self.default_product = default_value

    def is_optional(self):
        return self.default_product is not Empty or self.dependency_type is UnknownType


def collect_argument_specs(
    callable_obj: Callable, *default_args, **default_keywords
) -> Dict[str, DependencySpec]:
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


def collect_attr_specs(callable_class: Callable) -> Dict[str, DependencySpec]:
    """
    Collect Dependencies from the type hints of the class.
    It ignores any attributes without type hints.
    """
    from typing import get_type_hints

    if not isinstance(callable_class, type):
        return dict()
    else:
        return {
            attr_name: DependencySpec(
                attr_type, getattr(callable_class, attr_name, Empty)
            )
            for attr_name, attr_type in get_type_hints(callable_class).items()
        }
