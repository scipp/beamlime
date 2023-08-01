# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Helper functions for parsing type hints and annotation of a callable object.

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, Literal, NewType, Type, TypeVar, Union


class Empty:
    ...


class InsufficientAnnotationError(Exception):
    ...


class ProductNotFoundError(Exception):
    ...


class UnknownType:
    ...


def validate_annotation(annotation: Any) -> Literal[True]:
    """
    Check if the origin of the annotation is not Union.

    If it for later implementation of Union type handling.
    """
    from typing import get_origin

    if get_origin(annotation) == Union:
        raise NotImplementedError("Union annotation is not supported.")
    return True


Product = TypeVar("Product")
_NewT = NewType("_NewT", int)


class NewTypeMeta(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        return (
            callable(instance)
            and hasattr(instance, "__supertype__")
            and _NewT.__module__ == instance.__module__
            and _NewT.__qualname__ == instance.__qualname__
        )


class NewTypeProtocol(Generic[Product], metaclass=NewTypeMeta):
    __supertype__: Type[Product]


def extract_underlying_product_type(product_type: Type[Product]) -> Type[Product]:
    if isinstance(product_type, NewTypeProtocol):
        return product_type.__supertype__
    else:
        return product_type


class ProductSpec:
    """
    Specification of a product (returned value) of a provider.
    """

    def __init__(self, product_type: Union[Type[Product], ProductSpec]) -> None:
        self.product_type: Type[Product]
        self.returned_type: Type[Product]

        if isinstance(product_type, ProductSpec):
            self.product_type = product_type.product_type
            self.returned_type = product_type.returned_type
        else:
            validate_annotation(product_type)
            self.product_type = product_type
            self.returned_type = extract_underlying_product_type(product_type)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProductSpec):
            raise NotImplementedError(
                "Comparison between ProductSpec and other type is not supported."
            )
        else:
            return self.product_type == other.product_type


def collect_arg_typehints(callable_obj: Callable[..., Product]) -> Dict[str, Type[Any]]:
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


def extract_underlying_dep_type(tp: Any) -> Any:
    from typing import Union, get_args, get_origin

    if get_origin(tp) == Union:
        if len((args := get_args(tp))) == 2 and type(None) in args:
            # Optional
            return args[0] if isinstance(None, args[1]) else args[1]
        else:
            raise NotImplementedError(
                "Union annotation for dependencies is not supported."
            )
    else:
        return tp


class DependencySpec:
    """
    Specification of sub-dependencies (arguments/attributes) of a provider.
    """

    def __init__(self, dependency_type: Any, default_value: Product) -> None:
        self.dependency_type = extract_underlying_dep_type(dependency_type)
        self.default_product = default_value

    def is_optional(self) -> bool:
        return self.default_product is not Empty or self.dependency_type is UnknownType


def collect_argument_specs(
    callable_obj: Callable[..., Product], *default_args: Any, **default_keywords: Any
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


def collect_attr_specs(
    callable_class: Callable[..., Product]
) -> Dict[str, DependencySpec]:
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
