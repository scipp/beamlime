from __future__ import annotations

from inspect import Signature
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
    """
    from typing import get_origin

    if get_origin(annotation) == Union:
        raise NotImplementedError("Union annotation is not supported yet.")
    return True


def collect_arg_typehints(callable_obj: Callable) -> Dict[str, Any]:
    from functools import partial
    from typing import get_type_hints

    if isinstance(callable_obj, type):
        return get_type_hints(callable_obj.__init__)  # type: ignore[misc]
    elif isinstance(callable_obj, partial):
        return get_type_hints(callable_obj.func)
    else:
        return get_type_hints(callable_obj)


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


def issubproduct(_subproduct: ProductSpec, _baseproduct: ProductSpec) -> bool:
    """
    Check if the ``_subproduct`` is a sub class of the ``_baseproduct``.
    """
    if _baseproduct.returned_type in (None, Any, UnknownType):
        return True
    try:
        return _subproduct.returned_type == _baseproduct.returned_type or issubclass(
            _subproduct.returned_type, _baseproduct.returned_type
        )
    except TypeError:
        return False


class DependencySpec:  # TODO: Can be written in dataclass when mypy problem is resolved
    """
    Specification of sub-dependencies (arguments/attributes) of a provider.
    """

    def __init__(self, product_type: ProductType, default_value: Product) -> None:
        self.product_type = product_type
        self.default_product = default_value


def collect_argument_dep_specs(callable_obj: Callable) -> Dict[str, DependencySpec]:
    """
    Collect Dependencies from the signuatre and type hints.
    """
    from inspect import signature

    arg_params = signature(callable_obj).parameters
    type_hints = collect_arg_typehints(callable_obj)
    if any(
        (
            missing_params := [
                param_name
                for param_name, param_spec in arg_params.items()
                if (
                    param_name not in type_hints
                    and param_spec.default == Signature.empty
                )
            ]
        )
    ):
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
