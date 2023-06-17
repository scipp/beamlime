from contextlib import contextmanager
from typing import Any, Iterator, Union

from .providers import Constructor, ProductType, ProviderCall, _Providers


@contextmanager
def local_providers() -> Iterator[_Providers]:
    """
    Keep the copy of the original providers in Providers object,
    and restore the original providers at the end of the context.

    It also keeps the list of imported modules and restore it at the end.
    It is to make sure @provider decorator is executed as a module is imported.

    The Providers will still contain the global providers as well and
    the imported modules will be available in the context.
    """
    import sys
    from copy import copy

    from .providers import get_providers

    Providers = get_providers()
    original_providers = copy(Providers._providers)  # Doesn't need deep copy.
    original_modules = list(sys.modules.keys())
    try:
        yield Providers
    finally:  # Restore original providers and sys.modules.
        imported_modules = [
            module_name
            for module_name in sys.modules
            if module_name not in original_modules
        ]
        for extra_module_name in imported_modules:
            del sys.modules[extra_module_name]
        Providers._providers = original_providers


@contextmanager
def constant_provider(product_type: ProductType, hardcoded_value: Any):
    """
    Use a lambda function that returns ``hardcoded_value``.
    as a temporary provider of ``product_type``.
    """
    try:
        with temporary_provider(product_type, lambda: hardcoded_value):
            yield None
    finally:  # Restore the original provider
        ...


@contextmanager
def partial_provider(product_type: ProductType, **kwargs: Any):
    """
    Create a partial function from the deep copy of the provider for ``product_type``
    with keyword arguments in ``kwargs`` and use it as a temporary provider.

    To ensure that ``kwargs`` have the priority,
    the keys of the ``kwargs`` will be removed
    from the annotation of the copied function call.
    """
    from copy import deepcopy
    from functools import partial

    partial_constructor = partial(
        deepcopy(_Providers()[product_type].constructor), **kwargs
    )

    partial_constructor.func.__annotations__ = {
        arg_name: arg_type
        for arg_name, arg_type in partial_constructor.func.__annotations__.items()
        if arg_name not in kwargs
    }

    try:
        with temporary_provider(product_type, partial_constructor):
            yield None
    finally:
        ...


@contextmanager
def temporary_provider(
    product_type: ProductType, temp_provider: Union[Constructor, ProviderCall]
):
    """
    Stash an original provider of the ``product_type`` and
    register partial function of the original provider
    as a temporary provider of ``product_type``.

    Remove the temporary provider and restore the original one at the end.
    """
    _providers = _Providers()
    original_provider = _providers.pop(product_type)

    try:
        _providers[product_type] = temp_provider
        yield None
    finally:  # Restore the original provider
        _providers.pop(product_type)
        if original_provider:
            _providers[product_type] = original_provider
