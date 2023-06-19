# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from contextlib import contextmanager
from typing import Any, Iterator, Union

from .binders import Binder, GlobalBinder, ProviderExistsError
from .providers import Constructor, ProductType, Provider, UnknownProvider


class _BinderContext:
    _binder: Binder = GlobalBinder()  # Global binder by default.


def _validate_binders(*binders: Binder):
    from functools import reduce

    tp_sets = [set(tp for tp in binder) for binder in binders]
    flat_tp_sets = reduce(lambda x, y: x.union(y), tp_sets)
    if sum([len(tp_set) for tp_set in tp_sets]) > len(
        flat_tp_sets
    ):  # If there is any overlap of product type
        overlapped = {
            tp: const_set
            for tp in flat_tp_sets
            if len(
                (
                    const_set := set(
                        binder[tp].constructor for binder in binders if tp in binder
                    )
                )
            )
            > 1
        }
        if any(overlapped):
            raise ProviderExistsError("Binders have conflicting providers.", overlapped)


@contextmanager
def context_binder(*binders: Binder) -> Iterator[Binder]:
    """
    Yield a context binder configured by the argument ``binder`` or other decorators.
    If ``binders`` are given, the combined binder it will replace the current context
    and the original context will be restored at the end.
    If multiple binders are given, it checks if binders have
    conflicting providers(different providers of the same product type).
    If there are no conflicting providers,
    it copies all providers into the combined binder.

    If there is no context configured,
    the context binder is ``GlobalBinder()`` by default.
    """

    try:
        if binders:
            original_binder = _BinderContext._binder
            if len(binders) > 1:
                _validate_binders(*binders)
            tmp_binder = Binder()
            for binder in binders:
                for product_type in binder:
                    tmp_binder[product_type] = binder[product_type]
            _BinderContext._binder = tmp_binder
        else:
            original_binder = None

        yield _BinderContext._binder

    finally:
        if isinstance(original_binder, Binder):
            _BinderContext._binder = original_binder


@contextmanager
def global_binder() -> Iterator[Binder]:
    """
    Set the global binder as the current context binder.

    Restores the original context binder at the end.
    """
    try:
        original_binder = _BinderContext._binder
        _BinderContext._binder = GlobalBinder()
        yield _BinderContext._binder

    finally:
        _BinderContext._binder = original_binder


@contextmanager
def local_binder(*binders: Binder) -> Iterator[Binder]:
    """
    Create a new ``Binder`` object along with the copy
    of the providers in the original context,
    set it as the current context binder and yield it.

    If it is not under any other sub context,
    it will has the same providers as ``GlobalBinder``.

    The dictionary of the providers in the yielded binder object
    is independent from the original binder.
    Removing or setting different provider in the binder
    will not affect the original providers.

    However, direct change of each provider in the binder
    will affect the original ones since it is a shallow copy.

    Restores the original context binder at the end.
    """

    try:
        with context_binder() as original_binder:
            with context_binder(*(original_binder, *binders)) as local_binder:
                yield local_binder
    finally:
        ...


@contextmanager
def clean_binder() -> Iterator[Binder]:
    """
    Create an empty ``Binder``, set it as the current context binder and yield it.

    Restores the original context binder at the end.
    """
    try:
        with context_binder(Binder()) as empty_binder:
            yield empty_binder

    finally:
        ...


@contextmanager
def constant_provider(
    product_type: ProductType, hardcoded_value: Any
) -> Iterator[Binder]:
    """
    Use a lambda function that returns ``hardcoded_value``.
    as a temporary provider of ``product_type``.
    """
    try:
        with temporary_provider(product_type, lambda: hardcoded_value) as binder:
            yield binder
    finally:  # Restore the original provider
        ...


@contextmanager
def partial_provider(product_type: ProductType, **kwargs: Any) -> Iterator[Binder]:
    """
    Create a partial function from the deep copy of the provider for ``product_type``
    with keyword arguments in ``kwargs`` and use it as a temporary provider.

    To ensure that ``kwargs`` have the priority,
    the keys of the ``kwargs`` will be removed
    from the annotation of the copied function call.
    """
    from copy import deepcopy
    from functools import partial

    try:
        with context_binder() as original_binder:
            _partial = partial(
                deepcopy(original_binder[product_type].constructor), **kwargs
            )
            _partial.func.__annotations__ = {
                arg_name: arg_type
                for arg_name, arg_type in _partial.func.__annotations__.items()
                if arg_name not in kwargs
            }

        with temporary_provider(product_type, _partial) as binder:
            yield binder
    finally:
        ...


@contextmanager
def temporary_provider(
    product_type: ProductType, temp_provider: Union[Constructor, Provider]
) -> Iterator[Binder]:
    """
    Stash an original provider of the ``product_type`` and
    register partial function of the original provider
    as a temporary provider of ``product_type``.

    Remove the temporary provider and restore the original one at the end.
    """
    with context_binder() as binder:
        original_provider = binder.pop(product_type)
        try:
            binder[product_type] = temp_provider
            yield binder
        finally:  # Restore the original provider
            binder.pop(product_type)
            if original_provider is not UnknownProvider:
                binder[product_type] = original_provider
