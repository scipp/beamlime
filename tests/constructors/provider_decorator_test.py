# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import NewType

import pytest

from beamlime.constructors import Factory, ProviderGroup
from beamlime.constructors.providers import Provider, SingletonProvider

from .preset_providers import orange_joke

decorating_provider_group = ProviderGroup()
UsefulString = NewType("UsefulString", str)
SingletonList = NewType("SingletonList", list)
useful_info = UsefulString(orange_joke)


@decorating_provider_group.provider
def useful_function() -> UsefulString:
    return useful_info


@decorating_provider_group.provider(provider_type=SingletonProvider)
def singleton_provider() -> SingletonList:
    return SingletonList(list(orange_joke))


def test_provider_function():
    assert decorating_provider_group[UsefulString].constructor is useful_function
    assert decorating_provider_group[UsefulString]() == useful_info


def test_singleton_provider_function():
    assert (
        decorating_provider_group[SingletonList]
        == decorating_provider_group[SingletonList]
    )
    assert (
        decorating_provider_group[SingletonList]
        is decorating_provider_group[SingletonList]
    )


def test_provider_class():
    provider_gr = ProviderGroup()

    @provider_gr.provider
    class UsefulProduct:
        ...

    assert provider_gr[UsefulProduct].constructor is UsefulProduct
    assert isinstance(provider_gr[UsefulProduct](), UsefulProduct)


def test_provider_singleton_class():
    provider_gr = ProviderGroup()

    @provider_gr.provider(provider_type=SingletonProvider)
    class UsefulProduct:
        ...

    assert provider_gr[UsefulProduct].constructor is UsefulProduct
    assert provider_gr[UsefulProduct]() is provider_gr[UsefulProduct]()
    assert isinstance(provider_gr[UsefulProduct](), UsefulProduct)


def test_provider_singleton_class_as_dependency():
    provider_gr = ProviderGroup()

    @provider_gr.provider(provider_type=SingletonProvider)
    class SingletonList(list):
        ...

    @provider_gr.provider(provider_type=Provider)
    class App1:
        sink: SingletonList

    @provider_gr.provider
    class App2:
        sink: SingletonList

    factory = Factory(provider_gr)
    app1 = factory[App1]
    app2 = factory[App2]
    assert app1 is not factory[App1]
    assert app1.sink is app2.sink


def test_provider_method_raises():
    provider_gr = ProviderGroup()
    with pytest.raises(NotImplementedError):

        class _:
            @provider_gr.provider
            def __(self):
                ...


def test_provider_local_function_raises():
    provider_gr = ProviderGroup()
    with pytest.raises(NotImplementedError):

        @provider_gr.provider
        def _() -> None:
            return None
