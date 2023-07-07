# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import NewType

import pytest

from beamlime.constructors import ProviderGroup

from .preset_providers import orange_joke

decorating_provider_group = ProviderGroup()
UsefulString = NewType("UsefulString", str)
useful_info = UsefulString(orange_joke)


@decorating_provider_group.provider
def useful_function() -> UsefulString:
    return useful_info


def test_provider_function():
    assert decorating_provider_group[UsefulString].constructor is useful_function
    assert decorating_provider_group[UsefulString]() == useful_info


def test_provider_class():
    provider_gr = ProviderGroup()

    @provider_gr.provider
    class UsefulProduct:
        ...

    assert provider_gr[UsefulProduct].constructor is UsefulProduct
    assert isinstance(provider_gr[UsefulProduct](), UsefulProduct)


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
