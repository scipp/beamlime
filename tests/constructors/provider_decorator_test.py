# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest
from beamlime.constructors import provider, Providers
from .preset_providers import GoodTelling, sweet_reminder, Parent

@provider
def give_me_a_good_telling() -> GoodTelling:
    return GoodTelling(sweet_reminder)

def test_provider_function_call():
    assert Providers[GoodTelling].constructor == give_me_a_good_telling
    assert Providers[GoodTelling]() == give_me_a_good_telling()
    assert Providers[GoodTelling]() == sweet_reminder

def test_provider_incomplete_class_raises():
    from beamlime.constructors import ProviderNotFoundError
    with pytest.raises(ProviderNotFoundError):
        Providers[Parent]()

def test_provider_member_class_raises():    
    with pytest.raises(NotImplementedError):
        @provider
        class _:
            ...

def test_provider_member_function_raises():
    with pytest.raises(NotImplementedError):
        class _:
            @provider
            def __(self):
                ...

def test_provider_local_function_raises():
    with pytest.raises(NotImplementedError):
        @provider
        def _() -> GoodTelling:
            return GoodTelling("_")
