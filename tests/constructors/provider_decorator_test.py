# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest
from beamlime.constructors import provider, Providers, local_providers

def test_provider_function_call():
    with local_providers():
        from .preset_providers import give_a_good_telling, GoodTelling
        assert Providers[GoodTelling].constructor == give_a_good_telling
        assert Providers[GoodTelling]() == give_a_good_telling()

def test_provider_incomplete_class_raises():
    with local_providers():
        from .preset_providers import Parent
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
        def _() -> None:
            return None
