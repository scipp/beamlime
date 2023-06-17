# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest
from beamlime.constructors import Providers, local_providers


def test_provider_singleton():
    from beamlime.constructors import get_providers
    from beamlime.constructors.providers import _Providers
    with local_providers():
        assert Providers is get_providers()
        assert Providers is _Providers()

def test_provider_not_exist_rasies():
    from beamlime.constructors import ProviderNotFoundError
    with local_providers():
        from .preset_providers import Joke
        with pytest.raises(ProviderNotFoundError):
            Providers[Joke]

def test_provider_already_exists_raises():
    from beamlime.constructors import ProviderExistsError
    with local_providers():
        from .preset_providers import Joke, make_another_joke, make_a_joke
        Providers[Joke] = make_a_joke
    
        with pytest.raises(ProviderExistsError):
            Providers[Joke] = make_another_joke

def test_provider_already_exists_without_name_rasies():
    from beamlime.constructors import ProviderExistsError
    with local_providers():
        Providers[None] = lambda: None
        with pytest.raises(ProviderExistsError):
            Providers[None] = lambda: None

def test_provider_function_call():
    with local_providers():
        from .preset_providers import Joke, make_a_joke
        Providers[Joke] = make_a_joke
        assert Providers[Joke].constructor == make_a_joke
        assert Providers[Joke]() == make_a_joke()

def test_provider_class():
    with local_providers():
        from .preset_providers import Parent
        assert Providers[Parent].constructor == Parent

def test_provider_incomplete_class_arguments_rasies():
    from beamlime.constructors import ProviderNotFoundError
    with local_providers():
        from .preset_providers import Parent
        with pytest.raises(ProviderNotFoundError):
            Providers[Parent]()

def test_provider_incomplete_class_attributes_rasies():
    from beamlime.constructors import ProviderNotFoundError
    class IncompleteClass:
        attribute: None

    with local_providers():
        Providers[IncompleteClass] = IncompleteClass
        Providers[IncompleteClass] # Provider of incomplete class exists.
        with pytest.raises(ProviderNotFoundError):
            Providers[IncompleteClass]() # Provider of attribute does not exist.

def test_union_annotation_raises():
    from beamlime.constructors import temporary_provider
    from typing import Union
    def confusing_func(_: Union[float, int]) -> Union[float, int]:
        return _
    
    with pytest.raises(NotImplementedError):
        with temporary_provider(int, confusing_func):
            ...

def test_insufficient_annotation_raises():
    from beamlime.constructors import temporary_provider, InsufficientAnnotationError
    def confusing_func(_) -> int:
        return _
    
    with pytest.raises(InsufficientAnnotationError):
        with temporary_provider(int, confusing_func):
            Providers[int]()
