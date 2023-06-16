# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest
from typing import NewType
from beamlime.constructors import provider, Providers

GoodTelling = NewType("GoodTelling", str)
Joke = NewType("Joke", str)
sweet_reminder = "Drink some water!"

def make_a_joke(joke: str = "What cars do rich limes ride?\n\n\n\nA lime-o") -> Joke:
    return Joke(joke)

Providers[Joke] = make_a_joke

def make_another_joke(joke: str = "Orange... you glad that I didn't say orange?") -> Joke:
    return Joke(joke)


@provider
class Parent:
    good_telling: GoodTelling
    joke: Joke

    def give_a_good_telling(self) -> GoodTelling:
        return self.good_telling
    
    def make_a_joke(self) -> Joke:
        return self.joke

def test_provider_singleton():
    from beamlime.constructors import get_providers
    from beamlime.constructors.providers import _Providers
    assert Providers is get_providers()
    assert Providers is _Providers()

def test_provider_not_exist_rasies():
    from beamlime.constructors import ProviderNotFoundError
    with pytest.raises(ProviderNotFoundError):
        Providers[GoodTelling]

def test_provider_already_exists_raises():
    from beamlime.constructors import ProviderExistsError
    with pytest.raises(ProviderExistsError):
        Providers[Joke] = lambda: Joke("I don't know the third.")
    
    with pytest.raises(ProviderExistsError):
        Providers[Joke] = make_another_joke

def test_provider_already_exists_without_name_rasies():
    from beamlime.constructors import ProviderExistsError
    Providers[None] = lambda: None
    with pytest.raises(ProviderExistsError):
        Providers[None] = lambda: None

def test_provider_function_call():
    assert Providers[Joke].constructor == make_a_joke
    assert Providers[Joke]() == make_a_joke()

def test_provider_class():
    assert Providers[Parent].constructor == Parent

def test_provider_incomplete_class_rasies():
    from beamlime.constructors import ProviderNotFoundError
    with pytest.raises(ProviderNotFoundError):
        Providers[Parent]()

def test_provider_class():
    assert Providers[Parent].constructor == Parent


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
