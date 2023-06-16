# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest
from typing import NewType
from beamlime.constructors import Providers
from .preset_providers import GoodTelling, sweet_reminder, lime_joke, Joke, Parent

def test_partial_provider():
    from beamlime.constructors import partial_provider
    with partial_provider(Parent, joke=Joke(lime_joke)):
        _parent: Parent = Providers[Parent]()
        assert _parent.give_a_good_telling() == sweet_reminder
        assert _parent.make_a_joke() == lime_joke
    
    # partial provider should be destroyed.
    from beamlime.constructors import ProviderNotFoundError
    with pytest.raises(ProviderNotFoundError):
        Providers[Joke]

def test_partial_of_non_existing_provider_raises():
    """You can only create a partial provider from the existing one."""
    from beamlime.constructors import partial_provider, ProviderNotFoundError
    with pytest.raises(ProviderNotFoundError):
        with partial_provider(Joke):
            ...

def test_constant_provider():
    from beamlime.constructors import constant_provider, ProviderNotFoundError
    another_lime_joke = "Why are limes so observant?"\
                        "\n\n\n"\
                        "They're full of Vitamin See."
    with constant_provider(Joke, Joke(another_lime_joke)):
        _parent: Parent = Providers[Parent]()
        assert _parent.make_a_joke() == another_lime_joke
    
    # Constant provider should be destroyed.
    with pytest.raises(ProviderNotFoundError):
        Providers[Joke]


def test_temporary_provider():
    from beamlime.constructors import temporary_provider, ProviderNotFoundError
    def make_anoter_lime_joke() -> Joke:
        return Joke("Why did the lime go to hospital?"\
                    "\n\n\n"\
                    "Because she wasn't peeling well.")
    with temporary_provider(Joke, make_anoter_lime_joke):
        _parent: Parent = Providers[Parent]()
        assert _parent.make_a_joke() == make_anoter_lime_joke()

    # Temporary provider should be destroyed.
    with pytest.raises(ProviderNotFoundError):
        Providers[Joke]


def test_temporary_provider_compatible_new_type():
    from beamlime.constructors import temporary_provider
    BinaryJoke = NewType("BinaryJoke", str)

    def how_many_problems_i_have() -> BinaryJoke:
        return BinaryJoke('0b1100011')
    
    with temporary_provider(Joke, how_many_problems_i_have):
        Providers[Joke]() == bin(99)
    
def test_temporary_provider_non_compatible_type_raises():
    from beamlime.constructors import temporary_provider, MismatchingProductTypeError
    class BinaryJoke(str):
        ...

    def how_many_problems_i_have() -> BinaryJoke:
        return BinaryJoke('0b1100011')
    
    with pytest.raises(MismatchingProductTypeError):
        with temporary_provider(Joke, how_many_problems_i_have):
            ...
    
