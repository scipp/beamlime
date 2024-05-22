# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import NewType

import pytest

from beamlime.constructors import Factory, ProviderGroup, ProviderNotFoundError
from tests.providers.preset_providers import (
    Adult,
    GoodTelling,
    Joke,
    Parent,
    give_a_good_telling,
    lime_joke,
    sweet_reminder,
)


@pytest.fixture
def test_provider_group() -> ProviderGroup:
    return ProviderGroup(Adult, Parent, give_a_good_telling)


@pytest.fixture
def test_factory(test_provider_group) -> Factory:
    return Factory(test_provider_group)


def test_partial_provider(test_factory: Factory):
    with test_factory.partial_provider(Parent, joke=Joke(lime_joke)):
        _parent: Parent = test_factory[Parent]
        assert _parent.give_a_good_telling() == sweet_reminder
        assert _parent.make_a_joke() == lime_joke

    # partial provider should be destroyed.
    with pytest.raises(ProviderNotFoundError):
        test_factory[Parent]


def test_partial_of_non_existing_provider_raises(test_factory: Factory):
    """You can only create a partial provider from the existing one."""
    from tests.providers.preset_providers import Joke

    with test_factory.local_factory() as factory:
        with pytest.raises(ProviderNotFoundError):
            with factory.partial_provider(Joke):
                ...


def test_constant_provider(test_factory: Factory):
    another_lime_joke = (
        "Why are limes so observant?" "\n\n\n" "They're full of Vitamin See."
    )
    with test_factory.local_factory() as factory:
        with factory.constant_provider(Joke, Joke(another_lime_joke)):
            _parent = factory[Parent]
            assert _parent.make_a_joke() == another_lime_joke

        # Constant provider should be destroyed.
        with pytest.raises(ProviderNotFoundError):
            factory[Joke]


def make_anoter_lime_joke() -> Joke:
    return Joke(
        "Why did the lime go to the hospital?"
        "\n\n\n"
        "Because she wasn't peeling well."
    )


def test_temporary_provider(test_factory: Factory):
    with test_factory.temporary_provider(Joke, make_anoter_lime_joke):
        _parent = test_factory[Parent]
        assert _parent.make_a_joke() == make_anoter_lime_joke()

    # Temporary provider should be destroyed.
    with pytest.raises(ProviderNotFoundError):
        test_factory[Joke]


def give_another_good_telling() -> GoodTelling:
    return GoodTelling("Go home and eat dinner!")


def test_temporary_provider_replace(test_factory: Factory):
    assert test_factory[GoodTelling] == give_a_good_telling()

    with test_factory.temporary_provider(GoodTelling, give_another_good_telling):
        assert test_factory[GoodTelling] == give_another_good_telling()

    assert test_factory[GoodTelling] == give_a_good_telling()


BinaryJoke = NewType("BinaryJoke", str)


def how_many_problems_i_have() -> BinaryJoke:
    return BinaryJoke("0b1100011")


def test_temporary_provider_compatible_new_type(test_factory: Factory):
    with test_factory.local_factory() as factory:
        with factory.temporary_provider(Joke, how_many_problems_i_have):
            assert factory[Joke] == bin(99)


class HexJoke(str): ...


def how_many_problems_i_hex() -> BinaryJoke:
    return BinaryJoke("0x63")


def test_temporary_provider_incompatible_type_raises(test_factory: Factory):
    from beamlime.constructors import MismatchingProductTypeError

    with pytest.raises(MismatchingProductTypeError):
        with test_factory.temporary_provider(HexJoke, how_many_problems_i_hex):
            ...
