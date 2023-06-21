# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import ProviderNotFoundError
from beamlime.constructors.contexts import context_binder

from .preset_binder import TestBinder


def test_partial_provider():
    from beamlime.constructors import partial_provider

    from .preset_binder import Joke, Parent, lime_joke, sweet_reminder

    with context_binder(TestBinder) as binder:
        binder[Parent]
        with partial_provider(Parent, joke=Joke(lime_joke)):
            _parent: Parent = binder[Parent]()
            assert _parent.give_a_good_telling() == sweet_reminder
            assert _parent.make_a_joke() == lime_joke

        # partial provider should be destroyed.
        with pytest.raises(ProviderNotFoundError):
            binder[Parent]()


def test_partial_of_non_existing_provider_raises():
    """You can only create a partial provider from the existing one."""
    from beamlime.constructors import partial_provider

    from .preset_binder import Joke

    with context_binder(TestBinder) as _:
        with pytest.raises(ProviderNotFoundError):
            with partial_provider(Joke):
                ...


def test_constant_provider():
    from beamlime.constructors import constant_provider

    another_lime_joke = (
        "Why are limes so observant?" "\n\n\n" "They're full of Vitamin See."
    )
    with context_binder(TestBinder) as binder:
        from .preset_binder import Joke, Parent

        with constant_provider(Joke, Joke(another_lime_joke)):
            _parent = binder[Parent]()
            assert _parent.make_a_joke() == another_lime_joke

        # Constant provider should be destroyed.
        with pytest.raises(ProviderNotFoundError):
            binder[Joke]


def test_temporary_provider():
    from beamlime.constructors import temporary_provider

    from .preset_binder import Joke, Parent

    with context_binder(TestBinder) as binder:

        def make_anoter_lime_joke() -> Joke:
            return Joke(
                "Why did the lime go to hospital?"
                "\n\n\n"
                "Because she wasn't peeling well."
            )

        with temporary_provider(Joke, make_anoter_lime_joke):
            _parent: Parent = binder[Parent]()
            assert _parent.make_a_joke() == make_anoter_lime_joke()

        # Temporary provider should be destroyed.
        with pytest.raises(ProviderNotFoundError):
            binder[Joke]


def test_temporary_provider_compatible_new_type():
    from typing import NewType

    from beamlime.constructors import temporary_provider

    with context_binder(TestBinder) as binder:
        from .preset_binder import Joke

        BinaryJoke = NewType("BinaryJoke", str)

        def how_many_problems_i_have() -> BinaryJoke:
            return BinaryJoke("0b1100011")

        with temporary_provider(Joke, how_many_problems_i_have):
            assert binder[Joke]() == bin(99)


def test_temporary_provider_incompatible_type_raises():
    from beamlime.constructors import MismatchingProductTypeError, temporary_provider

    from .preset_binder import Joke

    class BinaryJoke(str):
        ...

    def how_many_problems_i_have() -> Joke:
        return Joke("0b1100011")

    with context_binder(TestBinder) as _:
        with pytest.raises(MismatchingProductTypeError):
            with temporary_provider(BinaryJoke, how_many_problems_i_have):
                ...
