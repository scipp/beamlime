# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Factory

from .preset_factory import GoodTelling, Joke, give_a_good_telling, make_a_joke


def test_factory_add():
    funny = Factory(make_a_joke)
    grumpy = Factory(give_a_good_telling)
    neutral = funny + grumpy
    assert neutral[Joke] == funny[Joke]
    assert neutral[GoodTelling] == grumpy[GoodTelling]


def test_factory_i_add():
    funny = Factory(make_a_joke)
    neutral = Factory(give_a_good_telling)
    neutral_ref = neutral
    neutral += funny
    assert neutral[Joke] == funny[Joke]
    assert neutral_ref is neutral


def test_factory_add_wrong_type_raises():
    funny = Factory(make_a_joke)
    with pytest.raises(NotImplementedError):
        funny + GoodTelling


def test_factory_iadd():
    funny = Factory(make_a_joke)
    with pytest.raises(NotImplementedError):
        funny += GoodTelling


def test_factory_catalogue():
    from .preset_factory import Adult, GoodTelling, Parent, test_factory

    assert test_factory.catalogue == set((GoodTelling, Adult, Parent))


def test_factory_clear_all():
    import pytest

    from beamlime.constructors import Factory, ProviderNotFoundError

    factory = Factory()
    factory.register(int, lambda: 99)
    assert factory[int] == 99
    factory.clear()
    with pytest.raises(ProviderNotFoundError):
        factory[int]
    assert len(factory) == 0
