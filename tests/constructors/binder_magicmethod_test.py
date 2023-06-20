# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Binder

from .preset_binder import GoodTelling, Joke, give_a_good_telling, make_a_joke


def test_binder_add():
    funny = Binder(make_a_joke)
    grumpy = Binder(give_a_good_telling)
    neutral = funny + grumpy
    assert neutral[Joke]() == funny[Joke]()
    assert neutral[GoodTelling]() == grumpy[GoodTelling]()


def test_binder_i_add():
    funny = Binder(make_a_joke)
    neutral = Binder(give_a_good_telling)
    neutral_ref = neutral
    neutral += funny
    assert neutral[Joke]() == funny[Joke]()
    assert neutral_ref is neutral


def test_binder_add_wrong_type_raises():
    funny = Binder(make_a_joke)
    with pytest.raises(NotImplementedError):
        funny + GoodTelling


def test_binder_iadd():
    funny = Binder(make_a_joke)
    with pytest.raises(NotImplementedError):
        funny += GoodTelling
