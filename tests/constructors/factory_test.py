# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Factory, ProviderNotFoundError

from .preset_factory import (
    GoodTelling,
    Joke,
    give_a_good_telling,
    make_a_joke,
    make_another_joke,
    test_factory,
)


def test_provider_incomplete_class_arguments_rasies():
    from .preset_factory import Parent

    with test_factory.local_factory() as factory:
        with pytest.raises(ProviderNotFoundError):
            factory[Parent]


class IncompleteClass:
    attribute: str


def test_provider_incomplete_class_attributes_rasies():
    factory = Factory()

    factory.register(IncompleteClass, IncompleteClass)
    assert IncompleteClass in factory.providers
    with pytest.raises(ProviderNotFoundError):
        factory[IncompleteClass]  # Provider of attribute does not exist.


def test_factory_initials():
    funny = Factory(make_a_joke)
    grumpy = Factory(give_a_good_telling)
    neutral = Factory(funny, grumpy)
    assert neutral[Joke] == funny[Joke]
    assert neutral[GoodTelling] == grumpy[GoodTelling]


def test_factory_conflicting_but_sharable():
    funny = Factory(make_a_joke)
    just_funny = Factory(funny, funny)
    assert just_funny[Joke] == funny[Joke]
    assert len(just_funny) == len(funny)


def test_factory_conflicting_initials_raises():
    from beamlime.constructors import ProviderExistsError

    funny = Factory(make_a_joke)
    funnier = Factory(make_another_joke)
    with pytest.raises(ProviderExistsError):
        Factory(funny, funnier)


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
