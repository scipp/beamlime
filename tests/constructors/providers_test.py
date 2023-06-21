# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Factory, ProviderNotFoundError
from beamlime.constructors.providers import Provider

from .preset_factory import test_factory


def test_provider_not_exist_rasies():
    factory = Factory()
    with pytest.raises(ProviderNotFoundError):
        factory[bool]


def test_provider_function_call():
    from .preset_factory import Joke, make_a_joke

    with test_factory.local_factory() as factory:
        factory.register(Joke, make_a_joke)
        assert factory[Joke] == make_a_joke()


def test_unknown_provider_call_raises():
    from beamlime.constructors.providers import UnknownProvider

    with pytest.raises(ProviderNotFoundError):
        UnknownProvider()


def test_provider_class():
    from .preset_factory import Adult

    assert isinstance(test_factory[Adult], Adult)


def test_provider_partial():
    from functools import partial

    from .preset_factory import make_a_joke, orange_joke

    provider = Provider(partial(make_a_joke, joke=orange_joke))
    assert make_a_joke != provider()
    assert provider() == orange_joke


def test_provider_compare_with_wrong_type_raises():
    provider = Provider(lambda: 0)
    with pytest.raises(NotImplementedError):
        assert provider == test_provider_compare_with_wrong_type_raises


def test_new_provider_with_args():
    from beamlime.constructors.providers import Provider

    from .preset_factory import make_a_joke, orange_joke

    expected_constructor = make_a_joke
    provider = Provider(make_a_joke, orange_joke)
    assert provider.constructor == expected_constructor
    assert orange_joke in provider.args
    assert provider.constructor(*provider.args) == provider() == orange_joke
