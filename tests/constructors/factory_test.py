# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import pytest

from beamlime.constructors import Factory, ProviderGroup, ProviderNotFoundError

from .preset_factory import test_factory


def test_factory_getitem():
    from .preset_factory import GoodTelling, give_a_good_telling

    assert test_factory[GoodTelling] == give_a_good_telling()


def test_factory_getitem_with_attr_dependency():
    from .preset_factory import Adult, GoodTelling, give_a_good_telling

    assert isinstance(test_factory[Adult], Adult)
    assert test_factory[Adult].good_telling == give_a_good_telling()
    assert test_factory[GoodTelling] == give_a_good_telling()


def test_factory_getitem_with_arg_dependency():
    from beamlime.constructors import ProviderGroup

    from .preset_factory import GoodTelling, give_a_good_telling

    provider_group = ProviderGroup(give_a_good_telling)
    another_good_telling = "Eat healthy!"
    provider_group[str] = lambda: another_good_telling
    factory = Factory(provider_group)

    assert factory[GoodTelling] != give_a_good_telling()
    assert factory[GoodTelling] == another_good_telling


def test_factory_catalogue():
    from .preset_factory import test_factory, test_provider_group

    assert set(test_provider_group.keys()) == test_factory.catalogue


def test_factory_len():
    from .preset_factory import test_factory, test_provider_group

    assert len(test_provider_group) == len(test_factory)


def test_provider_not_exist_rasies():
    factory = Factory()
    with pytest.raises(ProviderNotFoundError):
        factory[bool]


def test_provider_incomplete_class_arguments_rasies():
    from .preset_factory import Parent

    with pytest.raises(ProviderNotFoundError):
        test_factory[Parent]


def test_provider_incomplete_class_attributes_raises():
    class IncompleteClass:
        attribute: str

    factory = Factory(ProviderGroup(IncompleteClass))

    assert IncompleteClass in factory.catalogue
    with pytest.raises(ProviderNotFoundError):
        factory[IncompleteClass]


def test_provider_incomplete_class_attributes_with_defaults():
    class ClassWithNoneDefault:
        attr: None = None

    factory = Factory(ProviderGroup(ClassWithNoneDefault))

    assert None not in factory.catalogue
    assert isinstance(factory[ClassWithNoneDefault], ClassWithNoneDefault)
    assert factory[ClassWithNoneDefault].attr is None


def func_with_cyclic_dependency(arg: str) -> str:
    return arg


def test_cyclic_dependency_raises():
    factory = Factory(ProviderGroup(func_with_cyclic_dependency))
    with pytest.raises(RecursionError):
        assert factory[str]


def test_local_factory():
    from .preset_factory import Parent, make_a_joke

    with test_factory.local_factory(ProviderGroup(make_a_joke)) as factory:
        with pytest.raises(ProviderNotFoundError):
            test_factory[Parent]

        assert factory[Parent].joke == make_a_joke()
        assert factory[Parent].make_a_joke() == make_a_joke()


def test_local_factory_overwritten():
    from .preset_factory import Joke, make_a_joke, make_another_joke

    global_factory = Factory(ProviderGroup(make_a_joke))
    assert global_factory[Joke] == make_a_joke()
    with global_factory.local_factory(ProviderGroup(make_another_joke)) as factory:
        assert factory[Joke] == make_another_joke()
