# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any, Optional, Union

import pytest

from beamlime.constructors import Factory, ProviderGroup, ProviderNotFoundError

from .preset_providers import (
    Adult,
    GoodTelling,
    Joke,
    Parent,
    give_a_good_telling,
    make_a_joke,
    make_another_joke,
)


@pytest.fixture
def test_provider_group() -> ProviderGroup:
    return ProviderGroup(Adult, Parent, give_a_good_telling)


@pytest.fixture
def test_factory(test_provider_group) -> Factory:
    return Factory(test_provider_group)


def test_factory_getitem(test_factory: Factory):
    assert test_factory[GoodTelling] == give_a_good_telling()


def test_factory_getitem_with_attr_dependency(test_factory: Factory):
    assert isinstance(test_factory[Adult], Adult)
    assert test_factory[Adult].good_telling == give_a_good_telling()
    assert test_factory[GoodTelling] == give_a_good_telling()


def test_factory_getitem_with_arg_dependency():
    provider_group = ProviderGroup(give_a_good_telling)
    another_good_telling = "Eat healthy!"
    provider_group[str] = lambda: another_good_telling
    factory = Factory(provider_group)

    assert factory[GoodTelling] != give_a_good_telling()
    assert factory[GoodTelling] == another_good_telling


def func_implicit_optional_arg(arg: int = None) -> object:  # type:ignore[assignment]
    # TODO: Remove this test case when ``inspect.signature``
    # does not annotation implicit Optional.
    # When a default value is set as ``None``,
    # the annotation in the signature will be inferred as ``Optional``.
    # However, PEP 484 prohibits implicit Optional
    # and inspect module also may change accordingly.
    # Then this test case may be removed after the change.
    return arg


def func_optional_arg(arg: Optional[int] = None) -> object:
    return arg


def func_union_optional_arg(arg: Union[None, int] = None) -> object:
    return arg


@pytest.mark.parametrize(
    "optional_arg_func",
    (func_optional_arg, func_implicit_optional_arg, func_union_optional_arg),
)
def test_factory_optional_annotation(optional_arg_func):
    provider_group = ProviderGroup(optional_arg_func)
    factory = Factory(provider_group)
    with factory.constant_provider(int, 1):
        assert factory[object] == 1


@pytest.mark.parametrize(
    ["optional_arg_func"],
    [(func_optional_arg,), (func_implicit_optional_arg,), (func_union_optional_arg,)],
)
def test_factory_optional_annotation_none(optional_arg_func):
    provider_group = ProviderGroup(optional_arg_func)
    factory = Factory(provider_group)
    assert factory[object] is None


def func_union_arg(arg: Union[None, int, float] = None) -> Any:
    return arg


def test_union_arg_raises():
    with pytest.raises(NotImplementedError):
        ProviderGroup(func_union_arg)


def func_optional_return(arg: Optional[int] = None) -> Optional[int]:
    return arg


def func_union_return(arg: Union[int, float, None] = None) -> Union[int, float, None]:
    return arg


@pytest.mark.parametrize(
    ["union_return_func"], [(func_optional_return,), (func_union_return,)]
)
def test_factory_optional_return_raises(union_return_func):
    with pytest.raises(NotImplementedError):
        ProviderGroup(union_return_func)


def test_factory_catalogue(test_provider_group, test_factory: Factory):
    assert set(test_provider_group.keys()) == test_factory.catalogue


def test_factory_len(test_factory, test_provider_group):
    assert len(test_provider_group) == len(test_factory)


def test_provider_not_exist_rasies():
    factory = Factory()
    with pytest.raises(ProviderNotFoundError):
        factory[bool]


def test_provider_incomplete_class_arguments_rasies(test_factory: Factory):
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


def test_local_factory(test_factory: Factory):
    with test_factory.local_factory(ProviderGroup(make_a_joke)) as factory:
        with pytest.raises(ProviderNotFoundError):
            test_factory[Parent]

        assert factory[Parent].joke == make_a_joke()
        assert factory[Parent].make_a_joke() == make_a_joke()


def test_local_factory_overwritten():
    global_factory = Factory(ProviderGroup(make_a_joke))
    assert global_factory[Joke] == make_a_joke()
    with global_factory.local_factory(ProviderGroup(make_another_joke)) as factory:
        assert factory[Joke] == make_another_joke()
