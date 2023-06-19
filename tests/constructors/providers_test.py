# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import ProviderNotFoundError, clean_binder, context_binder

from .preset_binder import TestBinder


def test_provider_not_exist_rasies():
    with clean_binder() as binder:
        with pytest.raises(ProviderNotFoundError):
            binder[None]


def test_provider_function_call():
    from .preset_binder import Joke, make_a_joke

    with context_binder(TestBinder) as binder:
        binder[Joke] = make_a_joke
        assert binder[Joke].constructor == make_a_joke
        assert binder[Joke]() == make_a_joke()


def test_provider_function_call_with_arguments():
    from .preset_binder import GoodTelling

    good_telling_1 = "Sleep early!"
    with context_binder(TestBinder) as binder:
        assert binder[GoodTelling](good_telling=good_telling_1) == good_telling_1
        assert binder[GoodTelling](good_telling_1) == good_telling_1


def test_unknown_provider_call_raises():
    from beamlime.constructors.providers import UnknownProvider

    with pytest.raises(ProviderNotFoundError):
        UnknownProvider()


def test_provider_class():
    from .preset_binder import Parent

    with context_binder(TestBinder) as binder:
        assert binder[Parent].constructor == Parent


def test_provider_compare_with_wrong_type_raises():
    from .preset_binder import Parent

    with context_binder(TestBinder) as binder:
        with pytest.raises(NotImplementedError):
            assert binder[Parent] == Parent


def test_provider_incomplete_class_arguments_rasies():
    from .preset_binder import Parent

    with context_binder(TestBinder) as binder:
        with pytest.raises(ProviderNotFoundError):
            binder[Parent]()


def test_provider_incomplete_class_attributes_rasies():
    class IncompleteClass:
        attribute: str

    with clean_binder() as binder:
        binder[IncompleteClass] = IncompleteClass
        binder[IncompleteClass]  # Provider of incomplete class exists.
        with pytest.raises(ProviderNotFoundError):
            binder[IncompleteClass]()  # Provider of attribute does not exist.


def test_union_annotation_raises():
    from typing import Union

    from beamlime.constructors import temporary_provider

    def confusing_func(_: Union[float, int]) -> Union[float, int]:
        return _

    with pytest.raises(NotImplementedError):
        with temporary_provider(int, confusing_func):
            ...


def test_insufficient_annotation_raises():
    from beamlime.constructors import InsufficientAnnotationError, temporary_provider

    def confusing_func(_) -> int:
        return _

    with pytest.raises(InsufficientAnnotationError):
        with temporary_provider(int, confusing_func) as binder:
            binder[int]()
