# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import GlobalBinder, local_binder, provider

from .preset_binder import GoodTelling, TestBinder, give_a_good_telling


def integer_provider() -> int:
    return 10


def test_global_provider_decorator():
    original_call = provider(integer_provider)  # Call the decorator directly.
    assert original_call is integer_provider
    assert GlobalBinder()[int].constructor is integer_provider
    assert GlobalBinder()[int]() == integer_provider()
    GlobalBinder().clear_all()


def test_provider_function_call():
    assert TestBinder[GoodTelling].constructor == give_a_good_telling
    assert TestBinder[GoodTelling]() == give_a_good_telling()


def test_provider_incomplete_class_raises():
    with local_binder():
        from beamlime.constructors import ProviderNotFoundError

        from .preset_binder import Parent

        with pytest.raises(ProviderNotFoundError):
            TestBinder[Parent]()


def test_provider_member_class_raises():
    with pytest.raises(NotImplementedError):

        @provider
        class _:
            ...


def test_provider_member_function_raises():
    with pytest.raises(NotImplementedError):

        class _:
            @provider
            def __(self):
                ...


def test_provider_local_function_raises():
    with pytest.raises(NotImplementedError):

        @provider
        def _() -> None:
            return None
