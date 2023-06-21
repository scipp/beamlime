# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from .preset_factory import test_factory


def test_provider_incomplete_class_raises():
    with test_factory.local_factory():
        from beamlime.constructors import ProviderNotFoundError

        from .preset_factory import Parent

        with pytest.raises(ProviderNotFoundError):
            test_factory[Parent]


def test_provider_member_class_raises():
    with test_factory.local_factory() as factory:
        with pytest.raises(NotImplementedError):

            @factory.provider
            class _:
                ...


def test_provider_member_function_raises():
    with test_factory.local_factory() as factory:
        with pytest.raises(NotImplementedError):

            class _:
                @factory.provider
                def __(self):
                    ...


def test_provider_local_function_raises():
    with test_factory.local_factory() as factory:
        with pytest.raises(NotImplementedError):

            @factory.provider
            def _() -> None:
                return None
