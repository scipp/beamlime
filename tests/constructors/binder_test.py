# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import clean_binder, context_binder

from .preset_binder import TestBinder


def test_global_provider_singleton():
    from beamlime.constructors import GlobalBinder, get_global_binder

    assert GlobalBinder() is GlobalBinder()
    assert get_global_binder() is GlobalBinder()


def test_provider_already_exists_raises():
    from beamlime.constructors import ProviderExistsError

    from .preset_binder import Joke, make_a_joke, make_another_joke

    with context_binder(TestBinder) as binder:
        binder[Joke] = make_a_joke

        with pytest.raises(ProviderExistsError):
            binder[Joke] = make_another_joke


def test_provider_already_exists_without_name_rasies():
    from beamlime.constructors import ProviderExistsError

    with clean_binder() as binder:
        binder[type(None)] = lambda: None
        with pytest.raises(ProviderExistsError):
            binder[type(None)] = lambda: None
