# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Container, ProviderNotFoundError, clean_binder
from beamlime.constructors.contexts import context_binder

from .preset_binder import create_binder


def test_global_binder():
    from beamlime.constructors import GlobalBinder, get_global_binder, global_binder

    with global_binder() as binder:
        assert GlobalBinder() is binder
        assert get_global_binder() is binder


def test_clean_binder():
    from beamlime.constructors import GlobalBinder, ProviderNotFoundError

    with clean_binder() as binder:
        assert len(binder) == 0
        assert binder is not GlobalBinder()
        binder[int] = lambda: 0
        assert binder[int]() == 0

        with pytest.raises(ProviderNotFoundError):
            GlobalBinder()[int]

    with pytest.raises(ProviderNotFoundError):
        GlobalBinder()[int]


def test_context_binder_from_global():
    from beamlime.constructors import GlobalBinder

    with context_binder() as binder:
        assert binder._providers == GlobalBinder()._providers
        assert binder is GlobalBinder()
        with clean_binder() as temp_binder:
            hardcoded_value = 10
            temp_binder[int] = lambda: hardcoded_value
            assert temp_binder[int]() == hardcoded_value
            assert Container[int] == hardcoded_value

        with pytest.raises(ProviderNotFoundError):
            binder[int]

        with pytest.raises(ProviderNotFoundError):
            Container[int]


def test_context_binder_nested():
    from beamlime.constructors import Binder, ProviderNotFoundError

    with context_binder(Binder()) as first_binder:
        first_value = 20
        first_binder[int] = lambda: first_value
        assert first_binder[int]() == first_value
        assert Container[int] == first_value

        with context_binder(Binder()) as second_binder:
            with pytest.raises(ProviderNotFoundError):
                second_binder[int]

            second_value = 10
            second_binder[int] = lambda: second_value

            assert second_binder[int]() == second_value
            assert Container[int] == second_value

        assert first_binder[int]() == first_value
        assert Container[int] == first_value


def test_context_binder_multiple():
    with clean_binder() as _:
        int_binder = create_binder(int, (int_value := 20))
        float_binder = create_binder(float, (float_value := 0.5))

        with context_binder(int_binder, float_binder) as combineder:
            assert combineder[int]()
            assert Container[int] == int_value
            assert Container[float] == float_value


def test_local_binder():
    from beamlime.constructors import global_binder, local_binder

    with local_binder() as binder:
        hardcoded_values = list(range(5))
        binder[int] = lambda: hardcoded_values
        assert Container[int] == hardcoded_values

    with pytest.raises(ProviderNotFoundError):
        with global_binder() as binder:
            binder[int]


def test_local_binder_nested():
    from beamlime.constructors import local_binder

    int_binder = create_binder(int, (int_value := 20))
    float_binder = create_binder(float, (float_value := 0.5))

    def print_int_and_float(_int_num: int, _float_num: float) -> str:
        return f"Integer: {_int_num}, Float: {_float_num}"

    with local_binder(int_binder) as top_binder:
        top_binder[str] = print_int_and_float

        with local_binder(float_binder) as bottom_binder:
            assert bottom_binder[int]() == int_value
            assert bottom_binder[float]() == float_value
            assert Container[str] == print_int_and_float(int_value, float_value)

            with pytest.raises(ProviderNotFoundError):
                top_binder[float]

        with pytest.raises(ProviderNotFoundError):
            Container[str]


def test_context_conflicting_binders_acceptable():
    from beamlime.constructors import Binder

    from .preset_binder import Joke, make_a_joke

    binder1 = Binder(make_a_joke)
    binder2 = Binder(make_a_joke)
    assert binder1[Joke] == binder2[Joke]
    with context_binder(binder1, binder2) as combineder:
        assert combineder[Joke] == binder1[Joke]


def test_context_conflicting_binders_raises():
    from beamlime.constructors import Binder, ProviderExistsError

    from .preset_binder import Joke, make_a_joke, make_another_joke

    binder1 = Binder(make_a_joke)
    binder2 = Binder(make_another_joke)
    assert binder1[Joke].product_spec == binder2[Joke].product_spec
    with pytest.raises(ProviderExistsError):
        with context_binder(binder1, binder2):
            ...
