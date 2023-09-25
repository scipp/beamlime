# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors.providers import CachedProvider


def function_with_unhashable_arguments(_: list) -> None:
    return


def random_number() -> float:
    import random

    return random.random()


def random_number_with_seed(seed: int) -> float:
    import random

    random.seed(seed)
    return random.random()


def test_cached_provider_without_arguments():
    cached_random_number = CachedProvider(random_number)
    assert cached_random_number() == cached_random_number()
    assert cached_random_number() != random_number()


def test_cached_provider_with_arguments():
    import random

    from beamlime.constructors.providers import CachedProvider

    cached_random_number = CachedProvider(random_number_with_seed)
    assert cached_random_number(123) == cached_random_number(123)

    random.seed(123)
    assert cached_random_number(123) == random.random()
    assert cached_random_number(123) != random.random()


def test_cached_provider_called_with_different_args_raises():
    from beamlime.constructors.providers import CachedProviderCalledWithDifferentArgs

    cached_random_number = CachedProvider(random_number_with_seed)
    assert cached_random_number(123) == cached_random_number(123)

    with pytest.raises(CachedProviderCalledWithDifferentArgs):
        cached_random_number(100)


def test_cached_provider_unhashable_arguments_raises():
    from beamlime.constructors.providers import CachedProvider

    with pytest.raises(TypeError, match='cannot be cached. Unhashable argument'):
        CachedProvider(function_with_unhashable_arguments)
