# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import SingletonProvider


def function_with_unhashable_arguments(unhashable_arg: list) -> list:
    return list(unhashable_arg)


def random_number() -> float:
    import random

    return random.random()


def random_number_with_seed(seed: int) -> float:
    import random

    random.seed(seed)
    return random.random()


def test_singleton_provider_without_arguments():
    singleton_random_number = SingletonProvider(random_number)
    assert singleton_random_number() == singleton_random_number()
    assert singleton_random_number() != random_number()


def test_singleton_provider_with_arguments():
    import random

    from beamlime.constructors import SingletonProvider

    singleton_random_number = SingletonProvider(random_number_with_seed)
    assert singleton_random_number(123) == singleton_random_number(123)
    assert singleton_random_number(123) is singleton_random_number(123)

    random.seed(123)
    assert singleton_random_number(123) == random.random()
    assert singleton_random_number(123) != random.random()


def test_singleton_provider_unhashable_arguments():
    from beamlime.constructors import SingletonProvider

    provider = SingletonProvider(function_with_unhashable_arguments)
    unhashable_argument = [1, 2, 3]
    with pytest.raises(TypeError):
        hash(unhashable_argument)

    singleton_result = provider(unhashable_argument)
    assert singleton_result is provider(unhashable_argument)


def hash_object(hashable_arg: object) -> int:
    return hash(hashable_arg)


def test_singleton_provider_hash_key_changes():
    """
    If the same instance is used as an argument,
    argument check will pass even if the hash key changes.
    """
    from beamlime.constructors import SingletonProvider

    class NeverTheSame:
        def __eq__(self, _: object) -> bool:
            return False

        def __hash__(self) -> int:
            from random import random

            return int(random() * 10)

    hashable_argument = NeverTheSame()
    provider = SingletonProvider(hash_object)

    assert hash_object(hashable_argument) != hash_object(hashable_argument)
    assert provider(hashable_argument) == provider(hashable_argument)
    assert provider(hashable_argument) is provider(hashable_argument)


def test_singleton_provider_called_with_different_args_raises():
    from beamlime.constructors import SingletonProviderCalledWithDifferentArgs

    singleton_random_number = SingletonProvider(random_number_with_seed)
    assert singleton_random_number(123) == singleton_random_number(123)

    with pytest.raises(SingletonProviderCalledWithDifferentArgs):
        singleton_random_number(100)


def test_singleton_provider_called_with_different_unhashable_args_raises():
    from beamlime.constructors import (
        SingletonProvider,
        SingletonProviderCalledWithDifferentArgs,
    )

    provider = SingletonProvider(function_with_unhashable_arguments)
    provider([1, 2, 3])
    with pytest.raises(SingletonProviderCalledWithDifferentArgs):
        assert provider([1, 2, 3])
