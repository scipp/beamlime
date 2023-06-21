# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Union

import pytest

from beamlime.constructors import Factory

from .preset_factory import test_factory


def func_with_union_arg(_: Union[float, int]) -> Union[float, int]:
    return _


def test_union_annotation_raises():
    factory = Factory()

    with pytest.raises(NotImplementedError):
        factory.register(Union[float, int], func_with_union_arg)


def func_without_arg_type(_) -> int:
    return _


def test_insufficient_annotation_raises():
    from beamlime.constructors import InsufficientAnnotationError

    factory = Factory()

    with pytest.raises(InsufficientAnnotationError):
        factory.register(int, func_without_arg_type)


def test_provider_already_exists_raises():
    from beamlime.constructors import ProviderExistsError

    from .preset_factory import Joke, make_a_joke, make_another_joke

    with test_factory.local_factory() as factory:
        factory.register(Joke, make_a_joke)

        with pytest.raises(ProviderExistsError):
            factory.register(Joke, make_another_joke)


def test_provider_already_exists_lambda_rasies():
    from beamlime.constructors import ProviderExistsError

    with test_factory.local_factory() as factory:
        factory.register(type(None), lambda: None)
        with pytest.raises(ProviderExistsError):
            factory.register(type(None), lambda: None)


def test_provider_already_exists_but_compatible():
    from .preset_factory import Joke, make_a_joke

    with test_factory.local_factory() as factory:
        factory.register(Joke, make_a_joke)
        factory.register(Joke, make_a_joke)
