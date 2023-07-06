# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Union

import pytest

from beamlime.constructors.providers import Provider


def test_provider_function_call():
    from .preset_factory import make_a_joke

    joke_provider = Provider(make_a_joke)
    assert joke_provider.constructor == make_a_joke
    assert joke_provider() == make_a_joke()


def test_provider_class():
    from .preset_factory import Adult

    assert isinstance(Provider(Adult)(), Adult)


def test_provider_partial():
    from functools import partial

    from .preset_factory import make_a_joke, orange_joke

    provider: Provider = Provider(partial(make_a_joke, joke=orange_joke))
    assert make_a_joke() != provider()
    assert provider() == orange_joke


def test_provider_lambda():
    from .preset_factory import orange_joke

    provider = Provider(lambda: orange_joke)
    assert provider() == orange_joke


def test_provider_with_args():
    from .preset_factory import make_a_joke, orange_joke

    expected_constructor = make_a_joke
    provider = Provider(make_a_joke, orange_joke)
    assert provider.constructor == expected_constructor
    assert orange_joke in provider.args
    assert provider.constructor(*provider.args) == provider() == orange_joke


def test_provider_with_kwargs():
    from .preset_factory import make_a_joke, orange_joke

    kwargs = {"joke": orange_joke}
    provider = Provider(make_a_joke, **kwargs)
    assert provider.constructor == make_a_joke
    assert provider.keywords == kwargs
    assert provider.constructor(**kwargs) == provider() == orange_joke


def test_provider_compare_equal():
    from .preset_factory import make_a_joke, orange_joke

    assert Provider(make_a_joke) == Provider(make_a_joke)
    assert Provider(make_a_joke, orange_joke) == Provider(make_a_joke, orange_joke)
    assert Provider(make_a_joke, joke=orange_joke) == Provider(
        make_a_joke, joke=orange_joke
    )


def test_provider_compare_different():
    from .preset_factory import lime_joke, make_a_joke, make_another_joke, orange_joke

    assert Provider(make_a_joke) != Provider(make_another_joke)
    assert Provider(make_a_joke, orange_joke) != Provider(make_a_joke, lime_joke)
    assert Provider(make_a_joke, joke=orange_joke) != Provider(
        make_a_joke, joke=lime_joke
    )


def test_provider_can_provide_true():
    from .preset_factory import GoodTelling, Joke, make_a_joke

    assert Provider(make_a_joke).can_provide(str)
    assert Provider(make_a_joke).can_provide(GoodTelling)
    assert Provider(make_a_joke).can_provide(Joke)


def generic_provider(*args) -> list:
    return list(args)


def test_provider_can_provide_generic():
    assert Provider(generic_provider).can_provide(list[int])
    assert Provider(generic_provider).can_provide(list[str])


def test_provider_can_provide_false():
    from .preset_factory import make_a_joke

    class StrChild(str):
        ...

    assert not Provider(make_a_joke).can_provide(int)
    assert not Provider(make_a_joke).can_provide(StrChild)


def test_unknown_provider_call_raises():
    from beamlime.constructors.providers import UnknownProvider, UnknownProviderCalled

    with pytest.raises(UnknownProviderCalled):
        UnknownProvider()


def test_provider_compare_with_wrong_type_raises():
    provider = Provider(lambda: 0)
    with pytest.raises(NotImplementedError):
        assert provider == test_provider_compare_with_wrong_type_raises


def test_provider_class_method_raises():
    from .preset_factory import Parent

    with pytest.raises(NotImplementedError):
        Provider(Parent.give_a_good_telling)


def test_provider_local_scope_function_raises():
    def local_function():
        ...

    with pytest.raises(NotImplementedError):
        Provider(local_function)


def func_with_union_arg(_: Union[None, float]) -> int:
    return 0


def func_with_union_return() -> Union[None, float]:
    return None


def test_union_annotation_arg_raises():
    with pytest.raises(NotImplementedError):
        Provider(func_with_union_arg)


def test_union_annotation_return_raises():
    with pytest.raises(NotImplementedError):
        Provider(func_with_union_arg)


def func_without_arg_type(_) -> int:
    return _


def test_insufficient_annotation_raises():
    from beamlime.constructors import InsufficientAnnotationError

    with pytest.raises(InsufficientAnnotationError):
        Provider(func_without_arg_type)
