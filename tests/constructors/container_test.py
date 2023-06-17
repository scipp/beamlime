# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)


def test_container_singleton():
    from beamlime.constructors import get_container, Container
    from beamlime.constructors.containers import _Container
    assert _Container() is get_container()
    assert Container is get_container()
    assert _Container() is Container    

def test_container():
    from beamlime.constructors import Container, local_providers, temporary_provider
    with local_providers():
        from .preset_providers import Parent, Joke, make_a_joke, adult_default_status
        with temporary_provider(Joke, make_a_joke):
            parent = Container[Parent]
            isinstance(parent, Parent)
            parent.how_are_you() == adult_default_status
