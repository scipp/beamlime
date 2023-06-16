# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import NewType
from beamlime.constructors import provider, Container

GoodTelling = NewType("GoodTelling", str)
sweet_reminder = "Drink some water!"

@provider
def give_a_good_telling() -> GoodTelling:
    return GoodTelling("sweet_reminder")

@provider
class Parent:
    good_telling: GoodTelling

    def give_a_good_telling(self) -> GoodTelling:
        return self.good_telling

def test_container_singleton():
    from beamlime.constructors import get_container
    from beamlime.constructors import Container
    from beamlime.constructors.containers import _Container
    assert _Container() is get_container()
    assert Container is get_container()
    assert _Container() is Container    

def test_container():
    parent = Container[Parent]
    isinstance(parent, Parent)
    parent.give_a_good_telling() == sweet_reminder
