# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import NewType

from beamlime.constructors import Factory

GoodTelling = NewType("GoodTelling", str)
sweet_reminder = "Drink some water!"

Joke = NewType("Joke", str)
lime_joke = "What cars do rich limes ride?\n\n\n\nA lime-o"
orange_joke = "Orange... you glad that I didn't say orange?"

Status = NewType("Status", str)
adult_default_status = Status("I want to go home.")

test_factory = Factory()


@test_factory.provider
def give_a_good_telling(good_telling: str = sweet_reminder) -> GoodTelling:
    return GoodTelling(good_telling)


def make_a_joke(joke: str = lime_joke) -> Joke:
    return Joke(joke)


def make_another_joke(joke: str = orange_joke) -> Joke:
    return Joke(joke)


@test_factory.provider
class Adult:
    good_telling: GoodTelling
    status: Status = adult_default_status


@test_factory.provider
@dataclass
class Parent(Adult):
    joke: Joke

    def give_a_good_telling(self) -> GoodTelling:
        return self.good_telling

    def make_a_joke(self) -> Joke:
        return self.joke

    def how_are_you(self) -> Status:
        return self.status


def create_factory(_tp, value):
    factory = Factory()
    factory.register(_tp, lambda: value)
    return factory
