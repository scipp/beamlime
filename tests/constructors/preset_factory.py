# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import NewType

from beamlime.constructors import Factory, ProviderGroup

GoodTelling = NewType("GoodTelling", str)
sweet_reminder = "Drink some water!"

Joke = NewType("Joke", str)
lime_joke = "What cars do rich limes ride?\n\n\n\nA lime-o"
orange_joke = "Orange... you glad that I didn't say orange?"

Status = NewType("Status", str)
adult_default_status = Status("I want to go home.")


def give_a_good_telling(good_telling: str = sweet_reminder) -> GoodTelling:
    return GoodTelling(good_telling)


def make_a_joke(joke: str = lime_joke) -> Joke:
    return Joke(joke)


def make_another_joke(joke: str = orange_joke) -> Joke:
    return Joke(joke)


class Adult:
    good_telling: GoodTelling
    status: Status = adult_default_status


@dataclass
class Parent(Adult):
    joke: Joke

    def give_a_good_telling(self) -> GoodTelling:
        return self.good_telling

    def make_a_joke(self) -> Joke:
        return self.joke

    def how_are_you(self) -> Status:
        return self.status


test_provider_group = ProviderGroup(Adult, Parent, give_a_good_telling)
test_factory = Factory(test_provider_group)


def create_constant_factory(_tp, value):
    provider_gr = ProviderGroup()
    provider_gr[_tp] = lambda: value
    factory = Factory(provider_gr)
    return factory
