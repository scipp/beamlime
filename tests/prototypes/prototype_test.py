# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Any, Generator

import pytest

from beamlime.constructors import Factory

from ..benchmarks.runner import BenchmarkSession
from .parameters import PrototypeParameters


@pytest.fixture
def mini_factory() -> Generator[Factory, None, None]:
    from .prototype_mini import mini_prototype_factory

    yield mini_prototype_factory()


@pytest.fixture
def kafka_factory(kafka_test: bool) -> Generator[Factory, None, None]:
    from .prototype_kafka import kafka_prototype_factory

    assert kafka_test
    yield kafka_prototype_factory()


def prototype_test_helper(prototype_factory: Factory):
    from .prototype_mini import PrototypeBenchmarkRecipe, PrototypeRunner

    prototype_runner = PrototypeRunner()

    recipe = PrototypeBenchmarkRecipe(params=PrototypeParameters())
    prototype_runner(prototype_factory.providers, recipe)


def test_mini_prototype(mini_factory: Factory):
    prototype_test_helper(mini_factory)


def test_kafka_prototype(kafka_factory: Factory):
    prototype_test_helper(kafka_factory)


@pytest.fixture(scope='session')
def prototype_benchmark(benchmark_test: bool) -> Generator[BenchmarkSession, Any, Any]:
    from ..benchmarks.runner import BenchmarkRunner, create_benchmark_runner_factory
    from .prototype_mini import PrototypeRunner

    assert benchmark_test

    benchmark_factory = create_benchmark_runner_factory()
    with benchmark_factory.temporary_provider(BenchmarkRunner, PrototypeRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]
        yield benchmark_session

        benchmark_session.save()


def test_mini_prototype_benchmark(prototype_benchmark: BenchmarkSession):
    from .prototype_mini import (
        BenchmarkTargetName,
        PrototypeBenchmarkRecipe,
        mini_prototype_factory,
    )

    recipe = PrototypeBenchmarkRecipe(
        params=PrototypeParameters(), optional_parameters={'test-run': True}
    )

    with prototype_benchmark.configure(iterations=3):
        prototype_benchmark.run(
            mini_prototype_factory().providers,
            recipe,
            BenchmarkTargetName('mini_prototype'),
        )
