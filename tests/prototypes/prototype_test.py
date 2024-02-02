# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Any, Generator

import pytest

from beamlime.constructors import Factory
from tests.benchmarks.runner import BenchmarkSession
from tests.prototypes.parameters import EventRate, NumPixels, PrototypeParameters
from tests.prototypes.prototype_mini import (
    BenchmarkTargetName,
    PrototypeBenchmarkRecipe,
    mini_prototype_factory,
)


@pytest.fixture
def mini_factory() -> Generator[Factory, None, None]:
    from tests.prototypes.prototype_mini import mini_prototype_factory

    yield mini_prototype_factory()


@pytest.fixture
def kafka_factory(kafka_test: bool) -> Generator[Factory, None, None]:
    from tests.prototypes.prototype_kafka import kafka_prototype_factory

    assert kafka_test
    yield kafka_prototype_factory()


def prototype_test_helper(prototype_factory: Factory):
    from tests.prototypes.prototype_mini import (
        PrototypeBenchmarkRecipe,
        PrototypeRunner,
    )

    prototype_runner = PrototypeRunner()

    recipe = PrototypeBenchmarkRecipe(params=PrototypeParameters())
    prototype_runner(prototype_factory.providers, recipe)


def test_mini_prototype(mini_factory: Factory):
    prototype_test_helper(mini_factory)


def test_kafka_prototype(kafka_factory: Factory):
    prototype_test_helper(kafka_factory)


def test_mini_prototype_benchmark():
    """Test benchmark runner and discard the results."""
    from tests.benchmarks.runner import (
        BenchmarkRunner,
        create_benchmark_session_factory,
    )
    from tests.prototypes.prototype_mini import PrototypeRunner

    benchmark_factory = create_benchmark_session_factory()
    recipe = PrototypeBenchmarkRecipe(
        params=PrototypeParameters(), optional_parameters={'test-run': True}
    )

    with benchmark_factory.temporary_provider(BenchmarkRunner, PrototypeRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]

        with benchmark_session.configure(iterations=3, auto_save=False):
            benchmark_session.run(
                mini_prototype_factory().providers,
                recipe,
                BenchmarkTargetName('mini_prototype'),
            )


@pytest.fixture(scope='session')
def prototype_benchmark(benchmark_test: bool) -> Generator[BenchmarkSession, Any, Any]:
    from tests.benchmarks.runner import (
        BenchmarkRunner,
        create_benchmark_session_factory,
    )
    from tests.prototypes.prototype_mini import PrototypeRunner

    assert benchmark_test

    benchmark_factory = create_benchmark_session_factory()
    with benchmark_factory.temporary_provider(BenchmarkRunner, PrototypeRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]
        yield benchmark_session

        benchmark_session.save()


@pytest.fixture(params=[10_000, 100_000, 1_000_000, 10_000_000, 20_000_000])
def num_pixels_all_range(request: pytest.FixtureRequest) -> NumPixels:
    """Full range of num_pixels to benchmark."""
    return NumPixels(request.param)


@pytest.fixture(params=[10_000, 100_000, 1_000_000, 10_000_000, 100_000_000])
def event_rate_all_range(request: pytest.FixtureRequest) -> NumPixels:
    """Full range of event_rate to benchmark."""
    return NumPixels(request.param)


@pytest.fixture
def prototype_recipe_all_range(
    full_benchmark_test: bool,
    num_pixels_all_range: NumPixels,
    event_rate_all_range: EventRate,
) -> PrototypeParameters:
    from tests.prototypes.prototype_mini import PrototypeParameters

    assert full_benchmark_test

    return PrototypeParameters(
        num_pixels=num_pixels_all_range, event_rate=event_rate_all_range
    )


def test_mini_prototype_benchmark_all_range(
    prototype_benchmark: BenchmarkSession,
    prototype_recipe_all_range: PrototypeParameters,
):
    recipe = PrototypeBenchmarkRecipe(params=prototype_recipe_all_range)

    with prototype_benchmark.configure(iterations=3):
        prototype_benchmark.run(
            mini_prototype_factory().providers,
            recipe,
            BenchmarkTargetName('mini_prototype'),
        )
