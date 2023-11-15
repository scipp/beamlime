# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Any, Generator

import pytest

from beamlime.constructors import Factory

from ..benchmarks.runner import BenchmarkSession
from .parameters import EventRate, NumPixels, PrototypeParameters
from .prototype_mini import (
    BenchmarkTargetName,
    PrototypeBenchmarkRecipe,
    mini_prototype_factory,
)


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


# def test_mini_prototype_benchmark(prototype_benchmark: BenchmarkSession):
#     recipe = PrototypeBenchmarkRecipe(
#         params=PrototypeParameters(), optional_parameters={'test-run': True}
#     )

#     with prototype_benchmark.configure(iterations=3):
#         prototype_benchmark.run(
#             mini_prototype_factory().providers,
#             recipe,
#             BenchmarkTargetName('mini_prototype'),
#         )


@pytest.fixture(params=[1e4, 1e5, 1e6, 1e7], scope='function')
def num_pixels_all_range(request: pytest.FixtureRequest) -> NumPixels:
    return NumPixels(int(request.param))


@pytest.fixture(params=[1e4, 1e5, 1e6, 1e7], scope='function')
def event_rate_all_range(request: pytest.FixtureRequest) -> EventRate:
    return EventRate(int(request.param))


@pytest.fixture
def prototype_recipe_all_range(
    full_benchmark_test: bool,
    num_pixels_all_range: NumPixels,
    event_rate_all_range: EventRate,
) -> PrototypeParameters:
    from .prototype_mini import PrototypeParameters

    assert full_benchmark_test

    return PrototypeParameters(
        num_pixels=num_pixels_all_range, event_rate=event_rate_all_range
    )


@pytest.fixture(scope='function')
def iterations(num_pixels_all_range: NumPixels, event_rate_all_range: EventRate) -> int:
    if num_pixels_all_range >= 1_000_000 or event_rate_all_range >= 1_000_000:
        return 3
    else:
        return 10


def test_mini_prototype_benchmark_all_range(
    prototype_benchmark: BenchmarkSession,
    prototype_recipe_all_range: PrototypeParameters,
    iterations: int,
):
    import scipp as sc

    recipe = PrototypeBenchmarkRecipe(
        params=prototype_recipe_all_range,
        optional_parameters={'scipp-version': sc.__version__},
    )

    with prototype_benchmark.configure(iterations=iterations):
        prototype_benchmark.run(
            mini_prototype_factory().providers,
            recipe,
            BenchmarkTargetName('mini_prototype'),
        )
