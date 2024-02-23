# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Optional

import pytest

from beamlime.applications._parameters import EventRate, NumPixels, PrototypeParameters
from beamlime.constructors import Factory, ProviderGroup, multiple_constant_providers
from tests.benchmarks.environments import BenchmarkTargetName
from tests.benchmarks.runner import BenchmarkRunner, BenchmarkSession, SingleRunReport


@dataclass
class PrototypeBenchmarkRecipe:
    params: PrototypeParameters
    optional_parameters: Optional[dict] = None

    @property
    def arguments(self) -> dict[str, Any]:
        from dataclasses import asdict

        arguments = {
            contant_name: constant_value
            for contant_name, constant_value in asdict(self.params).items()
        }
        optional_info = self.optional_parameters or {}

        optional_param_keys = set(optional_info.keys())
        prototype_param_keys = set(arguments.keys())

        if self.optional_parameters and (
            overlapped := optional_param_keys.intersection(prototype_param_keys)
        ):
            raise ValueError(
                "Optional parameters have overlapping keys as prototype parameters.",
                overlapped,
            )
        else:
            arguments.update(self.optional_parameters or {})
            return arguments


class PrototypeRunner(BenchmarkRunner):
    def __call__(
        self,
        providers: ProviderGroup,
        recipe: PrototypeBenchmarkRecipe,
        prototype_name: Optional[BenchmarkTargetName] = None,
    ) -> SingleRunReport:
        from beamlime.applications.handlers import StopWatch
        from beamlime.constructors import SingletonProvider
        from beamlime.executables.prototypes import Prototype
        from tests.benchmarks.runner import BenchmarkResult, TimeMeasurement

        arguments = recipe.arguments  # Compose arguments here for earlier failure.

        factory = Factory(providers)
        with multiple_constant_providers(
            factory, constants=recipe.params.as_type_dict()
        ):
            with factory.temporary_provider(StopWatch, SingletonProvider(StopWatch)):
                output = factory[Prototype].run()
                time_consumed = factory[StopWatch].duration

                return SingleRunReport(
                    callable_name=prototype_name or BenchmarkTargetName(''),
                    benchmark_result=BenchmarkResult(
                        time=TimeMeasurement(value=time_consumed, unit='s')
                    ),
                    arguments=arguments,
                    output=output,
                )


@pytest.fixture
def mini_factory(tmp_path: Path) -> Generator[Factory, None, None]:
    from beamlime.applications.handlers import ImagePath
    from beamlime.executables.prototypes import mini_prototype_factory

    factory = mini_prototype_factory()
    with factory.constant_provider(
        ImagePath, ImagePath(tmp_path / "mini_prototype.png")
    ):
        yield factory


@pytest.fixture
def kafka_factory(kafka_test: bool) -> Generator[Factory, None, None]:
    from tests._prototypes.prototype_kafka import kafka_prototype_factory

    assert kafka_test
    yield kafka_prototype_factory()


def prototype_test_helper(prototype_factory: Factory):
    prototype_runner = PrototypeRunner()

    recipe = PrototypeBenchmarkRecipe(params=PrototypeParameters())
    prototype_runner(prototype_factory.providers, recipe)


def test_mini_prototype(mini_factory: Factory):
    prototype_test_helper(mini_factory)


def test_kafka_prototype(kafka_factory: Factory):
    prototype_test_helper(kafka_factory)


def test_mini_prototype_benchmark(mini_factory: Factory):
    """Test benchmark runner and discard the results."""
    from tests.benchmarks.runner import create_benchmark_session_factory

    benchmark_factory = create_benchmark_session_factory()
    recipe = PrototypeBenchmarkRecipe(
        params=PrototypeParameters(), optional_parameters={'test-run': True}
    )

    with benchmark_factory.temporary_provider(BenchmarkRunner, PrototypeRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]

        with benchmark_session.configure(iterations=3, auto_save=False):
            benchmark_session.run(
                mini_factory.providers,
                recipe,
                BenchmarkTargetName('mini_prototype'),
            )


@pytest.fixture(scope='session')
def prototype_benchmark(benchmark_test: bool) -> Generator[BenchmarkSession, Any, Any]:
    from tests.benchmarks.runner import create_benchmark_session_factory

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
    assert full_benchmark_test

    return PrototypeParameters(
        num_pixels=num_pixels_all_range, event_rate=event_rate_all_range
    )


def test_mini_prototype_benchmark_all_range(
    mini_factory: Factory,
    prototype_benchmark: BenchmarkSession,
    prototype_recipe_all_range: PrototypeParameters,
):
    import scipp as sc

    recipe = PrototypeBenchmarkRecipe(
        params=prototype_recipe_all_range,
        optional_parameters={'scipp-version': sc.__version__},
    )

    with prototype_benchmark.configure(iterations=3):
        prototype_benchmark.run(
            mini_factory.providers,
            recipe,
            BenchmarkTargetName('mini_prototype'),
        )
