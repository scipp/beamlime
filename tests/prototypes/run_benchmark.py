# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from contextlib import contextmanager
from typing import Any, Generator

from beamlime.constructors import Factory

from ..benchmarks.runner import BenchmarkSession
from .parameters import EventRate, NumPixels, PrototypeParameters
from .prototype_mini import (
    BenchmarkTargetName,
    PrototypeBenchmarkRecipe,
    mini_prototype_factory,
)


def mini_factory() -> Generator[Factory, None, None]:
    from .prototype_mini import mini_prototype_factory

    yield mini_prototype_factory()


@contextmanager
def prototype_benchmark() -> Generator[BenchmarkSession, Any, Any]:
    from ..benchmarks.runner import BenchmarkRunner, create_benchmark_runner_factory
    from .prototype_mini import PrototypeRunner

    benchmark_factory = create_benchmark_runner_factory()
    with benchmark_factory.temporary_provider(BenchmarkRunner, PrototypeRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]
        yield benchmark_session

        benchmark_session.save()


def prototype_recipe_all_range() -> Generator[PrototypeParameters, None, None]:
    from itertools import product

    from .prototype_mini import PrototypeParameters

    event_rates = [10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
    num_pixels = [10_000, 100_000, 1_000_000, 10_000_000, 20_000_000]

    for event_rate, num_pixel in product(event_rates, num_pixels):
        yield PrototypeParameters(
            num_pixels=NumPixels(num_pixel), event_rate=EventRate(event_rate)
        )


if __name__ == "__main__":
    import asyncio

    from rich.pretty import pretty_repr
    from rich.progress import Progress

    param_combinations = list(prototype_recipe_all_range())
    loop = asyncio.set_event_loop(asyncio.new_event_loop())

    with Progress() as progress:
        with prototype_benchmark() as benchmark:
            task = progress.add_task(
                "[green]Benchmarking...", total=len(param_combinations)
            )
            for params in param_combinations:
                progress.advance(task)
                progress.update(
                    task, description=f"[green]Benchmarking... \n{pretty_repr(params)}"
                )
                recipe = PrototypeBenchmarkRecipe(params=params)

                with benchmark.configure(iterations=3):
                    benchmark.run(
                        mini_prototype_factory().providers,
                        recipe,
                        BenchmarkTargetName('mini_prototype'),
                    )
                    benchmark.save()
