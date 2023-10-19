# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Tuple

import pytest

from .benchmark_runner import BenchmarkRunner, R, SingleRunReport


class BeamlimeAppRunner(BenchmarkRunner):
    def __call__(self, *args, **kwargs) -> Tuple[SingleRunReport, R]:
        from .benchmark_runner import (
            BenchmarkResult,
            BenchmarkTargetFunctionName,
            TimeMeasurement,
        )

        return (
            SingleRunReport(
                BenchmarkTargetFunctionName(args[0].__qualname__),
                BenchmarkResult(TimeMeasurement(0, 's')),
                arguments=kwargs,
            ),
            args[0](**kwargs),
        )


@pytest.fixture
def benchmark():
    from .benchmark_runner import BenchmarkSession, create_benchmark_factory

    factory = create_benchmark_factory()
    with factory.temporary_provider(BenchmarkRunner, BeamlimeAppRunner):
        session = factory[BenchmarkSession]
        yield session
        if not session.report.measurements:
            raise RuntimeError("No results!")
        session.save()


def test_benchmark_runner(benchmark):
    def sample_func(a):
        return a

    benchmark.run(sample_func, a=1)
