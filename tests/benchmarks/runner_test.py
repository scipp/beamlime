# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib
from typing import Callable

import pytest

from .environments import BenchmarkResultFilePath
from .runner import BenchmarkRunner, BenchmarkSession, R, SingleRunReport


class TestRunner(BenchmarkRunner):
    """Test runner that always has 0 as a time measurement result."""

    def __call__(self, func: Callable[..., R], **kwargs) -> SingleRunReport[R]:
        from .runner import BenchmarkResult, BenchmarkTargetName, TimeMeasurement

        return SingleRunReport(
            BenchmarkTargetName(func.__qualname__),
            BenchmarkResult(TimeMeasurement(0, 's')),
            arguments=kwargs,
            output=func(**kwargs),
        )


@pytest.fixture(scope='function')
def benchmark_tmp_path(tmp_path: pathlib.Path) -> BenchmarkResultFilePath:
    from .environments import BenchmarkRootDir
    from .runner import create_benchmark_runner_factory

    factory = create_benchmark_runner_factory()
    with factory.constant_provider(BenchmarkRootDir, tmp_path):
        return factory[BenchmarkResultFilePath]


@pytest.fixture(scope='function')
def benchmark(benchmark_tmp_path: BenchmarkResultFilePath):
    from .runner import BenchmarkSession, create_benchmark_runner_factory

    factory = create_benchmark_runner_factory(runner_type=TestRunner)
    with factory.constant_provider(BenchmarkResultFilePath, benchmark_tmp_path):
        session = factory[BenchmarkSession]
        yield session
        if not session.report.measurements:
            raise RuntimeError("No results!")


def _load_results(benchmark_tmp_path: BenchmarkResultFilePath) -> dict:
    import json

    return json.loads(benchmark_tmp_path.read_text())


def test_benchmark_runner(
    benchmark: BenchmarkSession, benchmark_tmp_path: BenchmarkResultFilePath
):
    def sample_func(a: int) -> int:
        return a

    assert benchmark.run(sample_func, a=1) == 1
    benchmark.save()
    saved = _load_results(benchmark_tmp_path)
    assert saved['measurements']['time']['value'] == [0]
    assert saved['arguments']['a'] == [1]


def test_benchmark_runner_multi_arguments(
    benchmark: BenchmarkSession, benchmark_tmp_path: BenchmarkResultFilePath
):
    def sample_func(a: int, **_) -> int:  # Allows arbitrary keyword arguments
        return a

    assert benchmark.run(sample_func, a=1) == 1
    assert benchmark.run(sample_func, a=1, b=1) == 1
    benchmark.save()
    saved = _load_results(benchmark_tmp_path)
    assert saved['measurements']['time']['value'] == [0, 0]
    assert saved['arguments']['a'] == [1, 1]
    assert saved['arguments']['b'] == [None, 1]


def test_benchmark_runner_multi_iterations(
    benchmark: BenchmarkSession, benchmark_tmp_path: BenchmarkResultFilePath
):
    def sample_func(a: int) -> int:
        return a

    with benchmark.configure(iterations=3):
        assert benchmark.run(sample_func, a=1) == 1

    benchmark.save()
    saved = _load_results(benchmark_tmp_path)
    assert len(saved['measurements']['time']['value']) == 3
