# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import Callable, Generator, NamedTuple

import pytest

from beamlime.constructors import Factory
from tests.benchmarks.runner import BenchmarkRunner, BenchmarkSession, SingleRunReport

NT = NamedTuple("NT", [('name', str), ('friends', list)])


@dataclass
class A:
    b: float
    c: NT


@dataclass
class B:
    a: A


@pytest.fixture()
def benchmark_session_factory():
    from tests.benchmarks.runner import create_benchmark_session_factory

    class TestRunner(BenchmarkRunner):
        """Test runner with the result of the function as a time measurement result."""

        def __call__(self, func: Callable[..., int], **kwargs) -> SingleRunReport[int]:
            from tests.benchmarks.runner import (
                BenchmarkResult,
                BenchmarkTargetName,
                TimeMeasurement,
            )

            output = func(**kwargs)

            return SingleRunReport(
                BenchmarkTargetName(func.__qualname__),
                BenchmarkResult(TimeMeasurement(output, 's')),
                arguments=kwargs,
                output=output,
            )

    return create_benchmark_session_factory(runner_type=TestRunner)


@pytest.fixture()
def benchmark_tmp_path(
    tmp_path: pathlib.Path, benchmark_session_factory: Factory
) -> pathlib.Path:
    from tests.benchmarks.environments import BenchmarkResultFilePath, BenchmarkRootDir

    with benchmark_session_factory.constant_provider(BenchmarkRootDir, tmp_path):
        return benchmark_session_factory[BenchmarkResultFilePath]


@pytest.fixture()
def benchmark_session(
    benchmark_session_factory: Factory, benchmark_tmp_path: pathlib.Path
) -> Generator[BenchmarkSession, None, None]:
    from tests.benchmarks.environments import BenchmarkResultFilePath

    factory = benchmark_session_factory
    with factory.constant_provider(BenchmarkResultFilePath, benchmark_tmp_path):
        session = factory[BenchmarkSession]
        yield session


def test_collecting_result_paths(
    benchmark_session: BenchmarkSession, benchmark_tmp_path: pathlib.Path
):
    from tests.benchmarks.loader import collect_result_paths

    def sample_func(a: int, b: float) -> float:
        return a + b

    benchmark_session.run(sample_func, a=1, b=3)
    benchmark_session.save()
    assert collect_result_paths(benchmark_tmp_path.parent)[0] == benchmark_tmp_path


def test_dataclass_reconstruction():
    from dataclasses import asdict

    from tests.benchmarks.loader import reconstruct_nested_dataclass

    original = B(a=A(b=1.0, c=NT('Amy', ['Jacob', 'Charles'])))
    reconstructed = reconstruct_nested_dataclass(asdict(original), root_type=B)
    assert original == reconstructed


def test_dataclass_reconstruction_missing_field_none():
    """Loader fills ``None`` if a field is missing."""
    from dataclasses import asdict

    from tests.benchmarks.loader import reconstruct_nested_dataclass

    nested = B(a=A(b=1.0, c=NT("Amy", ["Jacob", "Charles"])))
    nested_dict = asdict(nested)
    nested_dict['a'].pop('b')
    reconstructed = reconstruct_nested_dataclass(nested_dict, root_type=B)
    assert reconstructed.a.b is None


def test_reconstruct_report(benchmark_session: BenchmarkSession):
    from dataclasses import asdict

    from tests.benchmarks.loader import read_report, reconstruct_report

    def sample_func(a: int, b: float) -> float:
        return a + b

    with benchmark_session.configure(iterations=3):
        for a, b in zip([1, 2, 3], [3, 2, 1], strict=True):
            benchmark_session.run(sample_func, a=a, b=b)

    benchmark_session.save()
    raw_report = read_report(benchmark_session.file_manager.file_path)
    reconstructed = reconstruct_report(raw_report)
    assert asdict(benchmark_session.report) == asdict(reconstructed)
