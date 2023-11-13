# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import Callable, NamedTuple

import pytest

from .environments import BenchmarkResultFilePath
from .runner import BenchmarkRunner, BenchmarkSession, SingleRunReport


class TestRunner(BenchmarkRunner):
    """Test runner with the result of the function as a time measurement result."""

    def __call__(self, func: Callable[..., int], **kwargs) -> SingleRunReport[int]:
        from .runner import BenchmarkResult, BenchmarkTargetName, TimeMeasurement

        output = func(**kwargs)

        return SingleRunReport(
            BenchmarkTargetName(func.__qualname__),
            BenchmarkResult(TimeMeasurement(output, 's')),
            arguments=kwargs,
            output=output,
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
    assert saved['measurements']['time']['value'] == [1]
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
    assert saved['measurements']['time']['value'] == [1, 1]
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


NT = NamedTuple("NT", [('name', str), ('friends', list)])


@dataclass
class A:
    b: float
    c: NT


@dataclass
class B:
    a: A


def sample_function(a: int, b: float) -> float:
    return a + b


def test_collecting_result_paths(
    benchmark: BenchmarkSession, benchmark_tmp_path: pathlib.Path
):
    from .loader import collect_result_paths

    benchmark.run(sample_function, a=1, b=3)
    benchmark.save()
    assert collect_result_paths(benchmark_tmp_path.parent)[0] == benchmark_tmp_path


def test_benchmark_report_as_dataset(benchmark: BenchmarkSession):
    import scipp as sc

    def sample_func(a: int, b: int) -> int:
        return a if a > b else b

    a_cands = [1, 2, 3]
    b_cands = [3, 1, 2]
    expected_times = [sample_func(a, b) for a, b in zip(a_cands, b_cands)]

    for a, b, t in zip(a_cands, b_cands, expected_times):
        assert benchmark.run(sample_func, a=a, b=b) == t

    expected_data = sc.array(dims=['run'], values=expected_times, unit='s')
    expected_coords = {
        'target-name': sc.array(dims=['run'], values=[sample_func.__qualname__] * 3),
        'a': sc.array(dims=['run'], values=a_cands, unit=None),
        'b': sc.array(dims=['run'], values=b_cands, unit=None),
    }

    ds = benchmark.report.asdataset()
    assert sc.identical(
        ds, sc.Dataset({'time': sc.DataArray(expected_data, coords=expected_coords)})
    )


def test_dataclass_reconstruction():
    from dataclasses import asdict

    from .loader import reconstruct_nested_dataclass

    original = B(a=A(b=1.0, c=NT('Amy', ['Jacob', 'Charles'])))
    reconstructed = reconstruct_nested_dataclass(asdict(original), root_type=B)
    assert original == reconstructed


def test_dataclass_reconstruction_wrong_fields_raises():
    from dataclasses import asdict

    from .loader import reconstruct_nested_dataclass

    nested = B(a=A(b=1.0, c=NT("Amy", ["Jacob", "Charles"])))
    nested_dict = asdict(nested)
    nested_dict['a']['b'] = None
    with pytest.raises(ValueError):
        reconstruct_nested_dataclass(nested_dict, root_type=B)


def test_reconstruct_report(benchmark: BenchmarkSession):
    from dataclasses import asdict

    from .loader import reconstruct_report

    with benchmark.configure(iterations=3):
        for a, b in zip([1, 2, 3], [3, 2, 1]):
            benchmark.run(sample_function, a=a, b=b)

    benchmark.save()
    reconstructed = reconstruct_report(benchmark.file_manager.file_path)
    assert asdict(benchmark.report) == asdict(reconstructed)


def test_reconstruct_report_as_dataset(benchmark: BenchmarkSession):
    import scipp as sc

    from .loader import reconstruct_report

    with benchmark.configure(iterations=3):
        for a, b in zip([1, 2, 3], [3, 2, 1]):
            benchmark.run(sample_function, a=a, b=b)

    benchmark.save()
    reconstructed = reconstruct_report(benchmark.file_manager.file_path)
    ds = reconstructed.asdataset()
    assert sc.identical(ds, benchmark.report.asdataset())
