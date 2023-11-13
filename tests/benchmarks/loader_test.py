# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib
from dataclasses import dataclass
from typing import NamedTuple

import pytest

from .runner import BenchmarkSession

# The following functions are imported as pytest fixtures.
from .runner_test import benchmark, benchmark_tmp_path  # noqa: F401

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
    benchmark: BenchmarkSession, benchmark_tmp_path: pathlib.Path  # noqa: F811
):
    from .loader import collect_result_paths

    benchmark.run(sample_function, a=1, b=3)
    benchmark.save()
    assert collect_result_paths(benchmark_tmp_path.parent)[0] == benchmark_tmp_path


def test_benchmark_report_as_dataset(benchmark: BenchmarkSession):  # noqa: F811
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


def test_reconstruct_report(benchmark: BenchmarkSession):  # noqa: F811
    from dataclasses import asdict

    from .loader import reconstruct_report

    with benchmark.configure(iterations=3):
        for a, b in zip([1, 2, 3], [3, 2, 1]):
            benchmark.run(sample_function, a=a, b=b)

    benchmark.save()
    reconstructed = reconstruct_report(benchmark.file_manager.file_path)
    assert asdict(benchmark.report) == asdict(reconstructed)


def test_reconstruct_report_as_dataset(benchmark: BenchmarkSession):  # noqa: F811
    import scipp as sc

    from .loader import reconstruct_report

    with benchmark.configure(iterations=3):
        for a, b in zip([1, 2, 3], [3, 2, 1]):
            benchmark.run(sample_function, a=a, b=b)

    benchmark.save()
    reconstructed = reconstruct_report(benchmark.file_manager.file_path)
    ds = reconstructed.asdataset()
    assert sc.identical(ds, benchmark.report.asdataset())
