# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Callable, Optional, Tuple, TypeVar

from beamlime.constructors import Factory, ProviderGroup

from .benchmark_env import (
    BenchmarkEnvironment,
    BenchmarkResultFilePath,
    BenchmarkTargetFunctionName,
    env_providers,
)

session_providers = ProviderGroup()


@dataclass
class TimeMeasurement:
    """Time measurement results."""

    value: float
    unit: str


@dataclass
class SpaceMeasurement:
    """Memory measurement results."""

    value: float
    unit: str


@dataclass
class BenchmarkResult:  # Measurement results should always have value and unit.
    time: TimeMeasurement
    space: Optional[SpaceMeasurement] = None


_Item = TypeVar("_Item")


def _append_row(
    obj: dict[str, list[Optional[_Item]]], row: dict[str, _Item]
) -> dict[str, list[Optional[_Item]]]:
    """
    Helper function to extend Pandas Dataframe-like dictionary.
    All columns(Corresponding to the highest level keys)
    in the ``obj`` should always have the same length of rows.
    """
    # Reference count of rows.
    _ref = min(len(v) for _, v in obj.items()) if obj else 0

    for missing_k in (k for k in obj.keys() if k not in row):
        # Append ``None`` if there is an existing key
        # that does not exist in ``row``.
        obj[missing_k].append(None)

    for k, v in row.items():
        # Fills with ``[None]*_ref`` if there is a new key
        # that does not exist in the ``obj``.
        obj.setdefault(k, [None] * _ref).append(v)

    return obj


@dataclass
class SingleRunReport:
    callable_name: BenchmarkTargetFunctionName
    benchmark_result: BenchmarkResult
    arguments: dict
    extra: Optional[dict] = None

    def attach_extra(self, extra_field: str, extra_info: Any) -> None:
        """Extra information attachment interface for flexible logging."""
        if self.extra is None:
            self.extra = dict()

        if extra_field in self.extra:
            raise KeyError(f"{extra_field} already exists. Can't update the values.")
        else:
            self.extra[extra_field] = extra_info


@dataclass
class BenchmarkReport:
    """Benchmark report template."""

    environment: BenchmarkEnvironment
    function_names: list[BenchmarkTargetFunctionName]
    measurements: dict[str, dict[str, list]]
    arguments: dict[str, list]
    extra: dict[str, Any]

    def append(self, single_run_result: SingleRunReport) -> None:
        self.function_names.append(single_run_result.callable_name)
        measurement = asdict(single_run_result.benchmark_result)
        for meas_dim in measurement:
            _append_row(
                self.measurements.setdefault(meas_dim, dict(value=[], unit=[])),
                measurement.get(meas_dim) or dict(value=None, unit=None),
            )
        _append_row(self.arguments, single_run_result.arguments)
        if single_run_result.extra is not None:
            _append_row(self.extra, single_run_result.extra)


@dataclass
class BenchmarkReportContainer(BenchmarkReport):
    """Empty benchmark report for writing results."""

    def __init__(self) -> None:
        self.function_names = list()
        self.measurements = dict()
        self.arguments = dict()
        self.extra = dict()


class BenchmarkRunner(ABC):
    """Abstract benchmark runner class."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> Tuple[SingleRunReport, Any]:
        """Runner interface called by ``BenchmarkSession``.

        BenchmarkRunner should always return a tuple of
        ``BenchmarkResult`` and the result of the call.
        """
        ...


R = TypeVar("R")


class SimpleRunner(BenchmarkRunner):
    """Benchmark runner that simply measures duration of a function call."""

    def __call__(
        self, func: Callable[..., R], *args, **kwargs
    ) -> Tuple[SingleRunReport, R]:
        import inspect
        import time

        start = time.time()
        call_result = func(*args, **kwargs)
        stop = time.time()

        function_name = func.__qualname__
        function_arguments = inspect.signature(func).bind(*args, **kwargs).arguments

        single_report = SingleRunReport(
            BenchmarkTargetFunctionName(function_name),
            BenchmarkResult(time=TimeMeasurement(value=stop - start, unit='s')),
            function_arguments,
        )
        return single_report, call_result


class BenchmarkFileManager(ABC):
    file_path: BenchmarkResultFilePath

    @abstractmethod
    def save(
        self, report: BenchmarkReport, path: Optional[BenchmarkResultFilePath] = None
    ) -> None:
        ...

    @abstractmethod
    def load(self, path: Optional[BenchmarkResultFilePath] = None) -> BenchmarkReport:
        ...


class SimpleFileManager(BenchmarkFileManager):
    def save(
        self, report: BenchmarkReport, path: Optional[BenchmarkResultFilePath] = None
    ) -> None:
        import json

        file_path = path or self.file_path
        file_path.write_text(json.dumps(asdict(report), indent=2) + '\n')

    def load(self, path: Optional[BenchmarkResultFilePath] = None) -> BenchmarkReport:
        import json

        file_path = path or self.file_path
        return BenchmarkReport(**json.loads(file_path.read_text()))


session_providers[BenchmarkReport] = BenchmarkReportContainer
session_providers[BenchmarkRunner] = SimpleRunner
session_providers[BenchmarkFileManager] = SimpleFileManager


@session_providers.provider
@dataclass
class BenchmarkSession:
    """Benchmark session handling class.

    It works as a bridge object between
    ``BenchmarkReport``, ``BenchmarkRunner`` and ``BenchmarkFileManager``.
    """

    report: BenchmarkReport
    runner: BenchmarkRunner
    file_manager: BenchmarkFileManager

    def run(self, *runner_args, **parameters):
        single_report, call_result = self.runner(*runner_args, **parameters)
        self.report.append(single_report)
        return call_result

    def save(self):
        self.file_manager.save(self.report)


def create_benchmark_factory() -> Factory:
    return Factory(env_providers, session_providers)
