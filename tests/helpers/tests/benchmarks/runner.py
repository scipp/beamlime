# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Callable, Generic, NewType, Optional, TypeVar

from beamlime.constructors import Factory, ProviderGroup

from .environments import (
    BenchmarkEnvironment,
    BenchmarkResultFilePath,
    BenchmarkTargetName,
    env_providers,
)


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


R = TypeVar("R")


@dataclass
class SingleRunReport(Generic[R]):
    callable_name: BenchmarkTargetName
    benchmark_result: BenchmarkResult
    arguments: dict
    output: Optional[R] = None


@dataclass  # Need dataclass decorator to use ``as_dict``.
class BenchmarkReport:
    """Benchmark report template."""

    environment: BenchmarkEnvironment
    target_names: list[BenchmarkTargetName]
    measurements: dict[str, dict[str, list]]
    arguments: dict[str, list]

    def __init__(self) -> None:
        self.target_names = list()
        self.measurements = dict()
        self.arguments = dict()

    def append_measurement(self, result: BenchmarkResult) -> None:
        measurement = asdict(result)
        for meas_dim in measurement:
            _append_row(
                self.measurements.setdefault(meas_dim, dict(value=[], unit=[])),
                measurement.get(meas_dim) or dict(value=None, unit=None),
            )

    def append(self, single_run_result: SingleRunReport) -> None:
        self.target_names.append(single_run_result.callable_name)
        self.append_measurement(single_run_result.benchmark_result)
        _append_row(self.arguments, single_run_result.arguments)


class BenchmarkRunner(ABC):
    """Abstract benchmark runner class."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> SingleRunReport:
        """Runner interface called by ``BenchmarkSession``.

        BenchmarkRunner should always return a ``SingleRunReport``
        which contains the returned value(object) of the call.
        """
        ...


class SimpleRunner(BenchmarkRunner):
    """Benchmark runner that simply measures duration of a function call."""

    def __call__(self, func: Callable[..., R], *args, **kwargs) -> SingleRunReport[R]:
        import inspect
        import time

        start = time.time()
        call_result = func(*args, **kwargs)
        stop = time.time()

        function_name = func.__qualname__
        function_arguments = inspect.signature(func).bind(*args, **kwargs).arguments

        return SingleRunReport(
            BenchmarkTargetName(function_name),
            BenchmarkResult(time=TimeMeasurement(value=stop - start, unit='s')),
            arguments=function_arguments,
            output=call_result,
        )


class BenchmarkFileManager(ABC):
    """Abstract file manager with ``save``/``load`` methods pair."""

    def __init__(self, file_path: BenchmarkResultFilePath) -> None:
        self.file_path = file_path

    @abstractmethod
    def save(
        self, report: BenchmarkReport, path: Optional[BenchmarkResultFilePath] = None
    ) -> None: ...

    @abstractmethod
    def load(
        self, path: Optional[BenchmarkResultFilePath] = None
    ) -> BenchmarkReport: ...


class SimpleFileManager(BenchmarkFileManager):
    def save(self, report: BenchmarkReport) -> None:
        import json

        self.file_path.write_text(json.dumps(asdict(report), indent=2) + '\n')

    def load(self) -> BenchmarkReport:
        import json

        return BenchmarkReport(**json.loads(self.file_path.read_text()))


BenchmarkIterations = NewType("BenchmarkIterations", int)
AutoSaveFlag = NewType("AutoSaveFlag", bool)


@dataclass
class BenchmarkSessionConfiguration:
    """
    Parameters
    ----------
    iterations:
        Number of iterations to call a benchmark runner.

    auto_save:
        Whether to save the result after each iteration.

    """

    iterations: BenchmarkIterations = BenchmarkIterations(1)
    auto_save: AutoSaveFlag = AutoSaveFlag(True)


@dataclass
class BenchmarkSession:
    """Benchmark session handling class."""

    report: BenchmarkReport
    runner: BenchmarkRunner
    file_manager: BenchmarkFileManager
    configurations: BenchmarkSessionConfiguration

    def _update_configurations(self, **configs) -> None:
        for config_name, config_value in configs.items():
            setattr(self.configurations, config_name, config_value)

    @contextmanager
    def configure(self, **tmp_configurations):
        """Temporarily replaces the session configurations.

        See ``BenchmarkSessionConfiguration`` for available options.
        """
        original_configurations = {
            config_name: getattr(self.configurations, config_name)
            for config_name in tmp_configurations
        }
        self._update_configurations(**tmp_configurations)
        yield None
        self._update_configurations(**original_configurations)

    def run(self, *runner_args, **parameters):
        """Call ``self.runner`` with arguments and save the result.

        Calls ``self.runner``: ``BenchmarkRunner``
        with unpacked arguments, ``runner_args`` and ``parameters``.
        ``self.runner`` generates a single benchmark report (``SingleRunReport``).
        The result will be appended to the ``self.report``: ``BenchmarkReport``.

        Use ``self.configure`` to use non-default configurations.
        Configurable options should be handled by ``BenchmarkSessionConfiguration``
        instead of having extra arguments in ``run`` methods.
        So that all arguments of ``run`` can be directly passed to ``BenchmarkRunner``.
        """
        single_report: Optional[SingleRunReport] = None

        for _ in range(self.configurations.iterations):
            single_report = self.runner(*runner_args, **parameters)
            self.report.append(single_report)
            if self.configurations.auto_save:
                self.save()

        return single_report.output if single_report else None

    def save(self):
        """Save the accumulated benchmark results in the container (``self.report``).

        Calls  ``self.file_manager.save`` with ``self.report``,
        so that ``BenchmarkFileManager`` can dump the result into a file.
        """
        self.file_manager.save(self.report)


def create_benchmark_session_factory(
    runner_type: type[BenchmarkRunner] = SimpleRunner,
) -> Factory:
    """Collect all providers for ``BenchmarkSession`` and create a factory."""
    session_providers = ProviderGroup()
    session_providers[BenchmarkSession] = BenchmarkSession
    session_providers[BenchmarkReport] = BenchmarkReport
    session_providers[BenchmarkRunner] = runner_type
    session_providers[BenchmarkFileManager] = SimpleFileManager
    session_providers[BenchmarkSessionConfiguration] = BenchmarkSessionConfiguration
    return Factory(env_providers, session_providers)
