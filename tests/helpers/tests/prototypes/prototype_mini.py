# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import argparse
import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, Generic, List, NewType, Optional, TypeVar

from beamlime.constructors import Factory, ProviderGroup
from beamlime.logging import BeamlimeLogger
from beamlime.logging.mixins import LogMixin
from tests.benchmarks.environments import BenchmarkTargetName
from tests.benchmarks.runner import (
    BenchmarkResult,
    BenchmarkRunner,
    SingleRunReport,
    TimeMeasurement,
)

from .parameters import ChunkSize, NumFrames, PrototypeParameters
from .random_data_providers import RandomEvents
from .workflows import (
    Events,
    FirstPulseTime,
    Histogrammed,
    MergedData,
    PixelGrouped,
    ReducedData,
    Workflow,
)

TargetCounts = NewType("TargetCounts", int)


def calculate_target_counts(
    num_frames: NumFrames, chunk_size: ChunkSize
) -> TargetCounts:
    import math

    return TargetCounts(math.ceil(num_frames / chunk_size))


class StopWatch(LogMixin):
    logger: BeamlimeLogger

    def __init__(self) -> None:
        self.lapse: dict[str, list[float]] = dict()
        self._start_timestamp: Optional[float] = None
        self._stop_timestamp: Optional[float] = None

    @property
    def duration(self) -> float:
        if self._start_timestamp is None:
            raise TypeError(
                "Start-timestamp is not available. ``start`` was never called."
            )
        elif self._stop_timestamp is None:
            raise TypeError(
                "Stop-timestamp is not available. ``stop`` was never called."
            )
        return self._stop_timestamp - self._start_timestamp

    def start(self) -> None:
        import time

        if self._start_timestamp is None:
            self._start_timestamp = time.time()
        else:
            raise RuntimeError(
                "Start-timestamp is already recorded. "
                "``start`` cannot be called twice."
            )

    def stop(self) -> None:
        import time

        if self._start_timestamp is None:
            raise RuntimeError("``start`` must be called before ``stop``.")
        elif self._stop_timestamp is None:
            self._stop_timestamp = time.time()
        else:
            raise RuntimeError(
                "Stop-timestamp is already recorded. "
                "``stop`` cannot be called twice."
            )

    def lap(self, app_name: str) -> None:
        import time

        app_lapse = self.lapse.setdefault(app_name, [])
        app_lapse.append(time.time())

    @property
    def lap_counts(self) -> dict[str, int]:
        return {app_name: len(app_lapse) for app_name, app_lapse in self.lapse.items()}

    def log_benchmark_result(self):
        self.info("Lap counts: %s", str(self.lap_counts))
        self.info("Benchmark result: %s [s]", self.duration)


class BaseApp(LogMixin, ABC):
    logger: BeamlimeLogger
    stop_watch: StopWatch
    target_counts: TargetCounts

    @property
    def app_name(self) -> str:
        return self.__class__.__name__

    @property
    def data_counts(self) -> int:
        return len(self.stop_watch.lapse.get(self.app_name, []))

    @property
    def target_count_reached(self) -> bool:
        return self.target_counts <= self.data_counts

    async def commit_process(self):
        self.stop_watch.lap(self.app_name)
        await asyncio.sleep(0)

    def data_pipe_monitor(
        self,
        pipe: List[Any],
        timeout: float = 5,
        interval: float = 1 / 14,
        preferred_size: int = 1,
        target_size: int = 1,
    ):
        from beamlime.core.schedulers import async_retry

        @async_retry(
            TimeoutError, max_trials=int(timeout / interval), interval=interval
        )
        async def wait_for_preferred_size() -> None:
            if len(pipe) < preferred_size:
                raise TimeoutError

        async def is_pipe_filled() -> bool:
            try:
                await wait_for_preferred_size()
            except TimeoutError:
                ...
            return len(pipe) >= target_size

        return is_pipe_filled

    @abstractmethod
    async def run(self):
        ...


DataStreamListener = NewType("DataStreamListener", BaseApp)


class DataStreamSimulator(BaseApp):
    raw_data_pipe: List[Events]
    random_events: RandomEvents
    chunk_size: ChunkSize

    def slice_chunk(self) -> Events:
        chunk, self.random_events = (
            Events(self.random_events[: self.chunk_size]),
            RandomEvents(self.random_events[self.chunk_size :]),
        )
        return chunk

    async def run(self) -> None:
        self.stop_watch.start()

        for i_chunk in range(self.target_counts):
            chunk = self.slice_chunk()
            self.raw_data_pipe.append(chunk)
            self.debug("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            await self.commit_process()

        self.info("Data streaming finished...")


InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")


class DataReductionApp(BaseApp, Generic[InputType, OutputType]):
    workflow: Workflow
    input_pipe: List[InputType]
    output_pipe: List[OutputType]

    def __init__(self) -> None:
        self.input_type = self._retrieve_type_arg('input_pipe')
        self.output_type = self._retrieve_type_arg('output_pipe')
        self.first_pulse_time: FirstPulseTime
        super().__init__()

    @classmethod
    def _retrieve_type_arg(cls, attr_name: str) -> type:
        """
        Retrieve type arguments of an attribute with generic type.
        It is only for retrieving input/output pipe type.

        >>> class C(DataReductionApp):
        ...   attr0: list[int]
        ...
        >>> C._retrieve_type_arg('attr0')
        <class 'int'>
        """
        from typing import get_args, get_type_hints

        if not (attr_type := get_type_hints(cls).get(attr_name)):
            raise ValueError(
                f"Class {cls} does not have an attribute "
                f"{attr_name} or it is missing type annotation."
            )
        elif not (type_args := get_args(attr_type)):
            raise TypeError(f"Attribute {attr_name} does not have any type arguments.")
        else:
            return type_args[0]

    def format_received(self, data: InputType) -> str:
        return str(data)

    async def process_every_data(self, data: InputType) -> None:
        self.debug("Received, %s", self.format_received(data))
        with self.workflow.constant_provider(self.input_type, data):
            self.output_pipe.append(self.workflow[self.output_type])

        await self.commit_process()

    def process_first_data(self, data: InputType) -> None:
        ...

    def wrap_up(self, *args, **kwargs) -> Any:
        self.info("No more data coming in. Finishing ...")

    async def run(self) -> None:
        data_monitor = self.data_pipe_monitor(self.input_pipe, target_size=1)
        if not self.target_count_reached and await data_monitor():
            data = self.input_pipe.pop(0)
            self.process_first_data(data)
            await self.process_every_data(data)

        while not self.target_count_reached and await data_monitor():
            data = self.input_pipe.pop(0)
            await self.process_every_data(data)

        self.wrap_up()


class DataMerge(DataReductionApp[InputType, OutputType]):
    input_pipe: List[Events]
    output_pipe: List[MergedData]

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {self.input_type.__name__}"

    def process_first_data(self, data: Events) -> None:
        sample_event = data[0]
        first_pulse_time = sample_event.coords['event_time_zero'][0]
        self.workflow.providers[FirstPulseTime] = lambda: first_pulse_time


class DataBinning(DataReductionApp[InputType, OutputType]):
    input_pipe: List[MergedData]
    output_pipe: List[PixelGrouped]


class DataReduction(DataReductionApp[InputType, OutputType]):
    input_pipe: List[PixelGrouped]
    output_pipe: List[ReducedData]


class DataHistogramming(DataReductionApp[InputType, OutputType]):
    input_pipe: List[ReducedData]
    output_pipe: List[Histogrammed]


class VisualizationDaemon(DataReductionApp[InputType, OutputType]):
    input_pipe: List[Histogrammed]
    output_pipe: Optional[List[None]] = None

    def show(self):
        if not hasattr(self, "fig"):
            raise AttributeError("Please wait until the first figure is created.")
        return self.fig

    def process_first_data(self, data: Histogrammed) -> None:
        import plopp as pp

        self.first_data = data
        self.debug("First data as a seed of histogram: %s", self.first_data)
        self.stream_node = pp.Node(self.first_data)
        self.fig = pp.figure1d(self.stream_node)

    async def process_every_data(self, data: Histogrammed) -> None:
        if data is not self.first_data:
            self.first_data.values += data.values
            self.stream_node.notify_children("update")
            self.debug("Updated plot.")
        await self.commit_process()

    def wrap_up(self, *args, **kwargs) -> Any:
        from matplotlib import pyplot as plt

        self.stop_watch.stop()
        self.stop_watch.log_benchmark_result()
        plt.close()
        return super().wrap_up(*args, **kwargs)


@contextmanager
def asyncio_event_loop() -> Generator[asyncio.AbstractEventLoop, Any, Any]:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    yield loop

    try:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        ...


class BasePrototype(BaseApp, ABC):
    data_stream_listener: DataStreamListener
    data_merger: DataMerge[Events, MergedData]
    data_binner: DataBinning[MergedData, PixelGrouped]
    data_reducer: DataReduction[PixelGrouped, ReducedData]
    data_histogrammer: DataHistogramming[ReducedData, Histogrammed]
    visualizer: VisualizationDaemon

    def collect_sub_daemons(self) -> list[BaseApp]:
        return [
            self.data_stream_listener,
            self.data_merger,
            self.data_binner,
            self.data_reducer,
            self.data_histogrammer,
            self.visualizer,
        ]

    def run(self):
        """
        Collect all coroutines of daemons and schedule them into the event loop.

        Notes
        -----
        **Debugging log while running async daemons under various circumstances.**

        - ``asyncio.get_event_loop`` vs ``asyncio.new_event_loop``
        1. ``asyncio.get_event_loop``
        ``get_event_loop`` will always return the current event loop.
        If there is no event loop set in the thread, it will create a new one
        and set it as a current event loop of the thread, and return the loop.
        Many of ``asyncio`` free functions internally use ``get_event_loop``,
        i.e. ``asyncio.create_task``.

        **Things to be considered while using ``asyncio.get_event_loop``.
          - ``asyncio.create_task`` does not guarantee
            whether the current loop is/will be alive until the task is done.
            You may use ``run_until_complete`` to make sure the loop is not closed
            until the task is finished.
            When you need to throw multiple async calls to the loop,
            use ``asyncio.gather`` to merge all the tasks like in this method.
          - ``close`` or ``stop`` might accidentally destroy/interrupt
            other tasks running in the same event loop.
            i.e. You can accidentally destroy the main event loop of a jupyter kernel.
          - [1]``RuntimeError`` if there has been an event loop set in the
            thread object before but it is now removed.

        2. ``asyncio.new_event_loop``
        ``asyncio.new_event_loop`` will always return the new event loop,
        but it is not set it as a current loop of the thread automatically.

        However, sometimes it is automatically handled within the thread,
        and it caused errors which was hard to be debugged under ``pytest`` session.
        For example,
        - The new event loop was not closed properly as it is destroyed.
        - The new event loop was never started until it is destroyed.
        ``Traceback`` of ``pytest`` did not show
        where exactly the error is from in those cases.
        It was resolved by using ``get_event_loop``,
        or manually closing the event loop at the end of the test.

        **When to use ``asyncio.new_event_loop``.**
          - ``asyncio.get_event_loop`` raises ``RuntimeError``[1]
          - Multi-threads

        Please note that the loop object might need to be ``close``ed manually.
        """
        self.debug('Start running ...')
        with asyncio_event_loop() as loop:
            daemon_coroutines = [daemon.run() for daemon in self.collect_sub_daemons()]
            tasks = [loop.create_task(coro) for coro in daemon_coroutines]

            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*tasks))


Prototype = NewType("Prototype", BasePrototype)


def prototype_app_providers() -> ProviderGroup:
    from beamlime.constructors.providers import SingletonProvider

    app_providers = ProviderGroup(
        SingletonProvider(StopWatch),
        SingletonProvider(VisualizationDaemon),
        SingletonProvider(calculate_target_counts),
    )
    app_providers[DataMerge[Events, MergedData]] = DataMerge
    app_providers[DataBinning[MergedData, PixelGrouped]] = DataBinning
    app_providers[DataReduction[PixelGrouped, ReducedData]] = DataReduction
    app_providers[DataHistogramming[ReducedData, Histogrammed]] = DataHistogramming
    for pipe_type in (Events, PixelGrouped, MergedData, ReducedData, Histogrammed):
        app_providers[List[pipe_type]] = SingletonProvider(list)

    return app_providers


def prototype_base_providers() -> ProviderGroup:
    from beamlime.constructors.providers import merge
    from beamlime.logging.providers import log_providers

    from .parameters import collect_default_param_providers
    from .random_data_providers import random_data_providers
    from .workflows import workflow_providers

    return merge(
        collect_default_param_providers(),
        random_data_providers,
        prototype_app_providers(),
        log_providers,
        workflow_providers,
    )


@contextmanager
def _multiple_constant_providers(
    factory: Factory, constants: Optional[dict[type, Any]] = None
):
    if constants:
        tp, val = constants.popitem()
        with factory.constant_provider(tp, val):
            with multiple_constant_providers(factory, constants):
                yield
    else:
        yield


@contextmanager
def multiple_constant_providers(
    factory: Factory, constants: Optional[dict[type, Any]] = None
):
    from copy import copy  # Use a shallow copy of the constant dictionary

    with _multiple_constant_providers(factory, copy(constants)):
        yield


@contextmanager
def _multiple_temporary_providers(
    factory: Factory, providers: Optional[dict[type, Any]] = None
):
    if providers:
        tp, prov = providers.popitem()
        with factory.temporary_provider(tp, prov):
            with multiple_temporary_providers(factory, providers):
                yield
    else:
        yield


@contextmanager
def multiple_temporary_providers(
    factory: Factory, providers: Optional[dict[type, Any]] = None
):
    from copy import copy  # Use a shallow copy of the provider dictionary

    with _multiple_temporary_providers(factory, copy(providers)):
        yield


def mini_prototype_factory() -> Factory:
    providers = prototype_base_providers()
    providers[Prototype] = BasePrototype
    providers[DataStreamListener] = DataStreamSimulator
    return Factory(providers)


@contextmanager
def temporary_factory(
    prototype_factory: Factory,
    parameters: Optional[dict[type, Any]] = None,
    providers: Optional[dict[type, Any]] = None,
):
    tmp_factory = Factory(prototype_factory.providers)
    with multiple_constant_providers(tmp_factory, parameters):
        with multiple_temporary_providers(tmp_factory, providers):
            yield tmp_factory


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
        arguments = recipe.arguments  # Compose arguments here for earlier failure.

        factory = Factory(providers)
        with multiple_constant_providers(
            factory, constants=recipe.params.as_type_dict()
        ):
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


def prototype_arg_parser() -> argparse.ArgumentParser:
    from beamlime.constructors.inspectors import extract_underlying_type

    parser = argparse.ArgumentParser()
    default_params = PrototypeParameters()

    def wrap_name(name: str) -> str:
        return '--' + name.replace('_', '-')

    parser.add_argument_group('Event Generator Configuration')
    type_name_map = default_params.type_name_map

    for param_type, default_value in default_params.as_type_dict().items():
        parser.add_argument(
            wrap_name(type_name_map[param_type]),
            default=default_value,
            help=f": {param_type}",
            type=extract_underlying_type(param_type),
        )

    return parser


def run_standalone_prototype(
    prototype_factory: Factory, arg_name_space: argparse.Namespace
):
    import logging

    type_name_map = PrototypeParameters().type_name_map
    parameters = {
        field_type: getattr(arg_name_space, field_name)
        for field_type, field_name in type_name_map.items()
    }

    with temporary_factory(
        prototype_factory=prototype_factory,
        parameters=parameters,
    ) as factory:
        factory[BeamlimeLogger].setLevel(logging.DEBUG)
        factory[Prototype].run()


if __name__ == "__main__":
    factory = mini_prototype_factory()
    arg_parser = prototype_arg_parser()

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())
