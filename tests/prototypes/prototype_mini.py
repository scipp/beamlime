# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Generic, List, NewType, Optional, TypeVar

from beamlime.constructors import Factory, ProviderGroup
from beamlime.logging import BeamlimeLogger
from beamlime.logging.mixins import LogMixin

from .parameters import ChunkSize, EventRate, NumFrames, NumPixels
from .random_data_providers import RandomEvents
from .workflows import (
    BinnedData,
    Events,
    Histogrammed,
    MergedData,
    ReducedData,
    Workflow,
)

TargetCounts = NewType("TargetCounts", int)


def calculate_target_counts(
    num_frames: NumFrames, chunk_size: ChunkSize
) -> TargetCounts:
    import math

    return TargetCounts(math.ceil(num_frames / chunk_size))


class StopWatch:
    def __init__(self) -> None:
        self.lapse: dict[str, list[float]] = dict()
        self._start_timestamp: Optional[float] = None
        self._stop_timestamp: Optional[float] = None

    @property
    def laps_counts(self) -> int:
        if not self.lapse:
            raise Warning("No time lapse recorded. Did  you forget to call ``lap()``?")

        return (
            min([len(app_lapse) for app_lapse in self.lapse.values()])
            if self.lapse
            else 0
        )

    @property
    def start_timestamp(self) -> float:
        if self._start_timestamp is None:
            raise TypeError(
                "Start-timestamp is not available. " "``start`` was never called."
            )
        else:
            return self._start_timestamp

    @property
    def stop_timestamp(self) -> float:
        if self._stop_timestamp is None:
            raise TypeError(
                "Stop-timestamp is not available. " "``stop`` was never called."
            )
        else:
            return self._stop_timestamp

    def start(self) -> None:
        import time

        try:
            self.start_timestamp
        except TypeError:
            self._start_timestamp = time.time()
        else:
            raise RuntimeError(
                "Start-timestamp is already recorded. "
                "``start`` cannot be called twice."
            )

    def stop(self) -> None:
        import time

        try:
            self.start_timestamp
        except TypeError:
            raise RuntimeError("``start`` should be called before ``stop``.")

        try:
            self.stop_timestamp
        except TypeError:
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


class BaseApp(LogMixin, ABC):
    logger: BeamlimeLogger
    stop_watch: StopWatch
    target_counts: TargetCounts

    @property
    def app_name(self) -> str:
        return self.__class__.__name__

    @property
    def target_count_reached(self) -> bool:
        return self.target_counts <= self.data_counts

    def __init__(self) -> None:
        self.data_counts: int = 0
        super().__init__()

    async def commit_process(self):
        self.stop_watch.lap(self.app_name)
        self.data_counts += 1
        await asyncio.sleep(0)

    def data_pipe_monitor(
        self,
        pipe: List[Any],
        timeout: float = 5,
        interval: float = 1 / 14,
        prefered_size: int = 1,
        target_size: int = 1,
    ):
        from beamlime.core.schedulers import async_retry

        @async_retry(
            TimeoutError, max_trials=int(timeout / interval), interval=interval
        )
        async def wait_for_preferred_size() -> None:
            if len(pipe) < prefered_size:
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


class DataReductionMixin(BaseApp, ABC, Generic[InputType, OutputType]):
    workflow: Workflow
    input_pipe: List[InputType]
    output_pipe: List[OutputType]

    def __init__(self) -> None:
        self.input_type = self._retrieve_type_arg('input_pipe')
        self.output_type = self._retrieve_type_arg('output_pipe')
        self.data_counts = 0
        super().__init__()

    @classmethod
    def _retrieve_type_arg(cls, attr_name: str) -> type:
        from typing import get_args, get_type_hints

        return get_args(get_type_hints(cls)[attr_name])[0]

    def format_received(self, data: InputType) -> str:
        return str(data)

    async def run(self) -> None:
        data_monitor = self.data_pipe_monitor(self.input_pipe, target_size=1)
        while not self.target_count_reached and await data_monitor():
            data = self.input_pipe.pop(0)
            self.debug("Received, %s", self.format_received(data))

            with self.workflow.constant_provider(self.input_type, data):
                self.output_pipe.append(self.workflow[self.output_type])

            await self.commit_process()

        self.info("No more data coming in. Finishing ...")


class DataMerge(DataReductionMixin[InputType, OutputType]):
    input_pipe: List[Events]
    output_pipe: List[MergedData]

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {self.input_type.__name__}"


class DataBinning(DataReductionMixin[InputType, OutputType]):
    input_pipe: List[MergedData]
    output_pipe: List[BinnedData]


class DataReduction(DataReductionMixin[InputType, OutputType]):
    input_pipe: List[BinnedData]
    output_pipe: List[ReducedData]


class DataHistogramming(DataReductionMixin[InputType, OutputType]):
    input_pipe: List[ReducedData]
    output_pipe: List[Histogrammed]


class VisualizationDaemon(BaseApp):
    visualized_data_pipe: List[Histogrammed]

    def show(self):
        if not hasattr(self, "fig"):
            raise AttributeError("Please wait until the first figure is created.")
        return self.fig

    async def run(self) -> None:
        import plopp as pp

        data_monitor = self.data_pipe_monitor(self.visualized_data_pipe)

        if hasattr(self, "fig"):
            del self.fig

        if await data_monitor():
            self.first_data = self.visualized_data_pipe.pop(0)
            self.debug("First data as a seed of histogram: %s", self.first_data)
            self.stream_node = pp.Node(lambda: self.first_data)
            self.fig = pp.figure1d(self.stream_node)
            await self.commit_process()

        while not self.target_count_reached and await data_monitor():
            new_data = self.visualized_data_pipe.pop(0)
            self.first_data.values += new_data.values
            self.stream_node.notify_children("update")
            self.debug("Updated plot.")
            await self.commit_process()

        self.info("No more data coming in. Finishing ...")

        self.stop_watch.stop()
        try:
            assert self.stop_watch.laps_counts == self.target_counts
        except AssertionError:
            self.error(
                "Target data counts not reached. %s/%s",
                self.stop_watch.laps_counts,
                self.target_counts,
            )
        else:
            self.info(
                "Benchmark result: %s",
                self.stop_watch.stop_timestamp - self.stop_watch.start_timestamp,
            )


class BasePrototype(BaseApp, ABC):
    data_stream_listener: DataStreamListener
    data_merge: DataMerge[Events, MergedData]
    data_binning: DataBinning[MergedData, BinnedData]
    data_reduction: DataReduction[BinnedData, ReducedData]
    data_plotter: DataHistogramming[ReducedData, Histogrammed]
    visualizer: VisualizationDaemon

    def collect_sub_daemons(self) -> list[BaseApp]:
        return [
            self.data_stream_listener,
            self.data_merge,
            self.data_binning,
            self.data_reduction,
            self.data_plotter,
            self.visualizer,
        ]

    def run(self):
        self.debug('Start running ...')
        daemon_coroutines = [daemon.run() for daemon in self.collect_sub_daemons()]

        try:
            asyncio.get_running_loop()
            for daemon_job in daemon_coroutines:
                asyncio.create_task(daemon_job)

        except RuntimeError:
            loop = asyncio.new_event_loop()
            for daemon_job in daemon_coroutines[:-1]:
                loop.create_task(daemon_job)
            loop.run_until_complete(daemon_coroutines[-1])


Prototype = NewType("Prototype", BasePrototype)


def prototype_app_providers() -> ProviderGroup:
    app_providers = ProviderGroup()
    app_providers[DataMerge[Events, MergedData]] = DataMerge
    app_providers[DataBinning[MergedData, BinnedData]] = DataBinning
    app_providers[DataReduction[BinnedData, ReducedData]] = DataReduction
    app_providers[DataHistogramming[ReducedData, Histogrammed]] = DataHistogramming

    app_providers.cached_provider(StopWatch, StopWatch)
    app_providers.cached_provider(VisualizationDaemon, VisualizationDaemon)
    app_providers.cached_provider(TargetCounts, calculate_target_counts)
    for pipe_type in (Events, BinnedData, MergedData, ReducedData, Histogrammed):
        app_providers.cached_provider(List[pipe_type], list)

    return app_providers


def prototype_base_providers() -> ProviderGroup:
    from beamlime.constructors.providers import merge
    from beamlime.logging.providers import log_providers

    from .parameters import default_param_providers
    from .random_data_providers import random_data_providers
    from .workflows import workflow_providers

    return merge(
        default_param_providers,
        random_data_providers,
        prototype_app_providers(),
        log_providers,
        workflow_providers,
    )


@contextmanager
def multiple_constant_providers(
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
def multiple_temporary_providers(
    factory: Factory, providers: Optional[dict[type, Any]] = None
):
    if providers:
        tp, prov = providers.popitem()
        with factory.temporary_provider(tp, prov):
            with multiple_temporary_providers(factory, providers):
                yield
    else:
        yield


def base_factory() -> Factory:
    prototype_providers = prototype_base_providers()
    return Factory(prototype_providers)


def run_prototype(
    prototype_factory: Factory,
    parameters: Optional[dict[type, Any]] = None,
    providers: Optional[dict[type, Any]] = None,
):
    import logging

    prototype_factory[BeamlimeLogger].setLevel(logging.DEBUG)

    with multiple_constant_providers(prototype_factory, parameters):
        with multiple_temporary_providers(prototype_factory, providers):
            prototype = prototype_factory[Prototype]
            prototype.run()


if __name__ == "__main__":
    run_prototype(
        base_factory(),
        parameters={EventRate: 10**4, NumPixels: 10**5, NumFrames: 140},
        providers={Prototype: BasePrototype, DataStreamListener: DataStreamSimulator},
    )
