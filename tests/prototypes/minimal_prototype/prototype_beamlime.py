# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from itertools import product
from typing import Any, Generic, List, NewType, TypeVar

import pytest

from beamlime.core.schedulers import async_retry
from beamlime.logging import BeamlimeLogger
from beamlime.logging.mixins import LogMixin

from ..benchmark_helper import BenchmarkConfig
from .providers import (
    BinnedData,
    ChunkSize,
    Events,
    Histogrammed,
    MergedData,
    NumPixels,
    ReducedData,
    Workflow,
)


class BaseApp(LogMixin, ABC):
    logger: BeamlimeLogger
    benchmark_config: BenchmarkConfig

    def target_count_reached(self, data_count: int) -> bool:
        return (
            self.benchmark_config.target_count is not None
            and self.benchmark_config.target_count <= data_count
        )

    def data_pipe_monitor(
        self,
        pipe: List[Any],
        timeout: float = 5,
        interval: float = 1 / 14,
        prefered_size: int = 1,
        target_size: int = 1,
    ):
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
    random_events: Events
    chunk_size: ChunkSize

    async def run(self) -> None:
        import math

        num_chunks = math.ceil(len(self.random_events) / self.chunk_size)
        self.benchmark_config.target_count = num_chunks

        for i_event in range(num_chunks):
            data_list, self.random_events = (
                self.random_events[: self.chunk_size],
                self.random_events[self.chunk_size :],
            )
            self.raw_data_pipe.append(Events(data_list))
            self.debug("Chunk size %s", self.chunk_size)
            self.debug("Sending %s th, %s pieces of data.", i_event + 1, len(data_list))
            await asyncio.sleep(0)

        self.info("Data streaming finished...")


InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")


class DataReductionMixin(BaseApp, ABC, Generic[InputType, OutputType]):
    workflow: Workflow
    input_pipe: List[InputType]
    output_pipe: List[OutputType]
    benchmark_config: BenchmarkConfig

    def __init__(self) -> None:
        self.input_type = self._retrieve_type_arg('input_pipe')
        self.output_type = self._retrieve_type_arg('output_pipe')
        super().__init__()

    @classmethod
    def _retrieve_type_arg(cls, attr_name: str) -> type:
        from typing import get_args, get_type_hints

        return get_args(get_type_hints(cls)[attr_name])[0]

    def format_received(self, data: InputType) -> str:
        return str(data)

    async def run(self) -> None:
        data_monitor = self.data_pipe_monitor(self.input_pipe, target_size=1)
        data_count = 0
        while (
            not self.target_count_reached(data_count)
            and await data_monitor()
            and (data_count := data_count + 1)
        ):
            data = self.input_pipe.pop(0)
            self.debug("Received, %s", self.format_received(data))
            with self.workflow.constant_provider(self.input_type, data):
                self.output_pipe.append(self.workflow[self.output_type])
            await asyncio.sleep(0)

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
    benchmark_config: BenchmarkConfig

    def show(self):
        if not hasattr(self, "fig"):
            raise AttributeError("Please wait until the first figure is created.")
        return self.fig

    async def run(self) -> None:
        import plopp as pp

        data_monitor = self.data_pipe_monitor(self.visualized_data_pipe)
        data_count = 0
        if hasattr(self, "fig"):
            del self.fig

        if await data_monitor() and (data_count := data_count + 1):
            self.first_data = self.visualized_data_pipe.pop(0)
            self.debug("First data as a seed of histogram: %s", self.first_data)
            self.stream_node = pp.Node(lambda: self.first_data)
            self.fig = pp.figure1d(self.stream_node)
            await asyncio.sleep(0)

        while (
            not self.target_count_reached(data_count)
            and await data_monitor()
            and (data_count := data_count + 1)
        ):
            new_data = self.visualized_data_pipe.pop(0)
            self.first_data.values += new_data.values
            self.stream_node.notify_children("update")
            self.debug("Updated plot.")
            await asyncio.sleep(0)

        self.benchmark_config.result_count = data_count
        self.info("No more data coming in. Finishing ...")


class Prototype(BaseApp):
    data_stream_listener: DataStreamListener
    data_merge: DataMerge[Events, MergedData]
    data_binning: DataBinning[MergedData, BinnedData]
    data_reduction: DataReduction[BinnedData, ReducedData]
    data_plotter: DataHistogramming[ReducedData, Histogrammed]
    visualizer: VisualizationDaemon

    def run(self):
        self.debug('Start running ...')
        daemons: list[BaseApp] = [
            self.data_stream_listener,
            self.data_merge,
            self.data_binning,
            self.data_reduction,
            self.data_plotter,
        ]
        daemon_coroutines = [daemon.run() for daemon in daemons]

        try:
            for daemon_job in daemon_coroutines:
                asyncio.create_task(daemon_job)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            for daemon_job in daemon_coroutines:
                loop.create_task(daemon_job)
            loop.run_until_complete(self.visualizer.run())


def create_prototype(event_rate, num_pixels, chunk_size, frame_rate, num_frames):
    from beamlime.constructors import Factory, ProviderGroup
    from beamlime.ready_factory import log_providers

    from .providers import (
        EventRate,
        FrameRate,
        HistogramBinSize,
        NumFrames,
        fake_data_providers,
        workflow_providers,
    )

    prototype_providers = ProviderGroup(Prototype)
    prototype_providers[DataMerge[Events, MergedData]] = DataMerge
    prototype_providers[DataBinning[MergedData, BinnedData]] = DataBinning
    prototype_providers[DataReduction[BinnedData, ReducedData]] = DataReduction
    prototype_providers[
        DataHistogramming[ReducedData, Histogrammed]
    ] = DataHistogramming
    prototype_providers[DataStreamListener] = DataStreamSimulator

    for pipe_type in (Events, BinnedData, MergedData, ReducedData, Histogrammed):
        prototype_providers.cached_provider(List[pipe_type], list)

    prototype_providers.cached_provider(BenchmarkConfig, BenchmarkConfig)
    prototype_providers.cached_provider(VisualizationDaemon, VisualizationDaemon)

    config_providers = ProviderGroup()
    config_providers[FrameRate] = lambda: frame_rate
    config_providers[NumFrames] = lambda: num_frames
    config_providers[ChunkSize] = lambda: chunk_size
    config_providers[EventRate] = lambda: event_rate
    config_providers[NumPixels] = lambda: num_pixels
    config_providers[HistogramBinSize] = lambda: 50

    return Factory(
        fake_data_providers,
        prototype_providers,
        config_providers,
        log_providers,
        workflow_providers,
    )


event_rate_cands = [10000]
num_pixels_cands = [10000, 10000000]


@pytest.mark.parametrize(
    ["event_rate", "num_pixels", "chunk_size", "frame_rate", "num_frames"],
    [(*param, 28, 14, 140) for param in product(event_rate_cands, num_pixels_cands)],
)
def test_prototype(
    benchmark, event_rate, num_pixels, chunk_size, frame_rate, num_frames
):
    import logging

    factory = create_prototype(
        event_rate, num_pixels, chunk_size, frame_rate, num_frames
    )
    factory[BeamlimeLogger].setLevel(logging.DEBUG)
    prototype = factory[Prototype]
    if benchmark:
        benchmark.pedantic(prototype.run, iterations=1, rounds=1)
    else:
        prototype.run()


if __name__ == "__main__":
    test_prototype(
        None,
        event_rate=10**6,
        num_pixels=10**6,
        chunk_size=28,
        frame_rate=14,
        num_frames=140,
    )
