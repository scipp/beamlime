# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import scipp as sc
import plopp as pp
import argparse
import asyncio
import pathlib
from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, List, NewType, Optional
from .base import BaseDaemon, BaseHandler, BeamlimeMessage, MessageRouter

from beamlime.constructors import Factory, ProviderGroup
from beamlime.logging import BeamlimeLogger


from ._parameters import ChunkSize, PrototypeParameters
from ._random_data_providers import RandomEvents
from ._workflow import (
    Events,
    FirstPulseTime,
    Histogrammed,
    WorkflowPipeline,
    provide_pipeline,
)


DataStreamListener = NewType("DataStreamListener", BaseDaemon)

@dataclass
class RawDataSent(BeamlimeMessage):
    content: List[Events]
    last: bool


class DataStreamSimulator(BaseDaemon):
    raw_data_pipe: List[Events]
    random_events: RandomEvents
    chunk_size: ChunkSize
    messanger: MessageRouter

    def slice_chunk(self) -> Events:
        chunk, self.random_events = (
            Events(self.random_events[: self.chunk_size]),
            RandomEvents(self.random_events[self.chunk_size :]),
        )
        return chunk

    async def commit_process(self) -> None:
        return await self.messanger.send_message_async(
            RawDataSent(
                sender=DataStreamSimulator,
                receiver=Any,
                content=self.raw_data_pipe,
                last=False,
            )
        )

    async def run(self) -> None:
        import time
        self.info("Data streaming started...")

        num_chunks = len(self.random_events) // self.chunk_size

        for i_chunk in range(num_chunks):
            chunk = self.slice_chunk()
            self.raw_data_pipe.append(chunk)
            self.debug("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            await self.messanger.send_message_async(
                RawDataSent(
                    sender=DataStreamSimulator,
                    receiver=Any,
                    content=self.raw_data_pipe,
                    last=i_chunk == num_chunks - 1,
                )
            )

        self.info("Data streaming finished...")

@dataclass
class HistogramUpdated(BeamlimeMessage):
    content: pp.graphics.basefig.BaseFig
    last: bool


class DataReductionHandler(BaseHandler):
    input_pipe: List[Events]
    pipeline: WorkflowPipeline

    def __init__(self, messanger: MessageRouter) -> None:
        self.output_da: Histogrammed
        self.stream_node: pp.Node
        super().__init__(messanger)

    def register_handlers(self) -> None:
        self.messanger.register_awaitable_handler(RawDataSent, self.process_message)

    def format_received(self, data: Any) -> str:
        return f"{len(data)} pieces of {Events.__name__}"
    
    def process_first_input(self, da: Events) -> None:
        first_pulse_time = da[0].coords['event_time_zero'][0]
        self.pipeline[FirstPulseTime] = first_pulse_time

    def process_first_output(self, data: Histogrammed) -> None:
        self.output_da = sc.zeros_like(data)
        self.debug("First data as a seed of histogram: %s", self.output_da)
        self.stream_node = pp.Node(self.output_da)
        self.figure = pp.figure1d(self.stream_node)

    def process_data(self, data: Events) -> Histogrammed:
        self.debug("Received, %s", self.format_received(data))
        self.pipeline[Events] = data
        return self.pipeline.compute(Histogrammed)

    async def process_message(self, message: RawDataSent) -> None:
        data = message.content.pop(0)
        if not hasattr(self, "output_da"):
            self.process_first_input(data)
            output = self.process_data(data)
            self.process_first_output(output)
        else:
            output = self.process_data(data)

        self.output_da.values += output.values
        self.stream_node.notify_children("update")
        await self.messanger.send_message_async(
            HistogramUpdated(
                sender=DataReductionHandler,
                receiver=Any,
                content=self.figure,
                last=message.last,
            )
        )
    
    def __del__(self) -> None:
        from matplotlib import pyplot as plt
        plt.close()


ImagePath = NewType("ImagePath", pathlib.Path)
def random_image_path() -> ImagePath:
    import uuid

    return ImagePath(pathlib.Path(f"beamlime_plot_{uuid.uuid4().hex}.png"))

class PlottingDone(BeamlimeMessage):
    content: ImagePath


class PlotHandler(BaseHandler):
    def __init__(self, logger: BeamlimeLogger, messanger: MessageRouter, image_path: ImagePath) -> None:
        self.logger = logger
        self.image_path = image_path
        super().__init__(messanger)

    def register_handlers(self) -> None:
        self.create_dummy_image()
        self.messanger.register_handler(HistogramUpdated, self.save_histogram)
        self.info(f"PlotHandler will save updated image into: {self.image_path.absolute()}")

    def create_dummy_image(self) -> None:
        import matplotlib.pyplot as plt

        plt.plot([])
        plt.savefig(self.image_path)

    def save_histogram(self, message: HistogramUpdated) -> None:
        self.debug(f"Received histogram saved.")
        message.content.save(self.image_path)
        if message.last:
            self.messanger.send_message(
                PlottingDone(sender=PlotHandler, receiver=Any, content=self.image_path)
            )
            self.info(f"Last plot saved into {self.image_path}.")


class BasePrototype(BaseDaemon, ABC):
    data_stream_listener: DataStreamListener
    message_router: MessageRouter
    data_reduction: DataReductionHandler
    plot_saver: PlotHandler

    def collect_sub_daemons(self) -> list[BaseDaemon]:
        return [
            self.data_stream_listener,
            self.message_router,
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
        from beamlime.core.schedulers import temporary_event_loop

        self.message_router.register_handler(PlottingDone, self.kill)
        self.debug('Start running ...')
        with temporary_event_loop() as loop:
            self.loop = loop
            daemon_coroutines = [daemon.run() for daemon in self.collect_sub_daemons()]
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))

    def kill(self, message: PlottingDone):
        self.debug(f"Last plot saved into {message.content}. Killing all tasks.")
        self.debug("Killed all tasks.")


Prototype = NewType("Prototype", BasePrototype)


def prototype_app_providers() -> ProviderGroup:
    from beamlime.constructors.providers import SingletonProvider

    app_providers = ProviderGroup(
        SingletonProvider(MessageRouter),
        DataReductionHandler,
        PlotHandler,
        provide_pipeline,
        random_image_path,
    )

    for pipe_type in (Events, Histogrammed, BeamlimeMessage):
        app_providers[List[pipe_type]] = SingletonProvider(list)

    return app_providers


def prototype_base_providers() -> ProviderGroup:
    from beamlime.constructors.providers import merge
    from beamlime.logging.providers import log_providers

    from ._parameters import collect_default_param_providers
    from ._random_data_providers import random_data_providers

    return merge(
        collect_default_param_providers(),
        random_data_providers,
        prototype_app_providers(),
        log_providers,
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
