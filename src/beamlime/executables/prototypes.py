# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
from typing import Protocol, TypeVar

from ..applications.daemons import FakeListener, read_nexus_template_file
from ..applications.handlers import DataAssembler, PlotSaver

try:
    from appstract import logging as applogs
    from appstract.constructors import (
        Factory,
        ProviderGroup,
        SingletonProvider,
        multiple_constant_providers,
    )
    from appstract.constructors.providers import merge as merge_providers
    from appstract.event_driven import AsyncApplication, MessageRouter
    from appstract.logging.providers import log_providers
except ImportError as e:
    raise ImportError(
        "The appstract package is required for the old version of beamlime."
        "Please install it using `pip install appstract`."
    ) from e
T = TypeVar("T", bound="ArgumentInstantiable")


class ArgumentInstantiable(Protocol):
    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None: ...

    @classmethod
    def from_args(
        cls: type[T], logger: applogs.AppLogger, args: argparse.Namespace
    ) -> T: ...


def instantiate_from_args(
    logger: applogs.AppLogger, args: argparse.Namespace, tp: type[T]
) -> T:
    return tp.from_args(logger=logger, args=args)


def fake_listener_from_args(
    logger: applogs.AppLogger, args: argparse.Namespace
) -> FakeListener:
    return instantiate_from_args(logger, args, FakeListener)


def plot_saver_from_args(
    logger: applogs.AppLogger, args: argparse.Namespace
) -> PlotSaver:
    return instantiate_from_args(logger, args, PlotSaver)


def data_assembler_from_args(
    logger: applogs.AppLogger, args: argparse.Namespace
) -> DataAssembler:
    return instantiate_from_args(logger, args, DataAssembler)


def collect_default_providers() -> ProviderGroup:
    """Helper method to collect all default providers for this prototype."""
    from ..applications.handlers import DataReductionHandler
    from ..workflow_protocols import provide_beamlime_workflow

    app_providers = ProviderGroup(
        SingletonProvider(AsyncApplication),
        MessageRouter,
    )

    additional_providers = ProviderGroup(
        DataReductionHandler,
        provide_beamlime_workflow,
        read_nexus_template_file,
    )

    return merge_providers(app_providers, log_providers, additional_providers)


def run_standalone_prototype(
    prototype_factory: Factory, arg_name_space: argparse.Namespace
):
    from ..applications.daemons import DataPieceReceived, RunStart
    from ..applications.handlers import (
        DataAssembler,
        DataReady,
        DataReductionHandler,
        WorkflowResultUpdate,
    )
    from ..workflow_protocols import WorkflowName

    parameters = {
        WorkflowName: WorkflowName(arg_name_space.workflow),
        argparse.Namespace: arg_name_space,
    }

    factory = Factory(
        prototype_factory.providers,
        ProviderGroup(
            SingletonProvider(fake_listener_from_args),
            plot_saver_from_args,
            data_assembler_from_args,
        ),
    )

    with multiple_constant_providers(factory, parameters):
        factory[applogs.AppLogger].setLevel(arg_name_space.log_level.upper())
        app = factory[AsyncApplication]

        # Handlers
        plot_saver = factory[PlotSaver]
        app.register_handler(WorkflowResultUpdate, plot_saver.save_histogram)
        data_assembler = factory[DataAssembler]
        app.register_handler(RunStart, data_assembler.set_run_start)
        app.register_handler(DataPieceReceived, data_assembler.merge_data_piece)
        data_reduction_handler = factory[DataReductionHandler]
        app.register_handler(RunStart, data_reduction_handler.set_run_start)
        app.register_handler(DataReady, data_reduction_handler.reduce_data)

        # Daemons
        app.register_daemon(factory[FakeListener].run)
        app.run()
