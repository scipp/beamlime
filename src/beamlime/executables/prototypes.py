# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
from typing import Protocol, TypeVar

from beamlime import Factory, ProviderGroup, SingletonProvider
from beamlime.applications.daemons import FakeListener, read_nexus_template_file
from beamlime.applications.handlers import DataAssembler, PlotSaver
from beamlime.logging import BeamlimeLogger

T = TypeVar("T", bound="ArgumentInstantiable")


class ArgumentInstantiable(Protocol):
    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None: ...

    @classmethod
    def from_args(
        cls: type[T], logger: BeamlimeLogger, args: argparse.Namespace
    ) -> T: ...


def instantiate_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace, tp: type[T]
) -> T:
    return tp.from_args(logger=logger, args=args)


def fake_listener_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> FakeListener:
    return instantiate_from_args(logger, args, FakeListener)


def plot_saver_from_args(logger: BeamlimeLogger, args: argparse.Namespace) -> PlotSaver:
    return instantiate_from_args(logger, args, PlotSaver)


def data_assembler_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> DataAssembler:
    return instantiate_from_args(logger, args, DataAssembler)


def collect_default_providers() -> ProviderGroup:
    """Helper method to collect all default providers for this prototype."""
    from beamlime.constructors.providers import merge as merge_providers
    from beamlime.logging.providers import log_providers

    from ..applications.base import Application, MessageRouter
    from ..applications.handlers import DataReductionHandler
    from ..workflow_protocols import provide_stateless_workflow

    app_providers = ProviderGroup(
        SingletonProvider(Application),
        MessageRouter,
    )

    additional_providers = ProviderGroup(
        DataReductionHandler,
        provide_stateless_workflow,
        read_nexus_template_file,
    )

    return merge_providers(app_providers, log_providers, additional_providers)


def run_standalone_prototype(
    prototype_factory: Factory, arg_name_space: argparse.Namespace
):
    from ..applications.base import Application
    from ..applications.daemons import DataPieceReceived, RunStart
    from ..applications.handlers import (
        DataAssembler,
        DataReady,
        DataReductionHandler,
        WorkflowResultUpdate,
    )
    from ..constructors import multiple_constant_providers
    from ..stateless_workflow import Workflow

    parameters = {
        Workflow: Workflow(arg_name_space.workflow),
        argparse.Namespace: arg_name_space,
    }

    factory = Factory(
        prototype_factory.providers,
        ProviderGroup(
            fake_listener_from_args,
            plot_saver_from_args,
            data_assembler_from_args,
        ),
    )

    with multiple_constant_providers(factory, parameters):
        factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
        app = factory[Application]

        # Handlers
        plot_saver = factory[PlotSaver]
        app.register_handling_method(WorkflowResultUpdate, plot_saver.save_histogram)
        data_assembler = factory[DataAssembler]
        app.register_handling_method(RunStart, data_assembler.set_run_start)
        app.register_handling_method(DataPieceReceived, data_assembler.merge_data_piece)
        data_reduction_handler = factory[DataReductionHandler]
        app.register_handling_method(DataReady, data_reduction_handler.reduce_data)

        # Daemons
        app.register_daemon(factory[FakeListener])
        app.run()
