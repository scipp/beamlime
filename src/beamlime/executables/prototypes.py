# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
from typing import Optional

from beamlime import Factory, ProviderGroup
from beamlime.applications.daemons import FakeListener
from beamlime.applications.handlers import PlotSaver
from beamlime.logging import BeamlimeLogger


def collect_default_providers() -> ProviderGroup:
    """Helper method to collect all default providers for this prototype."""
    from beamlime import SingletonProvider
    from beamlime.constructors.providers import merge as merge_providers
    from beamlime.logging.providers import log_providers

    from ..applications.base import Application, MessageRouter
    from ..applications.handlers import DataReductionHandler, random_image_path
    from ..stateless_workflow import provide_stateless_workflow

    app_providers = ProviderGroup(
        SingletonProvider(Application),
        DataReductionHandler,
        MessageRouter,
        provide_stateless_workflow,
        random_image_path,
    )

    return merge_providers(
        app_providers,
        log_providers,
    )


def data_stream_arg_parser(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    from beamlime.applications.daemons import FakeListener

    parser = parser or argparse.ArgumentParser()
    FakeListener.argument_group(parser)

    return parser


def visualization_arg_parser(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    parser = parser or argparse.ArgumentParser()

    group = parser.add_argument_group('Plotting Configuration')
    group.add_argument(
        "--image-path",
        default="",
        help="Path to save the plot image. Default is a random path.",
        type=str,
    )

    return parser


def fake_kafka_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> FakeListener:
    listener = FakeListener(
        speed=args.data_feeding_speed,
        nexus_template_path=args.nexus_template_path,
        num_frames=args.num_frames,
    )
    listener.logger = logger
    return listener


def plot_saver_from_args(logger: BeamlimeLogger, args: argparse.Namespace) -> PlotSaver:
    from beamlime.applications.handlers import ImagePath

    return PlotSaver(logger=logger, image_path=ImagePath(pathlib.Path(args.image_path)))


def run_standalone_prototype(factory: Factory, arg_name_space: argparse.Namespace):
    from ..applications.base import Application
    from ..applications.daemons import DetectorDataReceived, FakeListener, RunStart
    from ..applications.handlers import (
        DataReductionHandler,
        PlotSaver,
        WorkflowResultUpdate,
    )
    from ..stateless_workflow import Workflow

    factory.providers[Workflow] = lambda: Workflow(arg_name_space.workflow)
    factory.providers[argparse.Namespace] = lambda: arg_name_space
    factory.providers[FakeListener] = fake_kafka_from_args
    factory.providers[PlotSaver] = plot_saver_from_args
    factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())

    app = factory[Application]

    # Handlers
    plot_saver = factory[PlotSaver]
    app.register_handling_method(WorkflowResultUpdate, plot_saver.save_histogram)
    data_reduction_handler = factory[DataReductionHandler]
    app.register_handling_method(RunStart, data_reduction_handler.update_nexus_template)
    app.register_handling_method(
        DetectorDataReceived, data_reduction_handler.process_message
    )

    # Daemons
    app.register_daemon(factory[FakeListener])
    app.run()
