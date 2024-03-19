# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
from typing import Optional

from beamlime import Factory, ProviderGroup, SingletonProvider
from beamlime.logging import BeamlimeLogger


def collect_default_providers() -> ProviderGroup:
    """Helper method to collect all default providers for this prototype."""
    from beamlime.constructors.providers import merge as merge_providers
    from beamlime.logging.providers import log_providers

    from ..applications._parameters import collect_default_param_providers
    from ..applications.base import Application, MessageRouter
    from ..applications.daemons import FakeListener
    from ..applications.handlers import (
        DataReductionHandler,
        PlotSaver,
        random_image_path,
    )
    from ..stateless_workflow import provide_stateless_workflow

    app_providers = ProviderGroup(
        SingletonProvider(Application),
        FakeListener,
        DataReductionHandler,
        PlotSaver,
        provide_stateless_workflow,
        random_image_path,
    )
    app_providers[MessageRouter] = SingletonProvider(MessageRouter)

    return merge_providers(
        collect_default_param_providers(),
        app_providers,
        log_providers,
    )


def default_prototype_factory() -> Factory:
    return Factory(collect_default_providers())


def event_generator_arg_parser(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    from beamlime.applications._parameters import HistogramBinSize, PrototypeParameters
    from beamlime.constructors.inspectors import extract_underlying_type

    parser = parser or argparse.ArgumentParser()
    default_params = PrototypeParameters()

    def wrap_name(name: str) -> str:
        return '--' + name.replace('_', '-')

    type_name_map = default_params.type_name_map

    group = parser.add_argument_group('Event Generator Configuration')
    event_generator_configs = {
        param_type: default_value
        for param_type, default_value in default_params.as_type_dict().items()
        if param_type != HistogramBinSize
    }
    for param_type, default_value in event_generator_configs.items():
        group.add_argument(
            wrap_name(type_name_map[param_type]),
            default=default_value,
            help=f": {param_type}",
            type=extract_underlying_type(param_type),
        )
    group.add_argument(
        "--nexus-template-path",
        default="",
        help="Path to the nexus template file.",
        type=str,
    )

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


def run_standalone_prototype(
    prototype_factory: Factory, arg_name_space: argparse.Namespace
):
    from ..applications._parameters import PrototypeParameters
    from ..applications.base import Application
    from ..applications.daemons import (
        DetectorDataReceived,
        FakeListener,
        NexusTemplatePath,
    )
    from ..applications.handlers import (
        DataReductionHandler,
        ImagePath,
        PlotSaver,
        WorkflowResultUpdate,
    )
    from ..constructors import multiple_constant_providers
    from ..stateless_workflow import Workflow

    type_name_map = PrototypeParameters().type_name_map
    parameters = {
        field_type: getattr(arg_name_space, field_name)
        for field_type, field_name in type_name_map.items()
    }
    if arg_name_space.image_path:
        parameters[ImagePath] = ImagePath(pathlib.Path(arg_name_space.image_path))
    if arg_name_space.nexus_template_path:
        parameters[NexusTemplatePath] = NexusTemplatePath(
            arg_name_space.nexus_template_path
        )

    parameters[Workflow] = Workflow(arg_name_space.workflow)

    factory = Factory(prototype_factory.providers)

    with multiple_constant_providers(factory, parameters):
        factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
        app = factory[Application]

        # Handlers
        plot_saver = factory[PlotSaver]
        app.register_handling_method(WorkflowResultUpdate, plot_saver.save_histogram)
        data_reduction_handler = factory[DataReductionHandler]
        app.register_handling_method(
            DetectorDataReceived, data_reduction_handler.process_message
        )

        # Daemons
        app.register_daemon(factory[FakeListener])
        app.run()
