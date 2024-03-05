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
    from ..applications._random_data_providers import random_data_providers
    from ..applications.base import Application
    from ..applications.daemons import DataStreamSimulator, MessageRouter
    from ..applications.handlers import (
        DataReductionHandler,
        PlotSaver,
        random_image_path,
    )
    from ..stateless_workflow import provide_stateless_workflow

    app_providers = ProviderGroup(
        SingletonProvider(Application),
        DataStreamSimulator,
        DataReductionHandler,
        PlotSaver,
        provide_stateless_workflow,
        random_image_path,
    )
    app_providers[MessageRouter] = SingletonProvider(MessageRouter)

    return merge_providers(
        collect_default_param_providers(),
        random_data_providers,
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

    return parser


def visualization_arg_parser(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    from beamlime.applications._parameters import HistogramBinSize, PrototypeParameters
    from beamlime.constructors.inspectors import extract_underlying_type

    parser = parser or argparse.ArgumentParser()
    default_params = PrototypeParameters()

    group = parser.add_argument_group('Plotting Configuration')
    group.add_argument(
        '--histogram-bin-size',
        default=default_params.histogram_bin_size,
        help=f": {HistogramBinSize}",
        type=extract_underlying_type(HistogramBinSize),
    )
    group.add_argument(
        "--image-path",
        default="",
        help="Path to save the plot image. Default is a random path.",
        type=str,
    )
    group.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level. Default is INFO.",
        type=str,
    )

    return parser


def run_standalone_prototype(
    prototype_factory: Factory, arg_name_space: argparse.Namespace
):
    from ..applications._parameters import PrototypeParameters
    from ..applications.base import Application
    from ..applications.daemons import DataStreamSimulator
    from ..applications.handlers import (
        DataReductionHandler,
        ImagePath,
        PlotSaver,
        RawDataSent,
        WorkflowResultUpdate,
    )
    from ..constructors import multiple_constant_providers

    type_name_map = PrototypeParameters().type_name_map
    parameters = {
        field_type: getattr(arg_name_space, field_name)
        for field_type, field_name in type_name_map.items()
    }
    if arg_name_space.image_path:
        parameters[ImagePath] = ImagePath(pathlib.Path(arg_name_space.image_path))

    factory = Factory(prototype_factory.providers)

    with multiple_constant_providers(factory, parameters):
        factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
        app = factory[Application]

        # Handlers
        plot_saver = factory[PlotSaver]
        app.register_handling_method(WorkflowResultUpdate, plot_saver.save_histogram)
        data_reduction_handler = factory[DataReductionHandler]
        app.register_handling_method(
            RawDataSent, data_reduction_handler.process_message
        )

        # Daemons
        app.register_daemon(factory[DataStreamSimulator])
        app.run()


if __name__ == "__main__":
    factory = default_prototype_factory()
    arg_parser = event_generator_arg_parser()
    visualization_arg_parser(arg_parser)

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())
