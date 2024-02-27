# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
from typing import Optional

from beamlime import Factory
from beamlime.applications.daemons import DataReductionApp
from beamlime.logging import BeamlimeLogger


def mini_prototype_factory() -> Factory:
    return Factory(DataReductionApp.collect_default_providers())


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
    from beamlime.applications._parameters import PrototypeParameters
    from beamlime.applications.handlers import ImagePath
    from beamlime.constructors import multiple_constant_providers

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
        factory[DataReductionApp].run()


if __name__ == "__main__":
    factory = mini_prototype_factory()
    arg_parser = event_generator_arg_parser()
    visualization_arg_parser(arg_parser)

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())
