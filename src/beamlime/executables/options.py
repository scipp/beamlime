# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Command line or explicit user-input options should be always prioritized
# over options loaded from default files(i.e. pyproject.toml).
import argparse
from typing import Optional


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BEAMLIME configuration.")
    return parser


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
    group.add_argument(
        "--workflow-name",
        default="dummy",
        help="Name of the workflow that produces the visualization",
        type=str,
    )

    return parser
