# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import pathlib
from contextlib import contextmanager
from typing import Any, List, NewType, Optional

from beamlime import Factory, ProviderGroup
from beamlime.applications.daemons import DataReductionDaemon
from beamlime.logging import BeamlimeLogger

Prototype = NewType("Prototype", DataReductionDaemon)


def prototype_app_providers() -> ProviderGroup:
    from beamlime.applications._workflow import Events, Histogrammed, provide_pipeline
    from beamlime.applications.base import BeamlimeMessage, MessageRouter
    from beamlime.applications.daemons import DataReductionMessageRouter
    from beamlime.applications.handlers import (
        DataReductionHandler,
        PlotHandler,
        StopWatch,
        random_image_path,
    )
    from beamlime.constructors.providers import SingletonProvider

    app_providers = ProviderGroup(
        DataReductionHandler,
        PlotHandler,
        StopWatch,
        provide_pipeline,
        random_image_path,
    )
    app_providers[MessageRouter] = SingletonProvider(DataReductionMessageRouter)
    for pipe_type in (Events, Histogrammed, BeamlimeMessage):
        app_providers[List[pipe_type]] = SingletonProvider(list)

    return app_providers


def prototype_base_providers() -> ProviderGroup:
    from beamlime.applications._parameters import collect_default_param_providers
    from beamlime.applications._random_data_providers import random_data_providers
    from beamlime.constructors.providers import merge
    from beamlime.logging.providers import log_providers

    return merge(
        collect_default_param_providers(),
        random_data_providers,
        prototype_app_providers(),
        log_providers,
    )


def mini_prototype_factory() -> Factory:
    from beamlime.applications.daemons import DataStreamListener, DataStreamSimulator

    providers = prototype_base_providers()
    providers[Prototype] = DataReductionDaemon
    providers[DataStreamListener] = DataStreamSimulator
    return Factory(providers)


@contextmanager
def temporary_factory(
    prototype_factory: Factory,
    parameters: Optional[dict[type, Any]] = None,
    providers: Optional[dict[type, Any]] = None,
):
    from beamlime.constructors import (
        multiple_constant_providers,
        multiple_temporary_providers,
    )

    tmp_factory = Factory(prototype_factory.providers)
    with multiple_constant_providers(tmp_factory, parameters):
        with multiple_temporary_providers(tmp_factory, providers):
            yield tmp_factory


def prototype_arg_parser() -> argparse.ArgumentParser:
    from beamlime.applications._parameters import PrototypeParameters
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
    parser.add_argument(
        "--image-path",
        default="",
        help="Path to save the plot image. Default is a random path.",
        type=str,
    )
    parser.add_argument(
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

    type_name_map = PrototypeParameters().type_name_map
    parameters = {
        field_type: getattr(arg_name_space, field_name)
        for field_type, field_name in type_name_map.items()
    }
    if arg_name_space.image_path:
        parameters[ImagePath] = ImagePath(pathlib.Path(arg_name_space.image_path))

    with temporary_factory(
        prototype_factory=prototype_factory,
        parameters=parameters,
    ) as factory:
        factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
        factory[Prototype].run()


if __name__ == "__main__":
    factory = mini_prototype_factory()
    arg_parser = prototype_arg_parser()

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())
