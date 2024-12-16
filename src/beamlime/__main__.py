# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


try:
    from appstract.constructors import Factory
    from appstract.script import build_arg_parser
except ImportError as e:
    raise ImportError(
        "The appstract package is required for the old version of beamlime."
        "Please install it using `pip install appstract`."
    ) from e

from importlib.metadata import entry_points


def list_entry_points() -> list[str]:
    return [ep.name for ep in entry_points(group='beamlime.workflow_plugin')]


def main() -> None:
    from beamlime.applications.daemons import FakeListener
    from beamlime.applications.handlers import DataAssembler, PlotSaver
    from beamlime.executables.prototypes import (
        collect_default_providers,
        run_standalone_prototype,
    )

    factory = Factory(collect_default_providers())
    arg_parser = build_arg_parser(FakeListener, PlotSaver, DataAssembler)
    arg_parser.add_argument(
        "--workflow",
        help="Name of the workflow to run",
        type=str,
        choices=list_entry_points(),
        required=True,
    )

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())


if __name__ == "__main__":
    main()
