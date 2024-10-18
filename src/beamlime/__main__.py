# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


def main() -> None:
    from beamlime import Factory
    from beamlime.applications.daemons import FakeListener
    from beamlime.applications.handlers import DataAssembler, PlotSaver, RawCountHandler
    from beamlime.executables.options import build_arg_parser
    from beamlime.executables.prototypes import (
        collect_default_providers,
        run_standalone_prototype,
    )

    factory = Factory(collect_default_providers())
    arg_parser = build_arg_parser(
        FakeListener, PlotSaver, DataAssembler, RawCountHandler
    )

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())


if __name__ == "__main__":
    main()
