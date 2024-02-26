# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


def main() -> None:
    from beamlime.executables.options import build_arg_parser
    from beamlime.executables.prototypes import (
        event_generator_arg_parser,
        mini_prototype_factory,
        run_standalone_prototype,
        visualization_arg_parser,
    )

    factory = mini_prototype_factory()

    arg_parser = build_arg_parser()
    event_generator_arg_parser(arg_parser)
    visualization_arg_parser(arg_parser)

    run_standalone_prototype(factory, arg_name_space=arg_parser.parse_args())


if __name__ == "__main__":
    main()
