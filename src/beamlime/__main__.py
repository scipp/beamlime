# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


def main() -> None:
    from beamlime.executables.configurations import (
        build_arg_parser,
        load_pyproject_toml_config,
    )

    load_pyproject_toml_config()
    parser = build_arg_parser()
    parser.parse_args()


if __name__ == "__main__":
    main()
