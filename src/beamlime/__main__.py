# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


def main() -> None:
    from beamlime.executables.options import build_arg_parser, merge_options

    parser = build_arg_parser()
    parser.parse_args()
    merge_options(command_args=parser.parse_args())


if __name__ == "__main__":
    main()
