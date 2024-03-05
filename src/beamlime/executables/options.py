# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Command line or explicit user-input options should be always prioritized
# over options loaded from default files(i.e. pyproject.toml).
import argparse


def build_arg_parser() -> argparse.ArgumentParser:
    """Builds the argument parser for the highest-level entry point."""
    parser = argparse.ArgumentParser(description="BEAMLIME configuration.")
    parser.add_argument(
        "--workflow",
        default="dummy",
        help="Name of the workflow to run",
        type=str,
    )
    return parser
