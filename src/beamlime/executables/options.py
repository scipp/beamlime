# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Command line or explicit user-input options should be always prioritized
# over options loaded from default files(i.e. pyproject.toml).
import argparse
from importlib.metadata import entry_points


def list_entry_points() -> list[str]:
    return [ep.name for ep in entry_points(group='beamlime.workflow_plugin')]


def build_minimum_arg_parser(*sub_group_classes: type) -> argparse.ArgumentParser:
    """Builds the minimum argument parser for the highest-level entry point."""
    parser = argparse.ArgumentParser(description="BEAMLIME configuration.")
    parser.add_argument(
        "--log-level",
        help="Set logging level. Default is INFO.",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    for sub_group_class in sub_group_classes:
        if callable(add_arg := getattr(sub_group_class, "add_argument_group", None)):
            add_arg(parser)

    return parser


def build_arg_parser(*sub_group_classes: type) -> argparse.ArgumentParser:
    """Builds the default argument parser for the highest-level entry point."""
    parser = build_minimum_arg_parser(*sub_group_classes)
    parser.add_argument(
        "--workflow",
        help="Name of the workflow to run",
        type=str,
        choices=list_entry_points(),
        required=True,
    )
    return parser
