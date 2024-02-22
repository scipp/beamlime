# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Command line or explicit user-input options should be always prioritized
# over options loaded from default files(i.e. pyproject.toml).
import argparse
from dataclasses import dataclass


@dataclass
class BeamlimeCommandOptions:
    workflow: list[str]
    gui: list[str]
    extra: dict[str, str]
    input_source: str = ''


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the BEAMLIME application.")
    parser.add_argument('-w', '--workflow', action='append', help='Workflow to be used')
    parser.add_argument('--input-source', help='Input source to be used')
    parser.add_argument(
        '-g', '--gui', action='append', help='Visualization channel to be used'
    )
    return parser


def merge_options(*, command_args: argparse.Namespace) -> BeamlimeCommandOptions:
    return BeamlimeCommandOptions(
        workflow=command_args.workflow or [],
        gui=command_args.gui or [],
        input_source=command_args.input_source or '',
        extra=vars(command_args),
    )
