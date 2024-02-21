# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Command line or explicit user-input configuration should be always prioritized
# over configuration from default files(i.e. pyproject.toml).
import argparse
import sys


def load_pyproject_toml_config() -> None:
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        import tomli as tomllib

    with open("pyproject.toml", "rb") as f:
        return tomllib.load(f)['tool']['beamlime']


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the BEAMLIME application.")
    return parser
