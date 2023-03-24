# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import importlib.resources
from importlib.abc import Traversable

import yaml


def find_source(filename: str, module: str = __package__) -> Traversable:
    return importlib.resources.files(module).joinpath(filename)


def read_source(filename: str, module: str = __package__) -> str:
    return find_source(filename, module=module).read_text()


def load_yaml(filename: str, module: str = __package__) -> dict:
    filepath = find_source(filename, module=module)
    with open(filepath) as file:
        return yaml.safe_load(file)
