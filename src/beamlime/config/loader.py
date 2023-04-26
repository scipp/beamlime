# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Union, overload


@overload
def safe_load_config(_: None) -> dict:
    ...


@overload
def safe_load_config(config: dict) -> dict:
    ...


@overload
def safe_load_config(config_path: str) -> dict:
    ...


def safe_load_config(config: Union[str, dict, None]) -> dict:
    if isinstance(config, dict):
        return config
    elif config is None:
        from beamlime.resources.generated import load_static_default_config

        return load_static_default_config()
    elif isinstance(config, str):
        import yaml

        from .inspector import validate_config_path

        validate_config_path(config_path=config)
        with open(config) as file:
            return yaml.safe_load(file)
