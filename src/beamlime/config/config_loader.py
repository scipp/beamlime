# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os
from importlib import resources

import yaml


def load_config(*, namespace: str, kind: str, env: str | None = None) -> dict:
    """Load configuration based on environment.

    Args:
        env: Environment name ('dev', 'staging', 'prod').
             Defaults to value of BEAMLIME_ENV environment variable.
    """
    env = env or os.getenv('BEAMLIME_ENV', 'dev')

    config_file = f'{namespace}_{kind}_{env}.yaml'
    # Use importlib.resources to access packaged config files
    with resources.files('beamlime.config.defaults').joinpath(config_file).open() as f:
        return yaml.safe_load(f)
