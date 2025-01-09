# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime.config.config_loader import load_config


def test_load_config():
    config = load_config(namespace='monitor_data', env='')
    assert 'topics' in config
