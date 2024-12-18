# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime.config.config_loader import load_config


def test_load_consumer_config_loads_dev_env_by_default():
    config = load_config(namespace='monitor_data', kind='consumer')
    assert config['kafka']['bootstrap.servers'] == 'localhost:9092'
