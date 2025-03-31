# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import json

import pytest

from beamlime.config.models import ConfigKey
from beamlime.handlers.config_handler import ConfigUpdate
from beamlime.kafka.message_adapter import RawConfigItem


class TestConfigUpdate:
    def test_init(self):
        config_key = ConfigKey(
            source_name="source1", service_name="service1", key="test_key"
        )
        value = {"param": 42}
        update = ConfigUpdate(config_key=config_key, value=value)

        assert update.config_key is config_key
        assert update.value is value

        # Test properties
        assert update.source_name == "source1"
        assert update.service_name == "service1"
        assert update.key == "test_key"

    def test_from_raw(self):
        item = RawConfigItem(
            key=b'source1/service1/test_key',
            value=json.dumps({'param': 42}).encode('utf-8'),
        )

        update = ConfigUpdate.from_raw(item)

        assert update.source_name == "source1"
        assert update.service_name == "service1"
        assert update.key == "test_key"
        assert update.value == {'param': 42}

    def test_from_raw_with_wildcards(self):
        item = RawConfigItem(
            key=b'*/*/test_key',
            value=json.dumps({'param': 42}).encode('utf-8'),
        )

        update = ConfigUpdate.from_raw(item)

        assert update.source_name is None
        assert update.service_name is None
        assert update.key == "test_key"
        assert update.value == {'param': 42}

    def test_from_raw_invalid_key(self):
        item = RawConfigItem(
            key=b'invalid_key_format',
            value=json.dumps({'param': 42}).encode('utf-8'),
        )

        with pytest.raises(ValueError, match="Invalid key format"):
            ConfigUpdate.from_raw(item)

    def test_from_raw_invalid_json(self):
        item = RawConfigItem(key=b'source1/service1/test_key', value=b'not valid json')

        with pytest.raises(json.JSONDecodeError):
            ConfigUpdate.from_raw(item)
