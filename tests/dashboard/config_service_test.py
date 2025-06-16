# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.models import ConfigKey, TOARange
from beamlime.dashboard.config_service import (
    ConfigSchemaManager,
    ConfigService,
    FakeMessageBridge,
)
from beamlime.dashboard.detector_config import TOARangeParam
from beamlime.handlers.config_handler import ConfigUpdate


@pytest.fixture
def config_key():
    return ConfigKey(service_name="detector_data", key="toa_range")


@pytest.fixture
def service(config_key: ConfigKey):
    schemas = ConfigSchemaManager({config_key: TOARange})
    return ConfigService(schema_manager=schemas)


@pytest.fixture
def service_with_bridge(config_key: ConfigKey):
    schemas = ConfigSchemaManager({config_key: TOARange})
    bridge = FakeMessageBridge()
    return ConfigService(schema_manager=schemas, message_bridge=bridge), bridge


class TestConfigService:
    def test_get_setter(self, service: ConfigService, config_key: ConfigKey):
        setter = service.get_setter(config_key)
        setter(enabled=True, low=0.0, high=72_000.0, unit='us')

    def test_subscriber(self, service_with_bridge, config_key: ConfigKey):
        service, bridge = service_with_bridge
        toa_range = TOARangeParam()
        service.subscribe(key=config_key, callback=toa_range.param_updater())

        # Create a config update and add it to the bridge
        config_data = {'enabled': False, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'}
        update = ConfigUpdate(config_key=config_key, value=config_data)
        bridge.add_incoming_message(update)

        # Process the message
        service.process_incoming_messages()

        assert toa_range.enabled is False
        assert toa_range.low == 1000.0
        assert toa_range.high == 2000.0
        assert toa_range.unit == 'us'
