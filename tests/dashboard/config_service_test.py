# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.models import TOARange
from beamlime.dashboard.config_service import (
    ConfigSchemaManager,
    ConfigService,
    FakeMessageBridge,
    LoopbackMessageBridge,
)
from beamlime.dashboard.detector_config import TOARangeParam


@pytest.fixture
def config_key():
    return "toa_range"


@pytest.fixture
def service(config_key: str):
    schemas = ConfigSchemaManager({config_key: TOARange})
    return ConfigService(schema_validator=schemas)


@pytest.fixture
def service_with_bridge(config_key: str):
    schemas = ConfigSchemaManager({config_key: TOARange})
    bridge = FakeMessageBridge()
    return ConfigService(schema_validator=schemas, message_bridge=bridge), bridge


@pytest.fixture
def service_with_loopback(config_key: str):
    schemas = ConfigSchemaManager({config_key: TOARange})
    bridge = LoopbackMessageBridge()
    return ConfigService(schema_validator=schemas, message_bridge=bridge), bridge


class TestConfigService:
    def test_subscriber(self, service_with_bridge, config_key: str):
        service, bridge = service_with_bridge
        toa_range = TOARangeParam()
        service.subscribe(key=config_key, callback=toa_range.from_pydantic())

        # Create a config update and add it to the bridge
        config_data = {'enabled': False, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'}
        bridge.add_incoming_message((config_key, config_data))

        # Process the message
        service.process_incoming_messages()

        assert toa_range.enabled is False
        assert toa_range.low == 1000.0
        assert toa_range.high == 2000.0
        assert toa_range.unit == 'us'

    def test_bidirectional_param_binding_no_infinite_cycle(self, service_with_loopback):
        """Test that bidirectional param binding doesn't cause infinite cycles."""
        service, bridge = service_with_loopback

        toa_range = TOARangeParam()
        toa_range.subscribe(service)

        # Init bridge with a single message
        bridge.add_incoming_message(
            (
                toa_range.config_key,
                {'enabled': True, 'low': 1000.0, 'high': 2000.0, 'unit': 'us'},
            )
        )

        assert len(bridge.messages) == 1

        # Simulate GUI change (should publish to bridge)
        toa_range.low = 1500.0
        assert len(bridge.messages) == 2

        # Process the external message. Should NOT trigger another update to the bridge
        service.process_incoming_messages(num=1)
        assert toa_range.low == 1000.0
        assert len(bridge.messages) == 1

        # Process our own update. Should NOT trigger another update to the bridge
        service.process_incoming_messages(num=1)
        assert toa_range.low == 1500.0
        assert len(bridge.messages) == 0
