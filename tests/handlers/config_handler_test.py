# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import json

import pytest

from beamlime.config.models import ConfigKey
from beamlime.core.message import CONFIG_STREAM_ID, Message
from beamlime.handlers.config_handler import ConfigHandler, ConfigUpdate
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


class TestConfigHandler:
    def test_init(self):
        handler = ConfigHandler(service_name="my_service")
        config = handler.get_config("source1")
        assert config == {}

    def test_get_config_returns_isolated_copies(self):
        handler = ConfigHandler(service_name="my_service")

        # Add a global config value
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/key1',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(messages)

        # Get config for specific source
        config1 = handler.get_config("source1")

        # Verify it contains the global values
        assert config1["key1"] == "value1"

        # Modify the returned config
        config1["key2"] = "local_value"

        # Get config for another source, should not have the local changes
        config2 = handler.get_config("source2")
        assert config2["key1"] == "value1"
        assert "key2" not in config2

    def test_handle_for_specific_source(self):
        handler = ConfigHandler(service_name="my_service")

        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Check that source-specific config has been updated
        assert handler.get_config("source1")["key1"] == "value1"

        # Other sources should not have this config
        assert "key1" not in handler.get_config("source2")

    def test_handle_for_all_sources(self):
        handler = ConfigHandler(service_name="my_service")

        # Add a source-specific config first
        source_messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=json.dumps("source1-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(source_messages)

        # Now add a global config
        global_messages = [
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/key1',
                    value=json.dumps("global-value").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(global_messages)

        # Check that source-specific config is updated
        assert handler.get_config("source1")["key1"] == "global-value"

        # Check that new sources get the global config
        assert handler.get_config("new_source")["key1"] == "global-value"

    def test_get_config_after_updates(self):
        handler = ConfigHandler(service_name="my_service")

        # Add global config first
        global_messages = [
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/common_key',
                    value=json.dumps("common-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(global_messages)

        # Add source-specific configs
        source_messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/specific_key',
                    value=json.dumps("specific-value").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(source_messages)

        # Get config for a completely new source
        new_source_config = handler.get_config("new_source")

        # Should have global keys but not source-specific ones
        assert new_source_config["common_key"] == "common-value"
        assert "specific_key" not in new_source_config

        # The original source should have both keys
        source1_config = handler.get_config("source1")
        assert source1_config["common_key"] == "common-value"
        assert source1_config["specific_key"] == "specific-value"

    def test_register_and_trigger_action(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # Send a message that should trigger the action
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps(42).encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Verify action was called with correct parameters
        assert len(action_calls) == 1
        assert action_calls[0] == ("source1", 42)

    def test_multiple_actions_for_same_key(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up mock actions
        action1_calls = []
        action2_calls = []

        def mock_action1(source_name: str, value):
            action1_calls.append((source_name, value))

        def mock_action2(source_name: str, value):
            action2_calls.append((source_name, value))

        # Register both actions for the same key
        handler.register_action(key="test_key", action=mock_action1)
        handler.register_action(key="test_key", action=mock_action2)

        # Send a message that should trigger both actions
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps(42).encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Verify both actions were called
        assert len(action1_calls) == 1
        assert len(action2_calls) == 1
        assert action1_calls[0] == ("source1", 42)
        assert action2_calls[0] == ("source1", 42)

    def test_ignore_messages_for_other_services(self):
        handler = ConfigHandler(service_name="my_service")

        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/other_service/key1',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Config should not be updated for any source
        assert "key1" not in handler.get_config("source1")
        assert "key1" not in handler.get_config("other_source")

    def test_global_service_wildcard(self):
        handler = ConfigHandler(service_name="my_service")

        # Send a message with service wildcard
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/*/key1',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Should be processed (wildcard matches any service)
        assert handler.get_config("source1")["key1"] == "value1"

        # But should not apply to other sources
        assert "key1" not in handler.get_config("source2")

    def test_global_source_wildcard(self):
        handler = ConfigHandler(service_name="my_service")

        # Send a message with source wildcard
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/key1',
                    value=json.dumps("global-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Should be available for any source
        assert handler.get_config("source1")["key1"] == "global-value"
        assert handler.get_config("source2")["key1"] == "global-value"
        assert handler.get_config("new_source")["key1"] == "global-value"

    def test_action_exception_handling(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up an action that will raise an exception
        def failing_action(source_name: str, value):
            raise KeyError("Test exception")

        # Register the action
        handler.register_action(key="test_key", action=failing_action)

        # Send a message that should trigger the action
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps(42).encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        # Exception should be caught, not propagated
        handler.handle(messages)

        # Config should still be updated despite action failure
        assert handler.get_config("source1")["test_key"] == 42

    def test_message_exception_handling(self):
        handler = ConfigHandler(service_name="my_service")

        # Send a message with invalid JSON
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=b'not valid json',
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        # Exception should be caught, not propagated
        handler.handle(messages)

        # No config should be updated due to the error
        assert "key1" not in handler.get_config("source1")

    def test_global_config_override_local_config(self):
        handler = ConfigHandler(service_name="my_service")

        # Add source-specific config first
        source_messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=json.dumps("source1-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(source_messages)

        # Later add global config with same key
        global_messages = [
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/key1',
                    value=json.dumps("global-value").encode('utf-8'),
                ),
                timestamp=123456890,  # Later timestamp
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(global_messages)

        # All sources should now have the global value
        assert handler.get_config("source1")["key1"] == "global-value"
        assert handler.get_config("source2")["key1"] == "global-value"

        # Now override with a new source-specific value
        override_messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=json.dumps("override-value").encode('utf-8'),
                ),
                timestamp=123456900,  # Even later timestamp
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(override_messages)

        # Source1 should have the override, other sources should keep the global value
        assert handler.get_config("source1")["key1"] == "override-value"
        assert handler.get_config("source2")["key1"] == "global-value"

    def test_process_messages_with_none_service(self):
        handler = ConfigHandler(service_name="my_service")
        config_key = ConfigKey(source_name="source1", service_name=None, key="key1")
        key_str = str(config_key)

        messages = [
            Message(
                value=RawConfigItem(
                    key=key_str.encode('utf-8'),
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]

        handler.handle(messages)

        # Should be processed since None service_name applies to all services
        assert handler.get_config("source1")["key1"] == "value1"

    def test_filter_mixed_service_messages(self):
        handler = ConfigHandler(service_name="my_service")

        # Create a batch of messages with different service targets
        messages = [
            # This one is for our service and should be processed
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/key1',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            ),
            # This one is for a different service and should be ignored
            Message(
                value=RawConfigItem(
                    key=b'source1/other_service/key2',
                    value=json.dumps("value2").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
            # This one has a wildcard service and should be processed
            Message(
                value=RawConfigItem(
                    key=b'source1/*/key3',
                    value=json.dumps("value3").encode('utf-8'),
                ),
                timestamp=123456791,
                stream=CONFIG_STREAM_ID,
            ),
        ]

        handler.handle(messages)

        config = handler.get_config("source1")
        # Should have processed key1 and key3, but not key2
        assert config["key1"] == "value1"
        assert "key2" not in config
        assert config["key3"] == "value3"

    def test_action_triggered_for_each_source_with_same_key(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # Send messages with same key but different source names
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps(42).encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source2/my_service/test_key',
                    value=json.dumps(43).encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source3/my_service/test_key',
                    value=json.dumps(44).encode('utf-8'),
                ),
                timestamp=123456791,
                stream=CONFIG_STREAM_ID,
            ),
        ]

        handler.handle(messages)

        # Verify action was called for each source
        assert len(action_calls) == 3
        assert ("source1", 42) in action_calls
        assert ("source2", 43) in action_calls
        assert ("source3", 44) in action_calls

    def test_action_triggered_for_multiple_global_and_source_specific_messages(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # Send a mix of global and source-specific messages with the same key
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("source1-value").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/test_key',
                    value=json.dumps("global-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source2/my_service/test_key',
                    value=json.dumps("source2-value").encode('utf-8'),
                ),
                timestamp=123456791,
                stream=CONFIG_STREAM_ID,
            ),
        ]

        handler.handle(messages)

        assert len(action_calls) == 2
        assert ("source2", "source2-value") in action_calls
        assert ("source1", "global-value") in action_calls

    def test_action_triggered_for_same_key_multiple_batches(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # First batch with a message for source1
        batch1 = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("value1").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            )
        ]
        handler.handle(batch1)

        # Second batch with messages for source2 and source3
        batch2 = [
            Message(
                value=RawConfigItem(
                    key=b'source2/my_service/test_key',
                    value=json.dumps("value2").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source3/my_service/test_key',
                    value=json.dumps("value3").encode('utf-8'),
                ),
                timestamp=123456791,
                stream=CONFIG_STREAM_ID,
            ),
        ]
        handler.handle(batch2)

        # Verify action was called for each source
        assert len(action_calls) == 3
        assert ("source1", "value1") in action_calls
        assert ("source2", "value2") in action_calls
        assert ("source3", "value3") in action_calls

    def test_action_triggered_for_duplicate_sources_different_values(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # Send multiple messages for the same source but with different values
        messages = [
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("first-value").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("updated-value").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
        ]

        handler.handle(messages)

        # Verify action was called once with the final value
        assert len(action_calls) == 1
        assert action_calls[0] == ("source1", "updated-value")

    def test_action_triggered_once_per_source_for_same_key(self):
        handler = ConfigHandler(service_name="my_service")

        # Set up a mock action
        action_calls = []

        def mock_action(source_name: str, value):
            action_calls.append((source_name, value))

        # Register the action
        handler.register_action(key="test_key", action=mock_action)

        # Send messages with multiple updates for different sources
        messages = [
            # First source has two updates
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("source1-first").encode('utf-8'),
                ),
                timestamp=123456789,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'source1/my_service/test_key',
                    value=json.dumps("source1-final").encode('utf-8'),
                ),
                timestamp=123456795,
                stream=CONFIG_STREAM_ID,
            ),
            # Global update
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/test_key',
                    value=json.dumps("global-first").encode('utf-8'),
                ),
                timestamp=123456791,
                stream=CONFIG_STREAM_ID,
            ),
            Message(
                value=RawConfigItem(
                    key=b'*/my_service/test_key',
                    value=json.dumps("global-final").encode('utf-8'),
                ),
                timestamp=123456792,
                stream=CONFIG_STREAM_ID,
            ),
            # Second source has one update
            Message(
                value=RawConfigItem(
                    key=b'source2/my_service/test_key',
                    value=json.dumps("source2-value").encode('utf-8'),
                ),
                timestamp=123456790,
                stream=CONFIG_STREAM_ID,
            ),
        ]

        handler.handle(messages)

        # Verify action was called once per source with the final value
        assert len(action_calls) == 2  # One for each source
        assert ("source1", "global-final") in action_calls
        assert ("source2", "source2-value") in action_calls
