# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import threading
import time
from functools import partial
from typing import Any

from ess.livedata.config.models import ConfigKey
from ess.livedata.dashboard.message_bridge import BackgroundMessageBridge
from ess.livedata.dashboard.message_transport import FakeTransport

BackgroundMessageBridge = partial(BackgroundMessageBridge, busy_wait_sleep=0.001)


class TestBackgroundMessageBridge:
    """Test the BackgroundMessageBridge class."""

    def test_publish_when_not_running_does_not_crash(self):
        """Test that publish when bridge is not running handles gracefully."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport)

        key = ConfigKey(source_name="test", service_name="service", key="param")
        bridge.publish(key, {"value": 123})

        # Should not crash and no messages should be sent
        send_count, _ = transport.get_stats()
        assert send_count == 0

    def test_pop_all_returns_empty_dict_initially(self):
        """Test that pop_all returns empty dict when no messages received."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport)

        result = bridge.pop_all()
        assert result == {}

    def test_bridge_processes_messages_in_background_thread(self):
        """Test that bridge processes messages when running in background thread."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(
            transport, outgoing_poll_interval=0.001, incoming_poll_interval=0.001
        )

        # Start bridge in background thread
        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            # Wait for bridge to start
            time.sleep(0.01)

            # Publish a message
            key = ConfigKey(source_name="test", service_name="service", key="param")
            test_message = {"value": 123, "timestamp": time.time()}
            bridge.publish(key, test_message)

            # Wait for processing
            time.sleep(0.02)

            # Verify message was sent
            send_count, _ = transport.get_stats()
            assert send_count > 0
            assert len(transport.sent_messages) > 0
            # Check that the key exists in the last sent batch (list of tuples)
            last_batch = dict(transport.sent_messages[-1])
            assert key in last_batch

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_receives_incoming_messages(self):
        """Test that bridge receives and processes incoming messages."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, incoming_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Add incoming message
            key = ConfigKey(source_name="test", service_name="service", key="param")
            incoming_message = {key: {"value": 456}}
            transport.add_incoming_messages(incoming_message)

            # Wait for processing
            time.sleep(0.02)

            # Verify we can pop incoming messages
            messages = bridge.pop_all()
            assert key in messages
            assert messages[key]["value"] == 456

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_deduplicates_multiple_publishes_same_key(self):
        """Test that multiple publishes of same key are deduplicated."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, outgoing_poll_interval=0.01)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Publish multiple messages with same key
            key = ConfigKey(source_name="test", service_name="service", key="param")
            bridge.publish(key, {"value": 1})
            bridge.publish(key, {"value": 2})
            bridge.publish(key, {"value": 3})

            # Wait for processing
            time.sleep(0.03)

            # Should only have latest value
            assert len(transport.sent_messages) >= 1
            last_batch = dict(transport.sent_messages[-1])
            assert last_batch[key]["value"] == 3

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_throttles_high_frequency_publishing(self):
        """Test that high frequency publishing is properly throttled."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(
            transport,
            outgoing_poll_interval=0.01,  # 10ms throttling
        )

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Publish many messages quickly
            key = ConfigKey(source_name="test", service_name="service", key="param")
            message_count = 20
            for i in range(message_count):
                bridge.publish(key, {"value": i})
                time.sleep(0.001)  # 1ms between publishes

            # Wait for some processing
            time.sleep(0.02)

            # Should have fewer batches than messages due to throttling
            send_count, _ = transport.get_stats()
            assert send_count < message_count  # Should be throttled
            assert send_count > 0  # But some should go through

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_handles_incoming_message_deduplication(self):
        """Test that incoming messages are deduplicated properly."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, incoming_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Add multiple messages with same key
            key = ConfigKey(source_name="test", service_name="service", key="param")
            transport.add_incoming_messages({key: {"value": 1}})
            transport.add_incoming_messages({key: {"value": 2}})
            transport.add_incoming_messages({key: {"value": 3}})

            # Wait for processing
            time.sleep(0.02)

            # Should only get latest value
            messages = bridge.pop_all()
            assert messages[key]["value"] == 3

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_handles_concurrent_publish_and_pop_all(self):
        """Test concurrent publish and pop_all operations are thread-safe."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(
            transport, outgoing_poll_interval=0.001, incoming_poll_interval=0.001
        )

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        results = {"messages_received": []}

        try:
            time.sleep(0.01)  # Let bridge start

            def publisher():
                key = ConfigKey(source_name="test", service_name="service", key="param")
                for i in range(10):
                    bridge.publish(key, {"value": i})
                    time.sleep(0.002)

            def consumer():
                for _ in range(5):
                    messages = bridge.pop_all()
                    if messages:
                        results["messages_received"].append(messages)
                    time.sleep(0.005)

            # Add some incoming messages
            key = ConfigKey(source_name="test", service_name="service", key="incoming")
            for i in range(3):
                transport.add_incoming_messages({key: {"incoming": i}})

            # Run concurrent operations
            publisher_thread = threading.Thread(target=publisher)
            consumer_thread = threading.Thread(target=consumer)

            publisher_thread.start()
            consumer_thread.start()

            publisher_thread.join()
            consumer_thread.join()

            # Verify operations completed without errors
            send_count, receive_count = transport.get_stats()
            assert send_count > 0
            assert receive_count > 0

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_prevents_message_flooding_to_kafka(self):
        """Test that bridge prevents flooding by throttling high-frequency messages."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(
            transport,
            outgoing_poll_interval=0.05,  # 50ms throttling
        )

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Simulate flooding scenario - many rapid publishes
            flood_count = 500
            key1 = ConfigKey(source_name="test", service_name="service", key="flood1")
            key2 = ConfigKey(source_name="test", service_name="service", key="flood2")
            start_time = time.time()

            for i in range(flood_count):
                bridge.publish(key1, {"flood_msg": i})
                bridge.publish(key2, {"flood_msg": i})
                # No sleep - maximum flooding attempt

            # Wait for processing
            time.sleep(0.15)  # Allow multiple throttle intervals

            end_time = time.time()
            elapsed = end_time - start_time

            # Should have significantly fewer sends than messages due to throttling
            send_count, _ = transport.get_stats()

            # With 50ms throttling over ~150ms, expect at most 2-3 batches
            expected_max_batches = int(elapsed / 0.05) + 2  # +2 for tolerance
            assert send_count <= expected_max_batches
            assert send_count > 0  # But some should go through

            # Verify latest value was preserved despite flooding
            if transport.sent_messages:
                last_batch = dict(transport.sent_messages[-1])
                assert last_batch[key1]["flood_msg"] == flood_count - 1
                assert last_batch[key2]["flood_msg"] == flood_count - 1

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_stops_cleanly_during_processing(self):
        """Test that stop works correctly even during active processing."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, outgoing_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Start continuous publishing
            key = ConfigKey(source_name="test", service_name="service", key="param")
            for i in range(20):
                bridge.publish(key, {"value": i})
                if i == 10:  # Stop in the middle
                    bridge.stop()
                time.sleep(0.001)

            # Wait for thread to finish
            bridge_thread.join(timeout=0.1)

            # Thread should have finished
            assert not bridge_thread.is_alive()

        finally:
            if bridge_thread.is_alive():
                bridge.stop()
                bridge_thread.join(timeout=0.1)

    def test_bridge_handles_transport_failures_gracefully(self):
        """Test that transport failures are handled without crashing."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, outgoing_poll_interval=0.001)

        # Make transport fail
        transport.should_fail_send = True

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Publish message that will cause transport failure
            key = ConfigKey(source_name="test", service_name="service", key="param")
            bridge.publish(key, {"value": 123})

            # Wait for processing and potential failure
            time.sleep(0.02)

            # Thread should finish due to exception handling
            bridge_thread.join(timeout=0.1)
            assert not bridge_thread.is_alive()

        finally:
            if bridge_thread.is_alive():
                bridge.stop()
                bridge_thread.join(timeout=0.1)

    def test_bridge_processes_different_config_keys(self):
        """Test that bridge handles different ConfigKey combinations correctly."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, outgoing_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Test different key combinations
            key1 = ConfigKey(
                source_name="source1", service_name="service1", key="param1"
            )
            key2 = ConfigKey(
                source_name=None, service_name="service2", key="param2"
            )  # Wildcard source
            key3 = ConfigKey(
                source_name="source3", service_name=None, key="param3"
            )  # Wildcard service
            key4 = ConfigKey(
                source_name=None, service_name=None, key="param4"
            )  # Both wildcards

            bridge.publish(key1, {"value": "test1"})
            bridge.publish(key2, {"value": "test2"})
            bridge.publish(key3, {"value": "test3"})
            bridge.publish(key4, {"value": "test4"})

            # Wait for processing
            time.sleep(0.02)

            # Verify all messages were processed
            send_count, _ = transport.get_stats()
            assert send_count > 0

            if transport.sent_messages:
                last_batch = dict(transport.sent_messages[-1])
                assert key1 in last_batch
                assert key2 in last_batch
                assert key3 in last_batch
                assert key4 in last_batch

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_handles_empty_message_values(self):
        """Test that bridge handles empty message values correctly."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, outgoing_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Publish empty message
            key = ConfigKey(source_name="test", service_name="service", key="param")
            bridge.publish(key, {})

            # Wait for processing
            time.sleep(0.02)

            # Should handle empty messages without crashing
            send_count, _ = transport.get_stats()
            assert send_count > 0

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)

    def test_bridge_multiple_start_stop_cycles(self):
        """Test that bridge can be started and stopped multiple times."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(
            transport, outgoing_poll_interval=0.001, incoming_poll_interval=0.001
        )

        for cycle in range(3):
            bridge_thread = threading.Thread(target=bridge.start)
            bridge_thread.daemon = True
            bridge_thread.start()

            time.sleep(0.01)  # Let bridge start

            # Publish a message
            key = ConfigKey(
                source_name="test", service_name="service", key=f"cycle_{cycle}"
            )
            bridge.publish(key, {"cycle": cycle})

            time.sleep(0.01)  # Let it process

            bridge.stop()
            bridge_thread.join(timeout=0.1)

            assert not bridge_thread.is_alive()

        # Should have processed messages from all cycles
        send_count, _ = transport.get_stats()
        assert send_count >= 3

    def test_bridge_pop_all_clears_messages(self):
        """Test that pop_all clears the message queue."""
        transport = FakeTransport[ConfigKey, dict[str, Any]]()
        bridge = BackgroundMessageBridge(transport, incoming_poll_interval=0.001)

        bridge_thread = threading.Thread(target=bridge.start)
        bridge_thread.daemon = True
        bridge_thread.start()

        try:
            time.sleep(0.01)  # Let bridge start

            # Add incoming messages
            key = ConfigKey(source_name="test", service_name="service", key="param")
            transport.add_incoming_messages({key: {"value": 123}})

            # Wait for processing
            time.sleep(0.02)

            # First pop should return messages
            messages1 = bridge.pop_all()
            assert key in messages1

            # Second pop should return empty
            messages2 = bridge.pop_all()
            assert messages2 == {}

        finally:
            bridge.stop()
            bridge_thread.join(timeout=0.1)
