# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import time

import pytest

from ess.livedata.dashboard.message_transport import FakeTransport
from ess.livedata.dashboard.throttling_message_handler import ThrottlingMessageHandler


class TestThrottlingMessageHandler:
    """Test the ThrottlingMessageHandler class."""

    def test_publish_stores_message_in_outgoing_queue(self):
        """Test that publish stores messages in the outgoing queue."""
        transport = FakeTransport[str, str]()
        handler = ThrottlingMessageHandler(transport)

        handler.publish("key1", "value1")

        # Message should be queued but not sent yet
        assert transport.send_call_count == 0
        assert len(transport.sent_messages) == 0

    def test_publish_deduplicates_messages_in_same_batch(self):
        """Test that duplicate messages in the same batch are deduplicated."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.0)

        handler.publish("key1", "value1")
        handler.publish("key1", "value2")  # Should overwrite previous
        handler.publish("key2", "value3")

        has_messages = handler.process_cycle()

        assert has_messages is True
        assert transport.send_call_count == 1
        assert len(transport.sent_messages) == 1
        assert transport.sent_messages[0] == [("key1", "value2"), ("key2", "value3")]

    def test_publish_deduplicates_messages_across_batches(self):
        """Test that duplicate messages across batches are deduplicated."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.0)

        # First batch
        handler.publish("key1", "value1")
        handler.process_cycle()

        # Second batch with same key
        handler.publish("key1", "value2")
        handler.process_cycle()

        assert transport.send_call_count == 2
        assert transport.sent_messages[0] == [("key1", "value1")]
        assert transport.sent_messages[1] == [("key1", "value2")]

    def test_publish_no_deduplication_after_pop_all(self):
        """Test that messages are not deduplicated after pop_all clears the queue."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        # Add incoming message
        transport.add_incoming_messages({"key1": "incoming_value"})

        # Process to get incoming message
        handler.process_cycle()

        # Pop incoming messages (clears the incoming queue)
        incoming = handler.pop_all()
        assert incoming == {"key1": "incoming_value"}

        # Add same key again - should not be deduplicated since queue was cleared
        transport.add_incoming_messages({"key1": "incoming_value2"})
        handler.process_cycle()

        incoming2 = handler.pop_all()
        assert incoming2 == {"key1": "incoming_value2"}

    def test_outgoing_timing_throttling(self):
        """Test that outgoing messages are throttled by timing."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.1)

        handler.publish("key1", "value1")

        # First call should process messages
        has_messages = handler.process_cycle()
        assert has_messages is True
        assert transport.send_call_count == 1

        # Immediate second call should be throttled
        handler.publish("key2", "value2")
        has_messages = handler.process_cycle()
        assert has_messages is False
        assert transport.send_call_count == 1  # No additional calls

    def test_outgoing_timing_throttling_respects_interval(self):
        """Test that outgoing messages are processed after interval passes."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.01)

        handler.publish("key1", "value1")
        handler.process_cycle()
        assert transport.send_call_count == 1

        # Add another message and wait for interval
        handler.publish("key2", "value2")
        time.sleep(0.02)  # Wait longer than interval

        has_messages = handler.process_cycle()
        assert has_messages is True
        assert transport.send_call_count == 2

    def test_incoming_timing_throttling(self):
        """Test that incoming messages are throttled by timing."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.1)

        transport.add_incoming_messages({"key1": "value1"})

        # First call should process messages
        has_messages = handler.process_cycle()
        assert has_messages is True
        assert transport.receive_call_count == 1

        # Immediate second call should be throttled
        transport.add_incoming_messages({"key2": "value2"})
        has_messages = handler.process_cycle()
        assert has_messages is False
        assert transport.receive_call_count == 1  # No additional calls

    def test_incoming_timing_throttling_respects_interval(self):
        """Test that incoming messages are processed after interval passes."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.01)

        transport.add_incoming_messages({"key1": "value1"})
        handler.process_cycle()
        assert transport.receive_call_count == 1

        # Wait for interval and try again
        time.sleep(0.02)
        transport.add_incoming_messages({"key2": "value2"})

        has_messages = handler.process_cycle()
        assert has_messages is True
        assert transport.receive_call_count == 2

    def test_incoming_timing_updates_even_without_messages(self):
        """Test that incoming timing is updated even when no messages are received."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.01)

        # First call with no messages
        has_messages = handler.process_cycle()
        assert has_messages is False
        assert transport.receive_call_count == 1

        transport.add_incoming_messages({"key1": "value1"})

        # Immediate second call should be throttled
        has_messages = handler.process_cycle()
        assert has_messages is False
        assert transport.receive_call_count == 1  # No additional calls

    def test_outgoing_timing_only_updates_when_messages_sent(self):
        """Test that outgoing timing is only updated when messages are actually sent."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.01)

        # Process cycle with no messages
        has_messages = handler.process_cycle()
        assert has_messages is False
        assert transport.send_call_count == 0

        # Immediate second call should not be throttled since no messages were sent
        handler.publish("key1", "value1")
        has_messages = handler.process_cycle()
        assert has_messages is True
        assert transport.send_call_count == 1

    def test_process_cycle_returns_true_when_any_messages_processed(self):
        """Test that process_cycle returns True when any messages are processed."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(
            transport, outgoing_poll_interval=0.0, incoming_poll_interval=0.0
        )

        # Only outgoing messages
        handler.publish("key1", "value1")
        assert handler.process_cycle() is True

        # Only incoming messages
        transport.add_incoming_messages({"key2": "value2"})
        assert handler.process_cycle() is True

        # Both outgoing and incoming
        handler.publish("key3", "value3")
        transport.add_incoming_messages({"key4": "value4"})
        assert handler.process_cycle() is True

    def test_process_cycle_returns_false_when_no_messages_processed(self):
        """Test that process_cycle returns False when no messages are processed."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport)

        # No messages at all
        assert handler.process_cycle() is False

        # Messages but throttled
        handler.publish("key1", "value1")
        handler.process_cycle()  # Process first time

        handler.publish("key2", "value2")
        assert handler.process_cycle() is False  # Should be throttled

    def test_pop_all_returns_incoming_messages(self):
        """Test that pop_all returns all incoming messages."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        transport.add_incoming_messages({"key1": "value1", "key2": "value2"})
        handler.process_cycle()

        messages = handler.pop_all()
        assert messages == {"key1": "value1", "key2": "value2"}

        # Second call should return empty dict
        messages = handler.pop_all()
        assert not messages

    def test_pop_all_accumulates_messages_across_cycles(self):
        """Test that pop_all accumulates messages from multiple process cycles."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        # First batch
        transport.add_incoming_messages({"key1": "value1"})
        handler.process_cycle()

        # Second batch
        transport.add_incoming_messages({"key2": "value2"})
        handler.process_cycle()

        # Should get both messages
        messages = handler.pop_all()
        assert messages == {"key1": "value1", "key2": "value2"}

    def test_pop_all_deduplicates_incoming_messages(self):
        """Test that incoming messages are deduplicated by key."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        # First batch
        transport.add_incoming_messages({"key1": "value1"})
        handler.process_cycle()

        # Second batch with same key
        transport.add_incoming_messages({"key1": "value2"})
        handler.process_cycle()

        # Should get only the latest value
        messages = handler.pop_all()
        assert messages == {"key1": "value2"}

    def test_empty_outgoing_queue_skips_transport_send(self):
        """Test that empty outgoing queue doesn't call transport send."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.0)

        has_messages = handler.process_cycle()

        assert has_messages is False
        assert transport.send_call_count == 0

    def test_empty_incoming_messages_from_transport(self):
        """Test handling of empty incoming messages from transport."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        # Transport returns empty dict
        has_messages = handler.process_cycle()

        assert has_messages is False
        assert transport.receive_call_count == 1
        assert handler.pop_all() == {}

    def test_transport_send_failure_propagates(self):
        """Test that transport send failures propagate."""
        transport = FakeTransport()
        transport.should_fail_send = True
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.0)

        handler.publish("key1", "value1")

        # Note that in practice the transport should handle most errors internally.
        with pytest.raises(RuntimeError, match="Transport send failed"):
            handler.process_cycle()

    def test_transport_receive_failure_propagates(self):
        """Test that transport receive failures propagate."""
        transport = FakeTransport()
        transport.should_fail_receive = True
        handler = ThrottlingMessageHandler(transport, incoming_poll_interval=0.0)

        # Note that in practice the transport should handle most errors internally.
        with pytest.raises(RuntimeError, match="Transport receive failed"):
            handler.process_cycle()

    def test_multiple_messages_single_batch(self):
        """Test processing multiple messages in a single batch."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(transport, outgoing_poll_interval=0.0)

        handler.publish("key1", "value1")
        handler.publish("key2", "value2")
        handler.publish("key3", "value3")

        has_messages = handler.process_cycle()

        assert has_messages is True
        assert transport.send_call_count == 1
        assert transport.sent_messages[0] == [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]

    def test_concurrent_outgoing_and_incoming_processing(self):
        """Test that outgoing and incoming messages are processed in same cycle."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(
            transport, outgoing_poll_interval=0.0, incoming_poll_interval=0.0
        )

        # Set up both outgoing and incoming messages
        handler.publish("out_key", "out_value")
        transport.add_incoming_messages({"in_key": "in_value"})

        has_messages = handler.process_cycle()

        assert has_messages is True
        assert transport.send_call_count == 1
        assert transport.receive_call_count == 1
        assert transport.sent_messages[0] == [("out_key", "out_value")]

        # Check incoming messages were processed
        incoming = handler.pop_all()
        assert incoming == {"in_key": "in_value"}

    def test_zero_poll_intervals(self):
        """Test behavior with zero poll intervals (no throttling)."""
        transport = FakeTransport()
        handler = ThrottlingMessageHandler(
            transport, outgoing_poll_interval=0.0, incoming_poll_interval=0.0
        )

        # Should process immediately every time
        for i in range(5):
            handler.publish(f"key{i}", f"value{i}")
            assert handler.process_cycle() is True
            assert transport.send_call_count == i + 1
