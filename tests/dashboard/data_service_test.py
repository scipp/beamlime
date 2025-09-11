# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pytest

from ess.livedata.dashboard.data_service import DataService
from ess.livedata.dashboard.data_subscriber import DataSubscriber, Pipe, StreamAssembler


class FakeDataAssembler(StreamAssembler[str]):
    """Fake assembler for testing."""

    def assemble(self, data: dict[str, Any]) -> dict[str, Any]:
        return data.copy()


class FakePipe(Pipe):
    """Fake pipe for testing."""

    def __init__(self) -> None:
        self.sent_data: list[dict[str, Any]] = []

    def send(self, data: Any) -> None:
        self.sent_data.append(data)


def create_test_subscriber(keys: set[str]) -> tuple[DataSubscriber[str], FakePipe]:
    """Create a test subscriber with the given keys."""
    assembler = FakeDataAssembler(keys)
    pipe = FakePipe()
    subscriber = DataSubscriber(assembler, pipe)
    return subscriber, pipe


@pytest.fixture
def data_service() -> DataService[str, int]:
    return DataService()


@pytest.fixture
def sample_data() -> dict[str, int]:
    return {"key1": 100, "key2": 200, "key3": 300}


def test_init_creates_empty_service():
    service = DataService[str, int]()
    assert len(service) == 0


def test_setitem_stores_value(data_service: DataService[str, int]):
    data_service["key1"] = 42
    assert data_service["key1"] == 42
    assert "key1" in data_service


def test_setitem_without_subscribers_no_error(data_service: DataService[str, int]):
    data_service["key1"] = 42
    assert data_service["key1"] == 42


def test_register_subscriber_adds_to_list(data_service: DataService[str, int]):
    subscriber, _ = create_test_subscriber({"key1"})
    data_service.register_subscriber(subscriber)


def test_setitem_notifies_matching_subscriber(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    data_service["key1"] = 42

    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 42}


def test_setitem_ignores_non_matching_subscriber(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"other_key"})
    data_service.register_subscriber(subscriber)

    data_service["key1"] = 42

    assert len(pipe.sent_data) == 0


def test_setitem_notifies_multiple_matching_subscribers(
    data_service: DataService[str, int],
):
    subscriber1, pipe1 = create_test_subscriber({"key1"})
    subscriber2, pipe2 = create_test_subscriber({"key1", "key2"})
    subscriber3, pipe3 = create_test_subscriber({"key2"})

    data_service.register_subscriber(subscriber1)
    data_service.register_subscriber(subscriber2)
    data_service.register_subscriber(subscriber3)

    data_service["key1"] = 42

    assert len(pipe1.sent_data) == 1
    assert len(pipe2.sent_data) == 1
    assert len(pipe3.sent_data) == 0


def test_setitem_multiple_updates_notify_separately(
    data_service: DataService[str, int],
):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    data_service["key1"] = 42
    data_service["key2"] = 84

    assert len(pipe.sent_data) == 2
    assert pipe.sent_data[0] == {"key1": 42}
    assert pipe.sent_data[1] == {"key1": 42, "key2": 84}


def test_transaction_batches_notifications(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    with data_service.transaction():
        data_service["key1"] = 42
        data_service["key2"] = 84
        # No notifications yet
        assert len(pipe.sent_data) == 0

    # Single notification after transaction
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 42, "key2": 84}


def test_transaction_nested_batches_correctly(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2", "key3"})
    data_service.register_subscriber(subscriber)

    with data_service.transaction():
        data_service["key1"] = 42
        with data_service.transaction():
            data_service["key2"] = 84
            assert len(pipe.sent_data) == 0
        # Still in outer transaction
        assert len(pipe.sent_data) == 0
        data_service["key3"] = 126

    # Single notification after all transactions
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 42, "key2": 84, "key3": 126}


def test_transaction_exception_still_notifies(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1"})
    data_service.register_subscriber(subscriber)

    try:
        with data_service.transaction():
            data_service["key1"] = 42
            raise ValueError("test error")
    except ValueError:
        # Exception should not prevent notification
        pass

    # Notification should still happen
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 42}


def test_dictionary_operations_work(
    data_service: DataService[str, int], sample_data: dict[str, int]
):
    # Test basic dict operations
    for key, value in sample_data.items():
        data_service[key] = value

    assert len(data_service) == 3
    assert data_service["key1"] == 100
    assert data_service.get("key1") == 100
    assert data_service.get("nonexistent", 999) == 999
    assert "key1" in data_service
    assert "nonexistent" not in data_service
    assert list(data_service.keys()) == ["key1", "key2", "key3"]
    assert list(data_service.values()) == [100, 200, 300]


def test_update_method_triggers_notifications(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    data_service.update({"key1": 42, "key2": 84})

    # Should trigger notifications for each key
    assert len(pipe.sent_data) == 2


def test_clear_removes_all_data(
    data_service: DataService[str, int], sample_data: dict[str, int]
):
    data_service.update(sample_data)
    assert len(data_service) == 3

    data_service.clear()
    assert len(data_service) == 0


def test_pop_removes_and_returns_value(data_service: DataService[str, int]):
    data_service["key1"] = 42

    value = data_service.pop("key1")
    assert value == 42
    assert "key1" not in data_service


def test_setdefault_behavior(data_service: DataService[str, int]):
    value = data_service.setdefault("key1", 42)
    assert value == 42
    assert data_service["key1"] == 42

    # Second call should return existing value
    value = data_service.setdefault("key1", 999)
    assert value == 42
    assert data_service["key1"] == 42


def test_subscriber_gets_full_data_dict(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1"})
    data_service.register_subscriber(subscriber)

    # Add some initial data
    data_service["existing"] = 999
    data_service["key1"] = 42

    # Subscriber should get the full data dict
    assert pipe.sent_data[-1] == {"key1": 42}


def test_subscriber_only_gets_subscribed_keys(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key3"})
    data_service.register_subscriber(subscriber)

    # Add data for subscribed and unsubscribed keys
    data_service["key1"] = 42
    data_service["key2"] = 84  # Not subscribed to this key
    data_service["key3"] = 126
    data_service["unrelated"] = 999  # Not subscribed to this key

    # Subscriber should only receive data for keys it's interested in
    expected_data = {"key1": 42, "key3": 126}
    assert pipe.sent_data[-1] == expected_data

    # Verify unrelated keys are not included
    assert "key2" not in pipe.sent_data[-1]
    assert "unrelated" not in pipe.sent_data[-1]


def test_empty_transaction_no_notifications(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1"})
    data_service.register_subscriber(subscriber)

    with data_service.transaction():
        pass  # No changes

    assert len(pipe.sent_data) == 0


def test_delitem_notifies_subscribers(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    # Add some data first
    data_service["key1"] = 42
    data_service["key2"] = 84
    pipe.sent_data.clear()  # Clear previous notifications

    # Delete a key
    del data_service["key1"]

    # Should notify with remaining data
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key2": 84}
    assert "key1" not in data_service


def test_delitem_in_transaction_batches_notifications(
    data_service: DataService[str, int],
):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    # Add some data first
    data_service["key1"] = 42
    data_service["key2"] = 84
    pipe.sent_data.clear()  # Clear previous notifications

    with data_service.transaction():
        del data_service["key1"]
        data_service["key2"] = 99
        # No notifications yet
        assert len(pipe.sent_data) == 0

    # Single notification after transaction
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key2": 99}


def test_transaction_set_then_del_same_key(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    # Add some initial data
    data_service["key2"] = 84
    pipe.sent_data.clear()

    with data_service.transaction():
        data_service["key1"] = 42  # Set key1
        del data_service["key1"]  # Then delete key1
        # No notifications yet
        assert len(pipe.sent_data) == 0

    # After transaction: key1 should not exist, only key2 should be in notification
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key2": 84}
    assert "key1" not in data_service


def test_transaction_del_then_set_same_key(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1", "key2"})
    data_service.register_subscriber(subscriber)

    # Add some initial data
    data_service["key1"] = 42
    data_service["key2"] = 84
    pipe.sent_data.clear()

    with data_service.transaction():
        del data_service["key1"]  # Delete key1
        data_service["key1"] = 99  # Then set key1 to new value
        # No notifications yet
        assert len(pipe.sent_data) == 0

    # After transaction: key1 should have the new value
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 99, "key2": 84}
    assert data_service["key1"] == 99


def test_transaction_multiple_operations_same_key(data_service: DataService[str, int]):
    subscriber, pipe = create_test_subscriber({"key1"})
    data_service.register_subscriber(subscriber)

    # Add initial data
    data_service["key1"] = 10
    pipe.sent_data.clear()

    with data_service.transaction():
        data_service["key1"] = 20  # Update
        data_service["key1"] = 30  # Update again
        del data_service["key1"]  # Delete
        data_service["key1"] = 40  # Set again
        # No notifications yet
        assert len(pipe.sent_data) == 0

    # After transaction: key1 should have final value
    assert len(pipe.sent_data) == 1
    assert pipe.sent_data[0] == {"key1": 40}
    assert data_service["key1"] == 40
