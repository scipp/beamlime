# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pytest

from ess.livedata.dashboard.data_subscriber import DataSubscriber, Pipe, StreamAssembler


class FakeStreamAssembler(StreamAssembler[str]):
    """Fake implementation of StreamAssembler for testing."""

    def __init__(self, keys: set[str], return_value: Any = None) -> None:
        super().__init__(keys)
        self.return_value = return_value
        self.assemble_calls: list[dict[str, Any]] = []

    def assemble(self, data: dict[str, Any]) -> Any:
        self.assemble_calls.append(data.copy())
        return self.return_value


class FakePipe(Pipe):
    """Fake implementation of Pipe for testing."""

    def __init__(self) -> None:
        self.send_calls: list[Any] = []

    def send(self, data: Any) -> None:
        self.send_calls.append(data)


@pytest.fixture
def sample_keys() -> set[str]:
    """Sample data keys for testing."""
    return {'key1', 'key2', 'key3'}


@pytest.fixture
def fake_assembler(sample_keys: set[str]) -> FakeStreamAssembler:
    """Fake assembler with sample keys."""
    return FakeStreamAssembler(sample_keys, 'assembled_data')


@pytest.fixture
def fake_pipe() -> FakePipe:
    """Fake pipe for testing."""
    return FakePipe()


@pytest.fixture
def subscriber(
    fake_assembler: FakeStreamAssembler, fake_pipe: FakePipe
) -> DataSubscriber[str]:
    """DataSubscriber instance for testing."""
    return DataSubscriber(fake_assembler, fake_pipe)


class TestDataSubscriber:
    """Test cases for DataSubscriber class."""

    def test_init_stores_assembler_and_pipe(
        self, fake_assembler: FakeStreamAssembler, fake_pipe: FakePipe
    ) -> None:
        """Test that initialization stores the assembler and pipe correctly."""
        subscriber = DataSubscriber(fake_assembler, fake_pipe)

        assert subscriber._assembler is fake_assembler
        assert subscriber._pipe is fake_pipe

    def test_keys_returns_assembler_keys(
        self, subscriber: DataSubscriber, sample_keys: set[str]
    ) -> None:
        """Test that keys property returns the assembler's keys."""
        assert subscriber.keys == sample_keys

    def test_trigger_with_complete_data(
        self,
        subscriber: DataSubscriber,
        fake_assembler: FakeStreamAssembler,
        fake_pipe: FakePipe,
    ) -> None:
        """Test trigger method when all required keys are present in store."""
        store = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
            'extra_key': 'extra_value',
        }

        subscriber.trigger(store)

        # Verify assembler was called with the correct subset of data
        assert len(fake_assembler.assemble_calls) == 1
        expected_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
        assert fake_assembler.assemble_calls[0] == expected_data

        # Verify pipe was called with assembled data
        assert len(fake_pipe.send_calls) == 1
        assert fake_pipe.send_calls[0] == 'assembled_data'

    def test_trigger_with_partial_data(
        self,
        subscriber: DataSubscriber,
        fake_assembler: FakeStreamAssembler,
        fake_pipe: FakePipe,
    ) -> None:
        """Test trigger method when only some required keys are present in store."""
        store = {'key1': 'value1', 'key3': 'value3', 'unrelated_key': 'unrelated_value'}

        subscriber.trigger(store)

        # Verify assembler was called with only available keys
        assert len(fake_assembler.assemble_calls) == 1
        expected_data = {'key1': 'value1', 'key3': 'value3'}
        assert fake_assembler.assemble_calls[0] == expected_data

        # Verify pipe was called
        assert len(fake_pipe.send_calls) == 1
        assert fake_pipe.send_calls[0] == 'assembled_data'

    def test_trigger_with_empty_store(
        self,
        subscriber: DataSubscriber,
        fake_assembler: FakeStreamAssembler,
        fake_pipe: FakePipe,
    ) -> None:
        """Test trigger method with an empty store."""
        store: dict[str, Any] = {}

        subscriber.trigger(store)

        # Verify assembler was called with empty data
        assert len(fake_assembler.assemble_calls) == 1
        assert fake_assembler.assemble_calls[0] == {}

        # Verify pipe was called
        assert len(fake_pipe.send_calls) == 1
        assert fake_pipe.send_calls[0] == 'assembled_data'

    def test_trigger_with_no_matching_keys(
        self,
        subscriber: DataSubscriber,
        fake_assembler: FakeStreamAssembler,
        fake_pipe: FakePipe,
    ) -> None:
        """Test trigger method when store contains no matching keys."""
        store = {'other_key1': 'value1', 'other_key2': 'value2'}

        subscriber.trigger(store)

        # Verify assembler was called with empty data
        assert len(fake_assembler.assemble_calls) == 1
        assert fake_assembler.assemble_calls[0] == {}

        # Verify pipe was called
        assert len(fake_pipe.send_calls) == 1
        assert fake_pipe.send_calls[0] == 'assembled_data'

    def test_trigger_multiple_calls(
        self,
        subscriber: DataSubscriber,
        fake_assembler: FakeStreamAssembler,
        fake_pipe: FakePipe,
    ) -> None:
        """Test multiple calls to trigger method."""
        store1 = {'key1': 'value1', 'key2': 'value2'}
        store2 = {'key2': 'updated_value2', 'key3': 'value3'}

        subscriber.trigger(store1)
        subscriber.trigger(store2)

        # Verify both calls were processed
        assert len(fake_assembler.assemble_calls) == 2
        assert fake_assembler.assemble_calls[0] == {'key1': 'value1', 'key2': 'value2'}
        assert fake_assembler.assemble_calls[1] == {
            'key2': 'updated_value2',
            'key3': 'value3',
        }

        assert len(fake_pipe.send_calls) == 2
        assert all(call == 'assembled_data' for call in fake_pipe.send_calls)

    def test_trigger_with_different_assembled_data(self, sample_keys: set[str]) -> None:
        """Test trigger method with assembler that returns different data types."""
        assembled_values = [42, {'result': 'success'}, [1, 2, 3], None]

        for value in assembled_values:
            assembler = FakeStreamAssembler(sample_keys, value)
            pipe = FakePipe()
            subscriber = DataSubscriber(assembler, pipe)

            store = {'key1': 'test_value'}
            subscriber.trigger(store)

            assert len(pipe.send_calls) == 1
            assert pipe.send_calls[0] == value
