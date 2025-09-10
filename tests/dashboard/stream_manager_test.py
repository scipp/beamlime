# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from typing import Any

import pytest
import scipp as sc

from beamlime.config.workflow_spec import JobId, ResultKey, WorkflowId
from beamlime.dashboard.data_service import DataService
from beamlime.dashboard.data_subscriber import Pipe, StreamAssembler
from beamlime.dashboard.stream_manager import StreamManager


class FakePipe(Pipe):
    """Fake implementation of Pipe for testing."""

    def __init__(self, data: Any) -> None:
        self.send_calls: list[Any] = []
        self.data: Any = data

    def send(self, data: Any) -> None:
        self.send_calls.append(data)
        self.data = data


class FakeStreamAssembler(StreamAssembler):
    """Fake implementation of StreamAssembler for testing."""

    def __init__(self, keys: set[Any]) -> None:
        super().__init__(keys)
        self.assemble_calls: list[dict[Any, Any]] = []

    def assemble(self, data: dict[Any, Any]) -> Any:
        self.assemble_calls.append(data.copy())
        return data


class FakePipeFactory:
    """Fake pipe factory for testing."""

    def __init__(self) -> None:
        self.call_count = 0
        self.created_pipes: list[FakePipe] = []

    def __call__(self, data: Any) -> FakePipe:
        self.call_count += 1
        pipe = FakePipe(data)
        self.created_pipes.append(pipe)
        return pipe


@pytest.fixture
def data_service() -> DataService:
    """Real DataService instance for testing."""
    return DataService()


@pytest.fixture
def fake_pipe_factory() -> FakePipeFactory:
    """Fake pipe factory that creates FakePipe instances."""
    return FakePipeFactory()


@pytest.fixture
def sample_data() -> sc.DataArray:
    """Sample data array for testing."""
    return sc.DataArray(
        data=sc.array(dims=['x'], values=[1, 2, 3]),
        coords={'x': sc.array(dims=['x'], values=[10, 20, 30])},
    )


class TestStreamManager:
    """Test cases for base StreamManager class."""

    def test_make_merging_stream_creates_pipe_and_registers_subscriber(
        self, data_service, fake_pipe_factory
    ):
        """Test that make_merging_stream creates a pipe and registers a subscriber."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        keys = {
            ResultKey(
                workflow_id=WorkflowId(
                    instrument="test", namespace="ns", name="wf", version=1
                ),
                job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
            )
        }

        pipe = manager.make_merging_stream(keys)

        assert isinstance(pipe, FakePipe)
        assert fake_pipe_factory.call_count == 1
        assert len(data_service._subscribers) == 1

    def test_partial_data_updates(self, data_service, fake_pipe_factory, sample_data):
        """Test handling of partial data updates when only some keys have data."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        key1 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf1", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )
        key2 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf2", version=1
            ),
            job_id=JobId(source_name="source2", job_number=uuid.uuid4()),
        )

        keys = {key1, key2}
        pipe = manager.make_merging_stream(keys)

        # Send data for only one key
        data_service[key1] = sample_data

        # Should receive partial data
        assert len(pipe.send_calls) == 1
        assert key1 in pipe.send_calls[0]
        assert key2 not in pipe.send_calls[0]
        assert sc.identical(pipe.send_calls[0][key1], sample_data)

    def test_stream_independence(self, data_service, fake_pipe_factory, sample_data):
        """Test that multiple streams operate independently."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Create two independent streams with different keys
        key1 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf1", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )
        key2 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf2", version=1
            ),
            job_id=JobId(source_name="source2", job_number=uuid.uuid4()),
        )

        pipe1 = manager.make_merging_stream({key1})
        pipe2 = manager.make_merging_stream({key2})

        # Send data for key1
        data_service[key1] = sample_data

        # Only pipe1 should receive data
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 0

        # Send data for key2
        data_service[key2] = sample_data

        # Now pipe2 should also have data, pipe1 unchanged
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1

    def test_single_source_data_flow(
        self, data_service, fake_pipe_factory, sample_data
    ):
        """Test data flow with a single source."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )

        pipe = manager.make_merging_stream({key})

        # Publish data
        data_service[key] = sample_data

        # Verify data received
        assert len(pipe.send_calls) == 1
        assert pipe.send_calls[0] == {key: sample_data}

    def test_multiple_sources_data_flow(
        self, data_service, fake_pipe_factory, sample_data
    ):
        """Test data flow with multiple sources."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        key1 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf1", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )
        key2 = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf2", version=1
            ),
            job_id=JobId(source_name="source2", job_number=uuid.uuid4()),
        )

        keys = {key1, key2}
        pipe = manager.make_merging_stream(keys)

        # Publish data for both keys
        sample_data2 = sc.DataArray(
            data=sc.array(dims=['y'], values=[4, 5, 6]),
            coords={'y': sc.array(dims=['y'], values=[40, 50, 60])},
        )

        data_service[key1] = sample_data
        data_service[key2] = sample_data2

        # Should receive data for both keys
        assert len(pipe.send_calls) == 2
        # First call has only key1
        assert pipe.send_calls[0] == {key1: sample_data}
        # Second call has both keys
        assert pipe.send_calls[1] == {key1: sample_data, key2: sample_data2}

    def test_incremental_updates(self, data_service, fake_pipe_factory, sample_data):
        """Test that incremental updates flow through correctly."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )

        pipe = manager.make_merging_stream({key})

        # Send initial data
        data_service[key] = sample_data

        # Send updated data
        updated_data = sc.DataArray(
            data=sc.array(dims=['x'], values=[7, 8, 9]),
            coords={'x': sc.array(dims=['x'], values=[70, 80, 90])},
        )
        data_service[key] = updated_data

        # Should receive both updates
        assert len(pipe.send_calls) == 2
        assert pipe.send_calls[0] == {key: sample_data}
        assert pipe.send_calls[1] == {key: updated_data}

    def test_empty_source_set(self, data_service, fake_pipe_factory):
        """Test behavior with empty source set."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Create stream with empty key set
        pipe = manager.make_merging_stream(set())

        # Publish some data
        key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="wf", version=1
            ),
            job_id=JobId(source_name="source1", job_number=uuid.uuid4()),
        )
        data_service[key] = sc.DataArray(data=sc.array(dims=[], values=[1]))

        # Should not receive any data
        assert len(pipe.send_calls) == 0

    def test_shared_source_triggering(
        self, data_service, fake_pipe_factory, sample_data
    ):
        """Test that shared sources trigger multiple streams appropriately."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Create shared key
        shared_key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="shared", version=1
            ),
            job_id=JobId(source_name="shared_source", job_number=uuid.uuid4()),
        )

        # Create two streams that both include the shared key
        pipe1 = manager.make_merging_stream({shared_key})
        pipe2 = manager.make_merging_stream({shared_key})

        # Publish data to shared key
        data_service[shared_key] = sample_data

        # Both pipes should receive the data
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1
        assert pipe1.send_calls[0] == {shared_key: sample_data}
        assert pipe2.send_calls[0] == {shared_key: sample_data}

    def test_unrelated_key_filtering(
        self, data_service, fake_pipe_factory, sample_data
    ):
        """Test that unrelated keys are filtered out."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Create stream with specific key
        target_key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="target", version=1
            ),
            job_id=JobId(source_name="target_source", job_number=uuid.uuid4()),
        )

        unrelated_key = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="unrelated", version=1
            ),
            job_id=JobId(source_name="unrelated_source", job_number=uuid.uuid4()),
        )

        pipe = manager.make_merging_stream({target_key})

        # Publish data for unrelated key
        data_service[unrelated_key] = sample_data

        # Should not receive any data
        assert len(pipe.send_calls) == 0

        # Publish data for target key
        data_service[target_key] = sample_data

        # Should receive data now
        assert len(pipe.send_calls) == 1
        assert pipe.send_calls[0] == {target_key: sample_data}

    def test_complex_multi_stream_scenario(self, data_service, fake_pipe_factory):
        """Test complex scenario with multiple streams and overlapping keys."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Create multiple keys
        key_a = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="a", version=1
            ),
            job_id=JobId(source_name="source_a", job_number=uuid.uuid4()),
        )
        key_b = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="b", version=1
            ),
            job_id=JobId(source_name="source_b", job_number=uuid.uuid4()),
        )
        key_c = ResultKey(
            workflow_id=WorkflowId(
                instrument="test", namespace="ns", name="c", version=1
            ),
            job_id=JobId(source_name="source_c", job_number=uuid.uuid4()),
        )

        # Create streams with overlapping keys
        pipe1 = manager.make_merging_stream({key_a, key_b})  # a, b
        pipe2 = manager.make_merging_stream({key_b, key_c})  # b, c
        pipe3 = manager.make_merging_stream({key_a})  # a only

        # Create sample data
        data_a = sc.DataArray(data=sc.array(dims=[], values=[1]))
        data_b = sc.DataArray(data=sc.array(dims=[], values=[2]))
        data_c = sc.DataArray(data=sc.array(dims=[], values=[3]))

        # Publish data in sequence
        data_service[key_a] = data_a
        data_service[key_b] = data_b
        data_service[key_c] = data_c

        # Verify pipe1 (keys a, b)
        assert len(pipe1.send_calls) == 2
        assert pipe1.send_calls[0] == {key_a: data_a}
        assert pipe1.send_calls[1] == {key_a: data_a, key_b: data_b}

        # Verify pipe2 (keys b, c)
        assert len(pipe2.send_calls) == 2
        assert pipe2.send_calls[0] == {key_b: data_b}
        assert pipe2.send_calls[1] == {key_b: data_b, key_c: data_c}

        # Verify pipe3 (key a only)
        assert len(pipe3.send_calls) == 1
        assert pipe3.send_calls[0] == {key_a: data_a}
