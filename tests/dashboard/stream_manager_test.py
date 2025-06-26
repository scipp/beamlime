# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pytest
import scipp as sc

from beamlime.dashboard.data_key import DataKey, MonitorDataKey
from beamlime.dashboard.data_service import DataService
from beamlime.dashboard.data_subscriber import Pipe, StreamAssembler
from beamlime.dashboard.stream_manager import (
    MonitorStreamManager,
    ReductionStreamManager,
    StreamManager,
)


class FakePipe(Pipe):
    """Fake implementation of Pipe for testing."""

    def __init__(self) -> None:
        self.send_calls: list[Any] = []
        self.data: Any = None

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

    def __call__(self) -> FakePipe:
        self.call_count += 1
        pipe = FakePipe()
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

    def test_init_stores_data_service_and_pipe_factory(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that initialization stores data service and pipe factory."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        assert manager.data_service is data_service


class TestMonitorStreamManager:
    """Test cases for MonitorStreamManager class."""

    def test_get_stream_caches_pipes_by_component(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that get_stream caches pipes by component name."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Same component returns same pipe
        pipe1 = manager.get_stream('test_component')
        pipe2 = manager.get_stream('test_component')
        assert pipe1 is pipe2
        assert fake_pipe_factory.call_count == 1

        # Different component creates new pipe
        pipe3 = manager.get_stream('other_component')
        assert pipe3 is not pipe1
        assert fake_pipe_factory.call_count == 2

    def test_data_flow_with_complete_monitor_data(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test data flow through monitor stream with complete data."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set complete data in transaction
        with data_service.transaction():
            data_service[monitor_key.cumulative_key()] = sample_data
            data_service[monitor_key.current_key()] = sample_data * 2

        # Verify complete data was sent
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert hasattr(sent_data, 'cumulative')
        assert hasattr(sent_data, 'current')
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sc.identical(sent_data.current, sample_data * 2)

    def test_monitor_stream_handles_partial_data_updates(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that monitor stream handles partial data updates correctly."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set only cumulative data first
        data_service[monitor_key.cumulative_key()] = sample_data
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sent_data.current is None

        # Add current data
        data_service[monitor_key.current_key()] = sample_data * 2
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sc.identical(sent_data.current, sample_data * 2)

    def test_monitor_streams_are_independent(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that multiple monitor streams are triggered independently."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream('component1')
        pipe2 = manager.get_stream('component2')

        monitor_key1 = MonitorDataKey(component_name='component1', view_name='')
        monitor_key2 = MonitorDataKey(component_name='component2', view_name='')

        # Update component1 data
        data_service[monitor_key1.cumulative_key()] = sample_data
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 0

        # Update component2 data
        data_service[monitor_key2.current_key()] = sample_data * 2
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1


class TestReductionStreamManager:
    """Test cases for ReductionStreamManager class."""

    def test_get_stream_caches_pipes_by_sources_and_view(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that get_stream caches pipes by source names and view name."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Same parameters return same pipe
        pipe1 = manager.get_stream({'source1', 'source2'}, 'test_view')
        pipe2 = manager.get_stream(
            {'source2', 'source1'}, 'test_view'
        )  # Order doesn't matter
        assert pipe1 is pipe2
        assert fake_pipe_factory.call_count == 1

        # Different view creates new pipe
        pipe3 = manager.get_stream({'source1', 'source2'}, 'other_view')
        assert pipe3 is not pipe1
        assert fake_pipe_factory.call_count == 2

        # Different sources create new pipe
        pipe4 = manager.get_stream({'source1', 'source3'}, 'test_view')
        assert pipe4 is not pipe1
        assert fake_pipe_factory.call_count == 3

    def test_data_flow_with_single_source(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test data flow through reduction stream with single source."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)
        data_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )

        # Set data in the service
        data_service[data_key] = sample_data

        # Verify data was sent through the pipe
        assert len(pipe.send_calls) == 1
        # The assembler merges data, so it should be a dict
        sent_data = pipe.send_calls[0]
        assert isinstance(sent_data, dict)
        assert data_key in sent_data
        assert sc.identical(sent_data[data_key], sample_data)

    def test_data_flow_with_multiple_sources(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test data flow through reduction stream with multiple sources."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream({'source1', 'source2'}, 'test_view')

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        # Set data for both sources in transaction
        with data_service.transaction():
            data_service[data_key1] = sample_data
            data_service[data_key2] = sample_data * 2

        # Verify merged data was sent
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert isinstance(sent_data, dict)
        assert data_key1 in sent_data
        assert data_key2 in sent_data
        assert sc.identical(sent_data[data_key1], sample_data)
        assert sc.identical(sent_data[data_key2], sample_data * 2)

    def test_reduction_stream_handles_incremental_updates(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that reduction stream handles incremental source updates correctly."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream({'source1', 'source2'}, 'test_view')

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        # Add sources incrementally
        data_service[data_key1] = sample_data
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert data_key1 in sent_data
        assert data_key2 not in sent_data

        data_service[data_key2] = sample_data * 2
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert data_key1 in sent_data
        assert data_key2 in sent_data

        # Update existing source preserves other data
        data_service[data_key1] = sample_data * 3
        assert len(pipe.send_calls) == 3
        sent_data = pipe.send_calls[2]
        assert sc.identical(sent_data[data_key1], sample_data * 3)
        assert sc.identical(sent_data[data_key2], sample_data * 2)

    def test_reduction_streams_are_independent(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that reduction streams are triggered independently and ignore unrelated keys."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream({'source1'}, 'view1')
        pipe2 = manager.get_stream({'source2'}, 'view2')

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/view1',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/view2',
        )
        unrelated_key = DataKey(
            service_name='data_reduction',
            source_name='source3',
            key='reduced/source3/view1',
        )

        # Update unrelated key - no pipes should be triggered
        data_service[unrelated_key] = sample_data
        assert len(pipe1.send_calls) == 0
        assert len(pipe2.send_calls) == 0

        # Update pipe1's key - only pipe1 should be triggered
        data_service[data_key1] = sample_data
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 0

        # Update pipe2's key - only pipe2 should be triggered
        data_service[data_key2] = sample_data * 2
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1

    def test_reduction_stream_with_empty_source_set(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test reduction stream with empty source set."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        # Empty source set
        pipe_empty = manager.get_stream(set(), 'test_view')
        assert isinstance(pipe_empty, FakePipe)

        # Test that empty source pipe is never triggered by any data
        unrelated_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )

        data_service[unrelated_key] = sample_data

        # Empty source pipe should not be triggered by any key
        assert len(pipe_empty.send_calls) == 0

        # Try another unrelated key
        another_key = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        data_service[another_key] = sample_data * 2

        # Empty source pipe should still not be triggered
        assert len(pipe_empty.send_calls) == 0

    def test_streams_with_shared_source_are_both_triggered(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that streams with a shared source are both triggered by an update."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        view_name = 'test_view'

        # Two streams with one source in common
        pipe1 = manager.get_stream({'source1', 'source2'}, view_name)
        pipe2 = manager.get_stream({'source1', 'source3'}, view_name)

        assert pipe1 is not pipe2
        assert fake_pipe_factory.call_count == 2

        # Test that updating the shared source triggers both pipes
        data_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )

        data_service[data_key] = sample_data

        # Both pipes should be triggered
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1

        # Verify the data was sent to both pipes
        assert data_key in pipe1.send_calls[0]
        assert data_key in pipe2.send_calls[0]
        assert sc.identical(pipe1.send_calls[0][data_key], sample_data)
        assert sc.identical(pipe2.send_calls[0][data_key], sample_data)

    def test_unrelated_key_updates_dont_trigger(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that unrelated key updates don't trigger reduction streams."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream({'source1'}, 'view1')

        unrelated_key = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/view1',
        )

        # Update unrelated key
        data_service[unrelated_key] = sample_data

        # Pipe should not be triggered
        assert len(pipe.send_calls) == 0

        # Update the correct key
        correct_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/view1',
        )
        data_service[correct_key] = sample_data

        # Now pipe should be triggered
        assert len(pipe.send_calls) == 1
